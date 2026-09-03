// Copyright 2019 Intel Corporation. All Rights Reserved.
//
// SPDX-License-Identifier: (Apache-2.0 AND BSD-3-Clause)

use std::cell::Cell;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fs::File;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::thread::{self, JoinHandle};
use std::{convert, error, fmt, io, process};

use futures::executor::{ThreadPool, ThreadPoolBuilder};
use log::*;

use vhost::vhost_user::message::*;
use vhost::vhost_user::Backend;
use vhost_user_backend::bitmap::BitmapMmapRegion;
use vhost_user_backend::{
    VhostUserBackend, VringMutex, VringState, VringStateGuard, VringStateMutGuard, VringT,
};
use virtio_bindings::bindings::virtio_config::*;
use virtio_bindings::bindings::virtio_ring::{
    VIRTIO_RING_F_EVENT_IDX, VIRTIO_RING_F_INDIRECT_DESC,
};
use virtio_queue::{DescriptorChain, QueueOwnedT, QueueT};
use vm_memory::{
    ByteValued, GuestAddressSpace, GuestMemoryAtomic, GuestMemoryLoadGuard, GuestMemoryMmap, Le32,
};
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::event::{
    new_event_consumer_and_notifier, EventConsumer, EventFlag, EventNotifier,
};

use crate::descriptor_utils::{Error as VufDescriptorError, Reader, Writer};
use crate::filesystem::{FileSystem, SerializableFileSystem};
use crate::server::Server;
use crate::util::other_io_error;
use crate::Error as VhostUserFsError;

type LoggedMemory = GuestMemoryMmap<BitmapMmapRegion>;
type LoggedMemoryAtomic = GuestMemoryAtomic<LoggedMemory>;
type FsVring = DrainingVring<LoggedMemoryAtomic>;

#[derive(Default)]
struct VringInflightState {
    ready: bool,
    enabled: bool,
    active: usize,
}

struct VringInflight {
    /// Serializes ready/enabled control-plane transitions while a stop waits for completion.
    /// Request admission only takes `state`, so completions never need this lock.
    control: Mutex<()>,
    state: Mutex<VringInflightState>,
    drained: Condvar,
}

/// A vring that closes request-pool admission and drains work before it is stopped.
///
/// `vhost-user-backend` calls `set_queue_ready(false)` while handling GET_VRING_BASE and only
/// reads the base and clears the call fd after this method returns.  Its stock `VringMutex` cannot
/// account for requests that have already advanced `next_avail` and released the vring mutex for
/// asynchronous execution.  This wrapper associates those requests with the vring until their
/// used-ring update is complete.
#[derive(Clone)]
pub struct DrainingVring<M: GuestAddressSpace> {
    inner: VringMutex<M>,
    inflight: Arc<VringInflight>,
}

struct VringInflightPermit {
    inflight: Arc<VringInflight>,
}

impl Drop for VringInflightPermit {
    fn drop(&mut self) {
        let mut state = self.inflight.state.lock().unwrap();
        debug_assert!(state.active > 0);
        state.active -= 1;
        if state.active == 0 {
            self.inflight.drained.notify_all();
        }
    }
}

impl<M: 'static + GuestAddressSpace> DrainingVring<M> {
    /// Admit one asynchronous request only while both vring gates are open.
    ///
    /// The permit must be acquired before advancing `next_avail` and held until after the used
    /// ring and call eventfd have been updated.  A concurrent stop either closes the gate first or
    /// observes this increment and waits for its matching decrement.
    fn try_begin_pool_request(&self) -> Option<VringInflightPermit> {
        let mut state = self.inflight.state.lock().unwrap();
        if !state.ready || !state.enabled {
            return None;
        }
        state.active = state
            .active
            .checked_add(1)
            .expect("too many in-flight vring requests");
        Some(VringInflightPermit {
            inflight: Arc::clone(&self.inflight),
        })
    }

    /// Re-arm notifications when the second vring gate opens and replay pending work.
    ///
    /// The dependency consumes a kick before calling into the backend.  A callback already
    /// returned by epoll can therefore consume the last kick while a concurrent queue stop makes
    /// it ineligible for dispatch.  Checking `avail_idx` against `next_avail` as the queue becomes
    /// runnable again closes that race.  A self-kick issued before epoll registration remains
    /// pending in the eventfd and is observed as soon as registration completes.
    fn kick_pending_on_start(&self, was_running: bool, now_running: bool) {
        if was_running || !now_running {
            return;
        }

        let pending = self.inner.enable_notification().unwrap_or_else(|error| {
            fatal_error_or_exit(
                "failed to enable notifications while starting a virtqueue",
                error,
            )
        });
        if !pending {
            return;
        }

        let notifier = {
            let vring_state = self.inner.get_ref();
            let kick = vring_state.get_kick().as_ref().unwrap_or_else(|| {
                fatal_error_or_exit(
                    "failed to replay pending work while starting a virtqueue",
                    "running virtqueue has no kick fd",
                )
            });
            let cloned_consumer = kick.try_clone().unwrap_or_else(|error| {
                fatal_error_or_exit("failed to clone kick fd while starting a virtqueue", error)
            });
            // SAFETY: virtiofsd is Linux-only and both wrappers own a descriptor referring to the
            // same read/write eventfd.
            unsafe { EventNotifier::from_raw_fd(cloned_consumer.into_raw_fd()) }
        };

        if let Err(error) = notifier.notify() {
            fatal_error_or_exit(
                "failed to replay pending work while starting a virtqueue",
                error,
            );
        }
    }

    fn set_ready_and_maybe_drain(&self, ready: bool) {
        let _control = self.inflight.control.lock().unwrap();
        let mut state = self.inflight.state.lock().unwrap();

        if ready {
            let was_running = state.ready && state.enabled;
            // Publish the underlying ring as ready before opening asynchronous admission.
            self.inner.set_queue_ready(true);
            state.ready = true;
            self.kick_pending_on_start(was_running, state.ready && state.enabled);
            return;
        }

        // Close admission first, but leave the underlying ring running until callbacks have
        // re-enabled EVENT_IDX notifications and asynchronous completions have returned their
        // descriptors.  This keeps the old ring valid throughout the drain and prevents a stale
        // avail_event value from being carried across GET_VRING_BASE/reconfiguration.
        let was_running = state.ready && state.enabled;
        state.ready = false;
        while state.active != 0 {
            state = self.inflight.drained.wait(state).unwrap();
        }
        if was_running {
            self.inner.enable_notification().unwrap_or_else(|error| {
                fatal_error_or_exit(
                    "failed to re-enable notifications while stopping a virtqueue",
                    error,
                )
            });
        }
        self.inner.set_queue_ready(false);
    }

    fn set_enabled_and_maybe_drain(&self, enabled: bool) {
        let _control = self.inflight.control.lock().unwrap();
        let mut state = self.inflight.state.lock().unwrap();

        if enabled {
            let was_running = state.ready && state.enabled;
            self.inner.set_enabled(true);
            state.enabled = true;
            self.kick_pending_on_start(was_running, state.ready && state.enabled);
            return;
        }

        // SET_VRING_ENABLE(0) and RESET_DEVICE use the same drain/rearm ordering as
        // GET_VRING_BASE.
        let was_running = state.ready && state.enabled;
        state.enabled = false;
        while state.active != 0 {
            state = self.inflight.drained.wait(state).unwrap();
        }
        if was_running {
            self.inner.enable_notification().unwrap_or_else(|error| {
                fatal_error_or_exit(
                    "failed to re-enable notifications while disabling a virtqueue",
                    error,
                )
            });
        }
        self.inner.set_enabled(false);
    }
}

impl<'a, M: 'a + GuestAddressSpace> VringStateGuard<'a, M> for DrainingVring<M> {
    type G = <VringMutex<M> as VringStateGuard<'a, M>>::G;
}

impl<'a, M: 'a + GuestAddressSpace> VringStateMutGuard<'a, M> for DrainingVring<M> {
    type G = <VringMutex<M> as VringStateMutGuard<'a, M>>::G;
}

impl<M: 'static + GuestAddressSpace> VringT<M> for DrainingVring<M> {
    fn new(mem: M, max_queue_size: u16) -> std::result::Result<Self, virtio_queue::Error> {
        Ok(Self {
            inner: VringMutex::new(mem, max_queue_size)?,
            inflight: Arc::new(VringInflight {
                control: Mutex::new(()),
                state: Mutex::new(VringInflightState::default()),
                drained: Condvar::new(),
            }),
        })
    }

    fn get_ref(&self) -> <Self as VringStateGuard<'_, M>>::G {
        self.inner.get_ref()
    }

    fn get_mut(&self) -> <Self as VringStateMutGuard<'_, M>>::G {
        self.inner.get_mut()
    }

    fn add_used(&self, desc_index: u16, len: u32) -> std::result::Result<(), virtio_queue::Error> {
        self.inner.add_used(desc_index, len)
    }

    fn signal_used_queue(&self) -> io::Result<()> {
        self.inner.signal_used_queue()
    }

    fn enable_notification(&self) -> std::result::Result<bool, virtio_queue::Error> {
        self.inner.enable_notification()
    }

    fn disable_notification(&self) -> std::result::Result<(), virtio_queue::Error> {
        self.inner.disable_notification()
    }

    fn needs_notification(&self) -> std::result::Result<bool, virtio_queue::Error> {
        self.inner.needs_notification()
    }

    fn set_enabled(&self, enabled: bool) {
        self.set_enabled_and_maybe_drain(enabled);
    }

    fn set_queue_info(
        &self,
        desc_table: u64,
        avail_ring: u64,
        used_ring: u64,
    ) -> std::result::Result<(), virtio_queue::Error> {
        self.inner.set_queue_info(desc_table, avail_ring, used_ring)
    }

    fn queue_next_avail(&self) -> u16 {
        self.inner.queue_next_avail()
    }

    fn set_queue_next_avail(&self, base: u16) {
        self.inner.set_queue_next_avail(base);
    }

    fn set_queue_next_used(&self, idx: u16) {
        self.inner.set_queue_next_used(idx);
    }

    fn queue_used_idx(&self) -> std::result::Result<u16, virtio_queue::Error> {
        self.inner.queue_used_idx()
    }

    fn set_queue_size(&self, num: u16) {
        self.inner.set_queue_size(num);
    }

    fn set_queue_event_idx(&self, enabled: bool) {
        self.inner.set_queue_event_idx(enabled);
    }

    fn set_queue_ready(&self, ready: bool) {
        self.set_ready_and_maybe_drain(ready);
    }

    fn set_kick(&self, file: Option<File>) {
        self.inner.set_kick(file);
    }

    fn read_kick(&self) -> io::Result<bool> {
        self.inner.read_kick()
    }

    fn set_call(&self, file: Option<File>) {
        self.inner.set_call(file);
    }

    fn set_err(&self, file: Option<File>) {
        self.inner.set_err(file);
    }
}

const QUEUE_SIZE: usize = 32768;

/// The default number of request queues.
pub const DEFAULT_REQUEST_QUEUES: u16 = 1;
/// The maximum number of request queues supported by this backend.
///
/// `vhost-user-backend` uses a `u64` bitmap to assign queues to worker
/// threads. Queue 0 is reserved for the high-priority queue, leaving 63
/// request queues.
pub const MAX_REQUEST_QUEUES: u16 = 63;

const HIPRIO_QUEUE_INDEX: usize = 0;
const REQUEST_QUEUE_INDEX_BASE: usize = 1;
const REQUESTS_PAUSED: usize = 1usize << (usize::BITS - 1);
const ACTIVE_REQUEST_MASK: usize = !REQUESTS_PAUSED;

/// The maximum length of the tag being used.
pub const MAX_TAG_LEN: usize = 36;

type Result<T> = std::result::Result<T, Error>;

// The compiler warns that some wrapped values are never read, but they are in fact read by
// `<Error as fmt::Display>::fmt()` via the derived `Debug`.
#[allow(dead_code)]
#[derive(Debug)]
pub enum Error {
    /// Failed to create kill eventfd.
    CreateKillEventFd(io::Error),
    /// Failed to create thread pool.
    CreateThreadPool(io::Error),
    /// Failed to create the temporary unshare(CLONE_FS) preflight thread.
    CreateUnsharePreflightThread(io::Error),
    /// Failed to handle event other than input event.
    HandleEventNotEpollIn,
    /// Failed to handle unknown event.
    HandleEventUnknownEvent,
    /// Iterating through the queue failed.
    IterateQueue,
    /// No memory configured.
    NoMemoryConfigured,
    /// Processing queue failed.
    ProcessQueue(VhostUserFsError),
    /// Creating a queue reader failed.
    QueueReader(VufDescriptorError),
    /// Creating a queue writer failed.
    QueueWriter(VufDescriptorError),
    /// The unshare(CLONE_FS) call failed.
    UnshareCloneFs(io::Error),
    /// The temporary unshare(CLONE_FS) preflight thread panicked.
    UnsharePreflightThreadPanicked,
    /// Invalid tag name
    InvalidTag,
    /// Invalid number of request queues.
    InvalidRequestQueueCount(u16),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use self::Error::UnshareCloneFs;
        match self {
            UnshareCloneFs(error) => {
                write!(
                    f,
                    "The unshare(CLONE_FS) syscall failed with '{error}'. \
                    If running in a container please check that the container \
                    runtime seccomp policy allows unshare."
                )
            }
            Self::InvalidTag => write!(
                f,
                "The tag may not be empty or longer than {MAX_TAG_LEN} bytes (encoded as UTF-8)."
            ),
            Self::InvalidRequestQueueCount(count) => write!(
                f,
                "The number of request queues must be between {DEFAULT_REQUEST_QUEUES} and \
                 {MAX_REQUEST_QUEUES}, got {count}"
            ),
            Self::QueueReader(e) => write!(f, "Failed to create a queue reader: {e}"),
            Self::QueueWriter(e) => write!(f, "Failed to create a queue writer: {e}"),
            Self::ProcessQueue(e) => write!(f, "Failed to handle an incoming request: {e}"),
            _ => write!(f, "{self:?}"),
        }
    }
}

impl error::Error for Error {}

impl convert::From<Error> for io::Error {
    fn from(e: Error) -> Self {
        other_io_error(e)
    }
}

struct VhostUserFsThread<F: FileSystem + Send + Sync + 'static> {
    mem: OnceLock<LoggedMemoryAtomic>,
    server: Arc<Server<F>>,
    // handle request from backend to frontend
    vu_req: RwLock<Option<Backend>>,
    event_idx: AtomicBool,
    pool: Option<ThreadPool>,
    lifecycle: Arc<RequestLifecycle>,
    unshare_fs_on_event: bool,
    worker_unshare: fn() -> io::Result<()>,
}

#[derive(Default)]
struct RequestLifecycleState {
    pause_count: usize,
    deferred_kicks: BTreeMap<usize, Arc<EventNotifier>>,
}

/// Coordinates request execution with reset and stopped-state migration.
///
/// Request admission is a single atomic operation.  The mutex is used only by the
/// migration/reset control path to nest pauses and retain kicks consumed while paused;
/// normal request dispatch and completion never take a shared mutex.
struct RequestLifecycle {
    /// Advanced by reset so callbacks from an old vring generation cannot affect the new session.
    session_generation: AtomicU64,
    /// The high bit is the pause gate; remaining bits count accepted requests.
    active_state: AtomicUsize,
    drain_lock: Mutex<()>,
    drained: Condvar,
    state: Mutex<RequestLifecycleState>,
}

struct RequestPermit {
    lifecycle: Arc<RequestLifecycle>,
}

enum RequestAcquireOutcome {
    Acquired(RequestPermit),
    Paused,
    Stale,
}

impl RequestLifecycle {
    fn new() -> Self {
        Self {
            session_generation: AtomicU64::new(0),
            active_state: AtomicUsize::new(0),
            drain_lock: Mutex::new(()),
            drained: Condvar::new(),
            state: Mutex::new(RequestLifecycleState::default()),
        }
    }

    /// Return the device session observed by a newly entered event callback.
    fn current_session_generation(&self) -> u64 {
        self.session_generation.load(Ordering::Acquire)
    }

    /// Admit one request unless migration/reset has paused the device.
    fn start_request_for_generation(
        self: &Arc<Self>,
        session_generation: u64,
    ) -> RequestAcquireOutcome {
        if self.current_session_generation() != session_generation {
            return RequestAcquireOutcome::Stale;
        }

        self.start_request_after_generation_check(session_generation)
    }

    /// Admit a request after the callback's initial session check.
    ///
    /// Kept as a separate step so the post-CAS generation check, which closes the reset ABA
    /// window, can be exercised deterministically in tests.
    fn start_request_after_generation_check(
        self: &Arc<Self>,
        session_generation: u64,
    ) -> RequestAcquireOutcome {
        let mut state = self.active_state.load(Ordering::Acquire);
        loop {
            if state & REQUESTS_PAUSED != 0 {
                return RequestAcquireOutcome::Paused;
            }
            assert!(
                state & ACTIVE_REQUEST_MASK != ACTIVE_REQUEST_MASK,
                "too many active requests"
            );

            match self.active_state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let permit = RequestPermit {
                        lifecycle: Arc::clone(self),
                    };
                    // active_state can make an ABA transition 0 -> PAUSED -> 0 across reset while
                    // this callback is descheduled.  Recheck after the successful CAS.  If reset
                    // already advanced the session, dropping the permit undoes the stale claim;
                    // otherwise the active count makes reset wait before advancing it.
                    if self.current_session_generation() != session_generation {
                        drop(permit);
                        return RequestAcquireOutcome::Stale;
                    }
                    return RequestAcquireOutcome::Acquired(permit);
                }
                Err(current) => state = current,
            }
        }
    }

    #[cfg(test)]
    fn start_request(self: &Arc<Self>) -> Option<RequestPermit> {
        match self.start_request_for_generation(self.current_session_generation()) {
            RequestAcquireOutcome::Acquired(permit) => Some(permit),
            RequestAcquireOutcome::Paused | RequestAcquireOutcome::Stale => None,
        }
    }

    /// Drop kick state belonging to a device session that is being reset.
    fn discard_queued_kicks(&self) {
        let mut state = self.state.lock().unwrap();
        debug_assert!(state.pause_count != 0);
        self.session_generation.fetch_add(1, Ordering::AcqRel);
        state.deferred_kicks.clear();
    }

    fn pause(&self) {
        let mut state = self.state.lock().unwrap();
        state.pause_count += 1;
        if state.pause_count == 1 {
            // Linearizes against start_request(): a starter that loaded the unpaused word either
            // increments first (and is drained) or loses its CAS and observes this bit.
            self.active_state
                .fetch_or(REQUESTS_PAUSED, Ordering::AcqRel);
        }
    }

    fn wait_for_drain(&self) {
        debug_assert!(self.state.lock().unwrap().pause_count > 0);

        // The last request locks drain_lock before notification. Taking the same
        // lock before checking the count prevents a completion between the check and wait from
        // becoming a lost wake-up, without putting a mutex on the normal request hot path.
        let mut drain_guard = self.drain_lock.lock().unwrap();
        while self.active_state.load(Ordering::Acquire) & ACTIVE_REQUEST_MASK != 0 {
            drain_guard = self.drained.wait(drain_guard).unwrap();
        }
    }

    fn resume(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(state.pause_count > 0, "unbalanced request lifecycle resume");
        state.pause_count -= 1;
        if state.pause_count != 0 {
            return;
        }

        let deferred_kicks = std::mem::take(&mut state.deferred_kicks);
        // Normally resume follows drain and the active count is zero. A transfer-thread spawn
        // failure resumes immediately, however, and safely preserves any already-admitted count.
        let _ = self
            .active_state
            .fetch_and(!REQUESTS_PAUSED, Ordering::Release);
        drop(state);

        // A vring kick is consumed before VhostUserBackend::handle_event() is called.  If the
        // lifecycle was paused at that point, re-signal the same eventfd after resume so an
        // available descriptor cannot remain stranded waiting for a new guest kick.
        for (queue_index, notifier) in deferred_kicks {
            notify_queue_or_exit(queue_index, "replay a deferred virtqueue kick", &notifier);
        }
    }

    fn defer_kick_for_generation(
        &self,
        queue_index: usize,
        notifier: EventNotifier,
        session_generation: u64,
    ) {
        let notifier = Arc::new(notifier);
        let mut state = self.state.lock().unwrap();
        if self.current_session_generation() != session_generation {
            return;
        }
        if state.pause_count != 0 {
            // Always retain the current kick generation.  An old fd can remain valid after
            // GET_VRING_BASE while no longer being registered with the queue's epoll worker.
            state.deferred_kicks.insert(queue_index, notifier);
            return;
        }

        // resume() won the race with the event handler, so replay immediately.
        drop(state);
        notify_queue_or_exit(queue_index, "replay a deferred virtqueue kick", &notifier);
    }

    #[cfg(test)]
    fn defer_kick(&self, queue_index: usize, notifier: EventNotifier) {
        self.defer_kick_for_generation(queue_index, notifier, self.current_session_generation());
    }
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        let previous = self.lifecycle.active_state.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous & ACTIVE_REQUEST_MASK, 0);
        if previous & REQUESTS_PAUSED != 0 && previous & ACTIVE_REQUEST_MASK == 1 {
            // See wait_for_drain(): this lock is touched only by the final request during a
            // pause, never by the normal request hot path.
            let _drain_guard = self.lifecycle.drain_lock.lock().unwrap();
            self.lifecycle.drained.notify_all();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PoolQueueOutcome {
    Drained,
    Deferred,
    Stale,
}

fn queue_is_running(vring: &FsVring) -> bool {
    let vring_state = vring.get_ref();
    vring_state.is_enabled() && vring_state.get_queue().ready()
}

/// Terminate the daemon when an error cannot be propagated past a dependency-owned worker.
///
/// `vhost-user-backend` does not surface a vring worker's returned error while the daemon is
/// running.  Returning `Err` or unwinding would therefore leave the socket alive with one or more
/// queues permanently unserviced.  Exiting makes the failure observable to QEMU and the service
/// supervisor instead.
fn fatal_error_or_exit(action: &str, error: impl fmt::Display) -> ! {
    error!("{action}: {error}");
    process::exit(1);
}

fn notify_queue_or_exit(queue_index: usize, action: &str, notifier: &EventNotifier) {
    if let Err(error) = notifier.notify() {
        // A failed replay strands descriptors without another guaranteed guest kick.  Returning
        // an error would only terminate one dependency-owned epoll worker, so fail the daemon.
        fatal_error_or_exit(
            &format!("failed to {action} for virtqueue {queue_index}"),
            error,
        );
    }
}

thread_local! {
    static FS_UNSHARED: Cell<bool> = const { Cell::new(false) };
}

fn unshare_fs() -> io::Result<()> {
    let ret = unsafe { libc::unshare(libc::CLONE_FS) };
    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn ensure_fs_unshared(worker_unshare: fn() -> io::Result<()>) -> Result<()> {
    FS_UNSHARED.with(|unshared| {
        if !unshared.get() {
            worker_unshare().map_err(Error::UnshareCloneFs)?;
            unshared.set(true);
        }
        Ok(())
    })
}

fn preflight_unshare_fs() -> Result<()> {
    // unshare(CLONE_FS) permanently changes the calling thread's fs_struct.
    // Probe it in a disposable thread so public builder callers keep sharing
    // cwd, root, and umask with their existing threads.
    let handle = thread::Builder::new()
        .name("virtiofsd-unshare-preflight".to_string())
        .spawn(unshare_fs)
        .map_err(Error::CreateUnsharePreflightThread)?;

    handle
        .join()
        .map_err(|_| Error::UnsharePreflightThreadPanicked)?
        .map_err(Error::UnshareCloneFs)
}

impl<F: FileSystem + SerializableFileSystem + Send + Sync + 'static> VhostUserFsThread<F> {
    fn new_with_unshare(
        fs: F,
        thread_pool_size: usize,
        num_request_queues: u16,
        preflight: fn() -> Result<()>,
        worker_unshare: fn() -> io::Result<()>,
    ) -> Result<Self> {
        // Without a request pool, multi-queue vring workers execute filesystem operations
        // themselves and therefore need private cwd/root/umask state.  Pool workers unshare in
        // ThreadPoolBuilder::after_start(), while their vring workers only dispatch requests.
        let unshare_fs_on_event =
            num_request_queues > DEFAULT_REQUEST_QUEUES && thread_pool_size == 0;

        if unshare_fs_on_event || thread_pool_size > 0 {
            // Test that unshare(CLONE_FS) works before worker threads are started.
            // The call is unprivileged, but some container seccomp policies reject it.
            preflight()?;
        }

        let pool = if thread_pool_size > 0 {
            Some(
                ThreadPoolBuilder::new()
                    .after_start(move |_| {
                        // Keep cwd, root, and umask private for xattr and POSIX ACL operations.
                        if let Err(error) = worker_unshare() {
                            // Losing a pool worker would otherwise leave requests queued forever.
                            error!("thread-pool worker failed to unshare(CLONE_FS): {error}");
                            process::exit(1);
                        }
                    })
                    .pool_size(thread_pool_size)
                    .create()
                    .map_err(Error::CreateThreadPool)?,
            )
        } else {
            None
        };

        Ok(VhostUserFsThread {
            mem: OnceLock::new(),
            server: Arc::new(Server::new(fs)),
            vu_req: RwLock::new(None),
            event_idx: AtomicBool::new(false),
            pool,
            lifecycle: Arc::new(RequestLifecycle::new()),
            unshare_fs_on_event,
            worker_unshare,
        })
    }

    fn return_descriptor(
        vring_state: &mut VringState<LoggedMemoryAtomic>,
        head_index: u16,
        event_idx: bool,
        len: usize,
    ) {
        let used_len: u32 = match len.try_into() {
            Ok(len) => len,
            Err(_) => panic!("Invalid used length, can't return used descritors to the ring"),
        };

        if vring_state.add_used(head_index, used_len).is_err() {
            warn!("Couldn't return used descriptors to the ring");
        }

        if event_idx {
            match vring_state.needs_notification() {
                Err(_) => {
                    warn!("Couldn't check if queue needs to be notified");
                    vring_state.signal_used_queue().unwrap();
                }
                Ok(needs_notification) => {
                    if needs_notification {
                        vring_state.signal_used_queue().unwrap();
                    }
                }
            }
        } else {
            vring_state.signal_used_queue().unwrap();
        }
    }

    fn process_message(
        server: &Server<F>,
        mem: &GuestMemoryLoadGuard<LoggedMemory>,
        chain: DescriptorChain<GuestMemoryLoadGuard<LoggedMemory>>,
        vu_req: Option<&mut Backend>,
    ) -> Result<usize> {
        let reader = Reader::new(mem, chain.clone()).map_err(Error::QueueReader)?;
        let writer = Writer::new(mem, chain).map_err(Error::QueueWriter)?;

        server
            .handle_message(reader, writer, vu_req)
            .map_err(Error::ProcessQueue)
    }

    fn process_queue_pool(
        &self,
        global_queue_index: usize,
        vring: FsVring,
        session_generation: u64,
    ) -> Result<PoolQueueOutcome> {
        if self.lifecycle.current_session_generation() != session_generation {
            return Ok(PoolQueueOutcome::Stale);
        }
        if !queue_is_running(&vring) {
            return Ok(PoolQueueOutcome::Drained);
        }
        let atomic_mem = self.mem.get().ok_or(Error::NoMemoryConfigured)?;
        let vu_req = self.vu_req.read().unwrap().clone();
        let event_idx = self.event_idx.load(Ordering::Acquire);

        loop {
            // Register the request with the global reset/migration drain before advancing
            // next_avail.  ThreadPool remains responsible for scheduling and limiting execution;
            // no backend-wide capacity mutex is taken here.
            let permit = match self
                .lifecycle
                .start_request_for_generation(session_generation)
            {
                RequestAcquireOutcome::Acquired(permit) => permit,
                RequestAcquireOutcome::Paused => {
                    self.defer_queue_kick(global_queue_index, &vring, session_generation);
                    return Ok(PoolQueueOutcome::Deferred);
                }
                RequestAcquireOutcome::Stale => return Ok(PoolQueueOutcome::Stale),
            };

            // Register this descriptor with the vring before advancing next_avail.  In
            // particular, GET_VRING_BASE closes this per-vring gate and waits for every permit to
            // be released before it reads the base, clears the call fd, and replies to QEMU.
            let Some(vring_permit) = vring.try_begin_pool_request() else {
                drop(permit);
                return Ok(PoolQueueOutcome::Drained);
            };

            let avail_desc = {
                let mut vring_state = vring.get_mut();

                // Linearize advancing next_avail against GET_VRING_BASE.  If teardown wins the
                // vring mutex and reports the old base to the frontend, this worker must not later
                // consume and execute that descriptor from the stopped ring.
                if !vring_state.is_enabled() || !vring_state.get_queue().ready() {
                    drop(vring_state);
                    drop(vring_permit);
                    drop(permit);
                    return Ok(PoolQueueOutcome::Drained);
                }

                let mem = atomic_mem.memory();
                vring_state
                    .get_queue_mut()
                    .iter(mem)
                    .map_err(|_| Error::IterateQueue)?
                    .next()
            };
            let Some(avail_desc) = avail_desc else {
                drop(vring_permit);
                drop(permit);
                return Ok(PoolQueueOutcome::Drained);
            };

            // Prepare a set of objects that can be moved to the worker thread.
            let atomic_mem = atomic_mem.clone();
            let server = self.server.clone();
            let mut vu_req = vu_req.clone();
            let worker_vring = vring.clone();
            let worker_desc = avail_desc.clone();

            self.pool.as_ref().unwrap().spawn_ok(async move {
                let mem = atomic_mem.memory();
                let head_index = worker_desc.head_index();

                let len = Self::process_message(&server, &mem, worker_desc, vu_req.as_mut())
                    .unwrap_or_else(|error| {
                        fatal_error_or_exit(
                            &format!(
                                "failed to process a request from virtqueue {global_queue_index}"
                            ),
                            error,
                        )
                    });

                Self::return_descriptor(&mut worker_vring.get_mut(), head_index, event_idx, len);

                // Completion must be visible in the old used ring before a queue stop can
                // return and allow the frontend to reuse this Vring object for a new
                // configuration.
                drop(vring_permit);
                drop(permit);
            });
        }
    }

    fn process_queue_serial(
        &self,
        vring_state: &mut VringState<LoggedMemoryAtomic>,
    ) -> Result<bool> {
        let mut used_any = false;
        let mem = self.mem.get().ok_or(Error::NoMemoryConfigured)?.memory();
        let mut vu_req = self.vu_req.read().unwrap().clone();
        let event_idx = self.event_idx.load(Ordering::Acquire);

        let avail_chains: Vec<DescriptorChain<GuestMemoryLoadGuard<LoggedMemory>>> = vring_state
            .get_queue_mut()
            .iter(mem.clone())
            .map_err(|_| Error::IterateQueue)?
            .collect();

        for chain in avail_chains {
            used_any = true;

            let head_index = chain.head_index();

            let len = Self::process_message(&self.server, &mem, chain, vu_req.as_mut())
                .unwrap_or_else(|error| {
                    error!("{error}");
                    process::exit(1);
                });

            Self::return_descriptor(vring_state, head_index, event_idx, len);
        }

        Ok(used_any)
    }

    fn defer_queue_kick(
        &self,
        global_queue_index: usize,
        vring: &FsVring,
        session_generation: u64,
    ) {
        if let Some(notifier) = Self::clone_queue_kick(global_queue_index, vring) {
            self.lifecycle.defer_kick_for_generation(
                global_queue_index,
                notifier,
                session_generation,
            );
        }
    }

    fn clone_queue_kick(global_queue_index: usize, vring: &FsVring) -> Option<EventNotifier> {
        let vring_state = vring.get_ref();
        if !vring_state.is_enabled() || !vring_state.get_queue().ready() {
            // GET_VRING_BASE/RESET_DEVICE can leave a valid kick fd after removing the queue from
            // epoll.  Never retain or signal that descriptor for a stopped vring generation.
            debug!("ignoring stale event for stopped virtqueue {global_queue_index}");
            return None;
        }
        let Some(kick) = vring_state.get_kick() else {
            // A consumed kick cannot be replayed and the queue would hang indefinitely.  The
            // dependency does not surface a single epoll worker's return value to the daemon.
            error!("virtqueue {global_queue_index} has no kick fd to replay");
            process::exit(1);
        };

        let cloned_consumer = kick.try_clone().unwrap_or_else(|error| {
            // Returning this error only stops one vhost-user-backend epoll worker.  Fail fast so
            // the supervisor can restart a backend instead of leaving one queue silently hung.
            error!("failed to clone the kick fd for virtqueue {global_queue_index}: {error}");
            process::exit(1);
        });
        // SAFETY: virtiofsd is Linux-only and the cloned descriptor refers to the same read/write
        // eventfd. EventConsumer and EventNotifier both take ownership of one File descriptor.
        let notifier = unsafe { EventNotifier::from_raw_fd(cloned_consumer.into_raw_fd()) };
        drop(vring_state);

        Some(notifier)
    }

    /// Return whether an EVENT_IDX callback crossed a device-session boundary.
    ///
    /// `retry` is the result of enable_notification().  If it reports pending work, re-signal the
    /// current vring instead of letting a callback carrying the old session generation consume it.
    fn replay_event_idx_after_session_change(
        &self,
        global_queue_index: usize,
        vring: &FsVring,
        session_generation: u64,
        retry: bool,
    ) -> bool {
        if self.lifecycle.current_session_generation() == session_generation {
            return false;
        }

        if !retry {
            return true;
        }

        let Some(notifier) = Self::clone_queue_kick(global_queue_index, vring) else {
            return true;
        };
        notify_queue_or_exit(
            global_queue_index,
            "replay work discovered across an EVENT_IDX session change",
            &notifier,
        );
        true
    }

    fn handle_event_pool(
        &self,
        local_queue_index: usize,
        global_queue_index: usize,
        vrings: &[FsVring],
        session_generation: u64,
    ) -> io::Result<()> {
        Self::log_queue_event(global_queue_index);

        if self.event_idx.load(Ordering::Acquire) {
            // vm-virtio's Queue implementation only checks avail_index
            // once, so to properly support EVENT_IDX we need to keep
            // calling process_queue() until it stops finding new
            // requests on the queue.
            loop {
                // Keep the old ring admitted from notification suppression through the final
                // EVENT_IDX double-check.  A concurrent queue stop closes admission and waits for
                // this permit before it invalidates the underlying ring.
                let Some(callback_permit) = vrings[local_queue_index].try_begin_pool_request()
                else {
                    break;
                };
                {
                    let mut vring_state = vrings[local_queue_index].get_mut();
                    if !vring_state.is_enabled() || !vring_state.get_queue().ready() {
                        break;
                    }
                    vring_state.disable_notification().unwrap();
                }
                let outcome = self.process_queue_pool(
                    global_queue_index,
                    vrings[local_queue_index].clone(),
                    session_generation,
                );
                let retry = {
                    let mut vring_state = vrings[local_queue_index].get_mut();
                    vring_state.enable_notification().unwrap()
                };
                drop(callback_permit);

                let outcome = match outcome {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        // Preserve the original EVENT_IDX behavior: an error must not bypass the
                        // matching enable_notification() and leave this queue suppressed.
                        error!("processing virtqueue {global_queue_index}: {error}");
                        if retry {
                            continue;
                        }
                        break;
                    }
                };

                // The session can change after process_queue_pool() chose its outcome, including
                // while this callback waits to reacquire the vring for enable_notification().
                // Treat every prior outcome as stale at this final linearization point.  If the
                // EVENT_IDX double-check found pending work, hand it to a new-generation callback.
                if self.replay_event_idx_after_session_change(
                    global_queue_index,
                    &vrings[local_queue_index],
                    session_generation,
                    retry,
                ) {
                    break;
                }

                match outcome {
                    PoolQueueOutcome::Deferred | PoolQueueOutcome::Stale => break,
                    PoolQueueOutcome::Drained if !retry => break,
                    PoolQueueOutcome::Drained => {}
                }
            }
        } else {
            // Without EVENT_IDX, a single call is enough.
            self.process_queue_pool(
                global_queue_index,
                vrings[local_queue_index].clone(),
                session_generation,
            )?;
        }

        Ok(())
    }

    fn handle_event_serial(
        &self,
        local_queue_index: usize,
        global_queue_index: usize,
        vrings: &[FsVring],
    ) -> io::Result<()> {
        Self::log_queue_event(global_queue_index);
        let mut vring_state = vrings[local_queue_index].get_mut();

        // The initial handle_event() gate and this lock acquisition are separate.  Recheck under
        // the same guard used to advance next_avail so GET_VRING_BASE either waits for this handler
        // or wins first and prevents any descriptor from being consumed after returning the base.
        if !vring_state.is_enabled() || !vring_state.get_queue().ready() {
            debug!("ignoring stale event for stopped virtqueue {global_queue_index}");
            return Ok(());
        }

        if self.event_idx.load(Ordering::Acquire) {
            // vm-virtio's Queue implementation only checks avail_index
            // once, so to properly support EVENT_IDX we need to keep
            // calling process_queue() until it stops finding new
            // requests on the queue.
            loop {
                vring_state.disable_notification().unwrap();
                // Preserve the original EVENT_IDX behavior: always re-enable notification even
                // when this processing pass reports an error.
                if let Err(error) = self.process_queue_serial(&mut vring_state) {
                    error!("processing virtqueue {global_queue_index}: {error}");
                }
                if !vring_state.enable_notification().unwrap() {
                    break;
                }
            }
        } else {
            // Without EVENT_IDX, a single call is enough.
            self.process_queue_serial(&mut vring_state)?;
        }

        Ok(())
    }

    fn log_queue_event(global_queue_index: usize) {
        if global_queue_index == HIPRIO_QUEUE_INDEX {
            debug!("HIPRIO_QUEUE_EVENT");
        } else {
            debug!(
                "REQUEST_QUEUE_EVENT[{}]",
                global_queue_index - REQUEST_QUEUE_INDEX_BASE
            );
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct VirtioFsConfig {
    tag: [u8; MAX_TAG_LEN],
    num_request_queues: Le32,
}

// vm-memory needs a Default implementation even though these values are never
// used anywhere...
impl Default for VirtioFsConfig {
    fn default() -> Self {
        Self {
            tag: [0; MAX_TAG_LEN],
            num_request_queues: Le32::default(),
        }
    }
}

unsafe impl ByteValued for VirtioFsConfig {}

struct PremigrationThread {
    handle: JoinHandle<()>,
    cancel: Arc<AtomicBool>,
}

fn cancel_premigration_thread(slot: &Mutex<Option<PremigrationThread>>, reason: &str) {
    let premigration_thread = slot.lock().unwrap().take();
    if let Some(premigration_thread) = premigration_thread {
        premigration_thread.cancel.store(true, Ordering::Relaxed);
        if premigration_thread.handle.join().is_err() {
            error!("Pre-migration preparation thread panicked while cancelling it: {reason}");
        }
    }
}

/// A builder for configurable creation of [`VhostUserFsBackend`] objects.
#[derive(Debug)]
pub struct VhostUserFsBackendBuilder {
    thread_pool_size: usize,
    num_request_queues: u16,
    tag: Option<String>,
}

impl Default for VhostUserFsBackendBuilder {
    fn default() -> Self {
        Self {
            thread_pool_size: 0,
            num_request_queues: DEFAULT_REQUEST_QUEUES,
            tag: None,
        }
    }
}

impl VhostUserFsBackendBuilder {
    /// Adjust the size of the thread pool to use.
    ///
    /// A value of `0` disables the usage of a thread pool.
    pub fn set_thread_pool_size(mut self, size: usize) -> Self {
        self.thread_pool_size = size;
        self
    }

    /// Set the number of request queues exposed by the backend.
    ///
    /// Valid values are [`DEFAULT_REQUEST_QUEUES`] through [`MAX_REQUEST_QUEUES`].  Values greater
    /// than one create an independent vring worker for every hiprio/request queue and require
    /// `unshare(CLONE_FS)` to be permitted when the backend is built.
    pub fn set_num_request_queues(mut self, count: u16) -> Self {
        self.num_request_queues = count;
        self
    }

    /// Set the tag to use for the file system.
    ///
    /// The tag length must not exceed [`MAX_TAG_LEN`] bytes.
    pub fn set_tag(mut self, tag: Option<String>) -> Self {
        self.tag = tag;
        self
    }

    /// Build the [`VhostUserFsBackend`] object.
    pub fn build<F>(self, fs: F) -> Result<VhostUserFsBackend<F>>
    where
        F: FileSystem + SerializableFileSystem + Send + Sync + 'static,
    {
        self.build_with_unshare(fs, preflight_unshare_fs, unshare_fs)
    }

    fn build_with_unshare<F>(
        self,
        fs: F,
        preflight: fn() -> Result<()>,
        worker_unshare: fn() -> io::Result<()>,
    ) -> Result<VhostUserFsBackend<F>>
    where
        F: FileSystem + SerializableFileSystem + Send + Sync + 'static,
    {
        if !(DEFAULT_REQUEST_QUEUES..=MAX_REQUEST_QUEUES).contains(&self.num_request_queues) {
            return Err(Error::InvalidRequestQueueCount(self.num_request_queues));
        }

        let thread = VhostUserFsThread::new_with_unshare(
            fs,
            self.thread_pool_size,
            self.num_request_queues,
            preflight,
            worker_unshare,
        )?;
        Ok(VhostUserFsBackend {
            thread,
            premigration_thread: Arc::new(Mutex::new(None)),
            migration_thread: None.into(),
            tag: self.tag,
            num_request_queues: self.num_request_queues,
        })
    }
}

pub struct VhostUserFsBackend<F: FileSystem + SerializableFileSystem + Send + Sync + 'static> {
    thread: VhostUserFsThread<F>,
    premigration_thread: Arc<Mutex<Option<PremigrationThread>>>,
    migration_thread: Mutex<Option<JoinHandle<io::Result<()>>>>,
    tag: Option<String>,
    num_request_queues: u16,
}

impl<F: FileSystem + SerializableFileSystem + Send + Sync + 'static> VhostUserFsBackend<F> {
    /// Create a [`VhostUserFsBackend`] without a thread pool or a tag.
    ///
    /// For more configurable creation refer to
    /// [`VhostUserFsBackendBuilder`].
    pub fn new(fs: F) -> Result<Self> {
        VhostUserFsBackendBuilder::default().build(fs)
    }

    fn total_queues(&self) -> usize {
        usize::from(self.num_request_queues) + REQUEST_QUEUE_INDEX_BASE
    }

    fn dedicated_queue_workers(&self) -> bool {
        self.num_request_queues > DEFAULT_REQUEST_QUEUES
    }

    fn cancel_premigration(&self, reason: &str) {
        cancel_premigration_thread(&self.premigration_thread, reason);
    }

    fn queue_index_for_event(
        &self,
        thread_id: usize,
        device_event: u16,
        local_queue_count: usize,
    ) -> Option<(usize, usize)> {
        let local_queue_index = usize::from(device_event);
        if local_queue_index >= local_queue_count {
            return None;
        }

        if !self.dedicated_queue_workers() {
            // Preserve the original topology: one worker handles both hiprio and request queues.
            (thread_id == 0 && local_queue_index < self.total_queues())
                .then_some((local_queue_index, local_queue_index))
        } else {
            // With multiple request queues each worker owns exactly one queue, so its local event
            // index is always zero and the worker thread ID is the global queue index.
            (local_queue_index == 0 && thread_id < self.total_queues())
                .then_some((local_queue_index, thread_id))
        }
    }

    fn handle_event_inner(
        &self,
        device_event: u16,
        evset: EventSet,
        vrings: &[FsVring],
        thread_id: usize,
    ) -> io::Result<()> {
        if evset != EventSet::IN {
            return Err(Error::HandleEventNotEpollIn.into());
        }

        // Reset advances this generation after pausing and draining accepted requests.  Carry the
        // value observed at callback entry through every lifecycle mutation so an old callback
        // that was delayed around reset cannot defer work or change state in the new session.
        let session_generation = self.thread.lifecycle.current_session_generation();

        let (local_queue_index, global_queue_index) = self
            .queue_index_for_event(thread_id, device_event, vrings.len())
            .ok_or(Error::HandleEventUnknownEvent)?;

        // The dependency can deliver an event that epoll had already returned just before
        // GET_VRING_BASE removed the queue registration and cleared its kick fd.  Recheck both
        // queue gates under the vring lock so this legal teardown race is not processed or treated
        // as a missing-kick failure.
        {
            let vring_state = vrings[local_queue_index].get_ref();
            if !vring_state.is_enabled() || !vring_state.get_queue().ready() {
                debug!("ignoring stale event for stopped virtqueue {global_queue_index}");
                return Ok(());
            }
        }

        if self.thread.unshare_fs_on_event {
            ensure_fs_unshared(self.thread.worker_unshare).unwrap_or_else(|error| {
                fatal_error_or_exit(
                    &format!(
                        "failed to isolate filesystem context for virtqueue {global_queue_index}"
                    ),
                    error,
                )
            });
        }

        if self.thread.pool.is_some() {
            self.thread.handle_event_pool(
                local_queue_index,
                global_queue_index,
                vrings,
                session_generation,
            )
        } else {
            // Direct queue workers execute independently while the lifecycle permit still allows
            // reset and stopped-state migration to drain them safely.
            let _permit = match self
                .thread
                .lifecycle
                .start_request_for_generation(session_generation)
            {
                RequestAcquireOutcome::Acquired(permit) => permit,
                RequestAcquireOutcome::Paused => {
                    self.thread.defer_queue_kick(
                        global_queue_index,
                        &vrings[local_queue_index],
                        session_generation,
                    );
                    return Ok(());
                }
                RequestAcquireOutcome::Stale => return Ok(()),
            };
            self.thread
                .handle_event_serial(local_queue_index, global_queue_index, vrings)
        }
    }
}

impl<F: FileSystem + SerializableFileSystem + Send + Sync + 'static> VhostUserBackend
    for VhostUserFsBackend<F>
{
    type Bitmap = BitmapMmapRegion;
    type Vring = FsVring;

    fn num_queues(&self) -> usize {
        self.total_queues()
    }

    fn queues_per_thread(&self) -> Vec<u64> {
        if !self.dedicated_queue_workers() {
            vec![(1u64 << self.total_queues()) - 1]
        } else {
            (0..self.total_queues())
                .map(|queue_index| 1u64 << queue_index)
                .collect()
        }
    }

    fn max_queue_size(&self) -> usize {
        QUEUE_SIZE
    }

    fn features(&self) -> u64 {
        (1 << VIRTIO_F_VERSION_1)
            | (1 << VIRTIO_RING_F_INDIRECT_DESC)
            | (1 << VIRTIO_RING_F_EVENT_IDX)
            | VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
            | VhostUserVirtioFeatures::LOG_ALL.bits()
    }

    fn protocol_features(&self) -> VhostUserProtocolFeatures {
        let mut protocol_features = VhostUserProtocolFeatures::MQ
            | VhostUserProtocolFeatures::BACKEND_REQ
            | VhostUserProtocolFeatures::BACKEND_SEND_FD
            | VhostUserProtocolFeatures::REPLY_ACK
            | VhostUserProtocolFeatures::CONFIGURE_MEM_SLOTS
            | VhostUserProtocolFeatures::LOG_SHMFD
            | VhostUserProtocolFeatures::DEVICE_STATE
            | VhostUserProtocolFeatures::RESET_DEVICE;

        if self.tag.is_some() {
            protocol_features |= VhostUserProtocolFeatures::CONFIG;
        }

        protocol_features
    }

    fn get_config(&self, offset: u32, size: u32) -> Vec<u8> {
        // virtio spec 1.2, 5.11.4:
        //   The tag is encoded in UTF-8 and padded with NUL bytes if shorter than
        //   the available space. This field is not NUL-terminated if the encoded
        //   bytes take up the entire field.
        // The length was already checked when parsing the arguments. Hence, we
        // only assert that everything looks sane and pad with NUL bytes to the
        // fixed length.
        let tag = self.tag.as_ref().expect("Did not expect read of config if tag is not set. We do not advertise F_CONFIG in that case!");
        assert!(tag.len() <= MAX_TAG_LEN, "too long tag length");
        assert!(!tag.is_empty(), "tag should not be empty");
        let mut fixed_len_tag = [0; MAX_TAG_LEN];
        fixed_len_tag[0..tag.len()].copy_from_slice(tag.as_bytes());

        let config = VirtioFsConfig {
            tag: fixed_len_tag,
            num_request_queues: Le32::from(u32::from(self.num_request_queues)),
        };

        let offset = offset as usize;
        let size = size as usize;
        let mut result: Vec<_> = config
            .as_slice()
            .iter()
            .skip(offset)
            .take(size)
            .copied()
            .collect();
        // pad with 0s up to `size`
        result.resize(size, 0);
        result
    }

    fn acked_features(&self, features: u64) {
        if features & VhostUserVirtioFeatures::LOG_ALL.bits() != 0 {
            // F_LOG_ALL set: Prepare for migration (unless we're already doing that)
            let mut premigration_thread = self.premigration_thread.lock().unwrap();
            if premigration_thread.is_none() {
                let cancel = Arc::new(AtomicBool::new(false));
                let cloned_server = Arc::clone(&self.thread.server);
                let cloned_cancel = Arc::clone(&cancel);
                let handle =
                    thread::spawn(move || cloned_server.prepare_serialization(cloned_cancel));
                *premigration_thread = Some(PremigrationThread { handle, cancel });
            }
        } else {
            // F_LOG_ALL cleared: Migration cancelled, if any was ongoing
            // (Note that this is our interpretation, and not said by the specification.  The back
            // end might clear this flag also on the source side once the VM has been stopped, even
            // before we receive SET_DEVICE_STATE_FD.  QEMU will clear F_LOG_ALL only when the VM
            // is running, i.e. when the source resumes after a cancelled migration, which is
            // exactly what we want, but it would be better if we had a more reliable way that is
            // backed up by the spec.  We could delay cancelling until we receive a guest request
            // while F_LOG_ALL is cleared, but that can take an indefinite amount of time.)
            self.cancel_premigration("F_LOG_ALL was cleared");
        }
    }

    fn reset_device(&self) {
        // The control-plane handler serializes SET/CHECK/RESET callbacks, but the transfer thread
        // survives after SET_DEVICE_STATE_FD has replied.  RESET_DEVICE has no error return, so it
        // must never silently acknowledge a reset that we cannot complete.  A transfer that has
        // already finished can be joined and discarded here; a transfer still blocked on an
        // arbitrary File cannot be cancelled through SerializableFileSystem's current API, so
        // fail the daemon instead of hanging the control plane or returning a false success.
        let (completed_transfer, lifecycle_already_paused) = {
            let mut migration_thread = self.migration_thread.lock().unwrap();
            match migration_thread.as_ref() {
                Some(handle) if !handle.is_finished() => fatal_error_or_exit(
                    "cannot safely RESET_DEVICE while state transfer is still running",
                    "the transfer channel is not cancellable",
                ),
                Some(_) => (migration_thread.take(), true),
                None => (None, false),
            }
        };

        if let Some(transfer) = completed_transfer {
            match transfer.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!("discarding failed device-state transfer during reset: {error}")
                }
                Err(_) => error!("device-state transfer panicked before reset"),
            }
        }

        // The vhost-user handler disables all vrings before calling us.  Wake pool dispatchers,
        // wait until every descriptor already consumed by the backend is complete, and only then
        // clear filesystem state.  Resume accepts requests from a later device reinitialization.
        if !lifecycle_already_paused {
            self.thread.lifecycle.pause();
        }
        self.cancel_premigration("the device was reset");
        self.thread.lifecycle.wait_for_drain();
        self.thread.server.destroy();
        // Unlike migration cancellation, reset starts a new device/FUSE session.  A kick cloned
        // from the old vring generation must not be replayed into a reconfigured queue.
        self.thread.lifecycle.discard_queued_kicks();
        self.thread.lifecycle.resume();
    }

    fn set_event_idx(&self, enabled: bool) {
        self.thread.event_idx.store(enabled, Ordering::Release);
    }

    fn update_memory(&self, mem: LoggedMemoryAtomic) -> io::Result<()> {
        // The standard vhost-user-backend path passes clones of the same GuestMemoryAtomic and
        // replaces its contents in place.  Also support a caller passing a different atomic: keep
        // the already-published object (and therefore the lock-free hot path), but replace its map
        // with a clone of the newly supplied map.
        if let Err(mem) = self.thread.mem.set(mem) {
            let replacement = (*mem.memory()).clone();
            self.thread
                .mem
                .get()
                .expect("memory was initialized by a competing update")
                .lock()
                .map_err(|_| io::Error::other("guest memory update lock is poisoned"))?
                .replace(replacement);
        }
        Ok(())
    }

    fn handle_event(
        &self,
        device_event: u16,
        evset: EventSet,
        vrings: &[FsVring],
        thread_id: usize,
    ) -> io::Result<()> {
        self.handle_event_inner(device_event, evset, vrings, thread_id)
    }

    fn exit_event(&self, _thread_index: usize) -> Option<(EventConsumer, EventNotifier)> {
        Some(
            new_event_consumer_and_notifier(EventFlag::NONBLOCK)
                .expect("Failed to create exit notifier"),
        )
    }

    fn set_backend_req_fd(&self, vu_req: Backend) {
        *self.thread.vu_req.write().unwrap() = Some(vu_req);
    }

    fn set_device_state_fd(
        &self,
        direction: VhostTransferStateDirection,
        phase: VhostTransferStatePhase,
        file: File,
    ) -> io::Result<Option<File>> {
        // Our caller (vhost-user-backend crate) pretty much ignores error objects we return (only
        // cares whether we succeed or not), so log errors here
        if let Err(err) = self.do_set_device_state_fd(direction, phase, file) {
            error!("Failed to initiate state (de-)serialization: {err}");
            return Err(err);
        }
        Ok(None)
    }

    fn check_device_state(&self) -> io::Result<()> {
        // Our caller (vhost-user-backend crate) pretty much ignores error objects we return (only
        // cares whether we succeed or not), so log errors here
        if let Err(err) = self.do_check_device_state() {
            error!("Migration failed: {err}");
            return Err(err);
        }
        Ok(())
    }
}

impl<F: FileSystem + SerializableFileSystem + Send + Sync + 'static> VhostUserFsBackend<F> {
    fn do_set_device_state_fd(
        &self,
        direction: VhostTransferStateDirection,
        phase: VhostTransferStatePhase,
        file: File,
    ) -> io::Result<()> {
        if phase != VhostTransferStatePhase::STOPPED {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Transfer in phase {phase:?} is not supported"),
            ));
        }

        let mut migration_thread = self.migration_thread.lock().unwrap();
        if migration_thread.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "A device state transfer is already in progress",
            ));
        }
        // STOPPED means no new guest requests should arrive.  Stop accepting before spawning the
        // transfer thread, then let that thread wait for already-consumed descriptors so this
        // callback remains non-blocking as required by the vhost-user-backend trait.
        self.thread.lifecycle.pause();

        let server = Arc::clone(&self.thread.server);
        let lifecycle = Arc::clone(&self.thread.lifecycle);
        let join_handle = match direction {
            VhostTransferStateDirection::SAVE => {
                // We should have a premigration thread that was started with `F_LOG_ALL`.  It
                // should already be finished, but you never know.
                // Take it inside the successfully spawned transfer thread. If spawning fails the
                // handle remains owned by the backend and can still be cancelled or reused.
                let premigration_thread = Arc::clone(&self.premigration_thread);

                thread::Builder::new()
                    .name("virtiofsd-state-save".to_string())
                    .spawn(move || {
                        lifecycle.wait_for_drain();

                        let premigration_thread = premigration_thread.lock().unwrap().take();
                        if let Some(premigration_thread) = premigration_thread {
                            // Let’s hope it’s finished.  Otherwise, we block migration downtime
                            // for a bit longer, but there’s nothing we can do.
                            premigration_thread.handle.join().map_err(|_| {
                                other_io_error(
                                    "Failed to finalize serialization preparation".to_string(),
                                )
                            })?;
                        } else {
                            // If we don’t have a premigration thread, that either means migration
                            // was cancelled at some point, or that there simply was no F_LOG_ALL at
                            // all.  QEMU doesn’t necessarily do memory logging when snapshotting,
                            // and in such cases we have no choice but to run preserialization now.
                            warn!(
                                "Front-end did not announce migration to begin, so we failed to \
                                     prepare for it; collecting data now.  If you are doing a snapshot, \
                                     that is OK; otherwise, migration downtime may be prolonged."
                            );
                            server.prepare_serialization(Arc::new(AtomicBool::new(false)));
                        }

                        server.serialize(file).map_err(|e| {
                            io::Error::new(e.kind(), format!("Failed to save state: {e}"))
                        })
                    })
            }

            VhostTransferStateDirection::LOAD => {
                // Take and cancel preparation only inside a successfully spawned transfer thread.
                // If spawning fails, the backend can resume without losing ownership of that
                // thread or leaving partially cancelled preparation state behind.
                let premigration_thread = Arc::clone(&self.premigration_thread);

                thread::Builder::new()
                    .name("virtiofsd-state-load".to_string())
                    .spawn(move || {
                        cancel_premigration_thread(
                            &premigration_thread,
                            "incoming migration state is being loaded",
                        );
                        lifecycle.wait_for_drain();

                        server.deserialize_and_apply(file).map_err(|e| {
                            io::Error::new(e.kind(), format!("Failed to load state: {e}"))
                        })
                    })
            }
        };

        let join_handle = match join_handle {
            Ok(join_handle) => join_handle,
            Err(error) => {
                self.thread.lifecycle.resume();
                return Err(io::Error::new(
                    error.kind(),
                    format!("Failed to spawn device state transfer thread: {error}"),
                ));
            }
        };

        *migration_thread = Some(join_handle);

        Ok(())
    }

    fn do_check_device_state(&self) -> io::Result<()> {
        // Keep the slot locked through joining and publishing IDLE.  Although normal vhost-user
        // control callbacks are serialized, this makes RESET/SET/CHECK atomic for direct callers
        // too: RESET can never observe TRANSFER after CHECK has taken away its JoinHandle.
        let mut migration_thread_slot = self.migration_thread.lock().unwrap();
        let Some(migration_thread) = migration_thread_slot.take() else {
            // `check_device_state()` must follow a successful `set_device_state_fd()`, so this is
            // a protocol violation
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Front-end attempts to check migration state, but no migration has been done",
            ));
        };

        let result = match migration_thread.join() {
            Ok(result) => result,
            Err(_) => Err(other_io_error("Failed to join the migration thread")),
        };

        // Whether transfer succeeded or failed, CHECK_DEVICE_STATE completes this transfer.  A
        // source may resume after failed/cancelled migration and a destination starts processing
        // requests after a successful load.
        self.thread.lifecycle.resume();
        drop(migration_thread_slot);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::{DirEntry, DirectoryIterator};
    use std::process::{Command, ExitStatus};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use vm_memory::{Bytes, GuestAddress, GuestMemory};
    use vmm_sys_util::tempfile::TempFile;

    struct EmptyDir;

    impl DirectoryIterator for EmptyDir {
        fn next(&mut self) -> Option<DirEntry<'_>> {
            None
        }
    }

    struct TestFs;

    impl FileSystem for TestFs {
        type Inode = u64;
        type Handle = u64;
        type DirIter = EmptyDir;
    }

    impl SerializableFileSystem for TestFs {}

    struct FinishedTransferFs {
        serialized: Arc<AtomicBool>,
        destroyed: Arc<AtomicBool>,
    }

    impl FileSystem for FinishedTransferFs {
        type Inode = u64;
        type Handle = u64;
        type DirIter = EmptyDir;

        fn destroy(&self) {
            self.destroyed.store(true, Ordering::Release);
        }
    }

    impl SerializableFileSystem for FinishedTransferFs {
        fn serialize(&self, _state_pipe: File) -> io::Result<()> {
            self.serialized.store(true, Ordering::Release);
            Ok(())
        }
    }

    struct BlockingTransferFs {
        serialization_started: Arc<AtomicBool>,
    }

    impl FileSystem for BlockingTransferFs {
        type Inode = u64;
        type Handle = u64;
        type DirIter = EmptyDir;
    }

    impl SerializableFileSystem for BlockingTransferFs {
        fn serialize(&self, _state_pipe: File) -> io::Result<()> {
            self.serialization_started.store(true, Ordering::Release);
            loop {
                thread::park();
            }
        }
    }

    fn successful_preflight() -> Result<()> {
        Ok(())
    }

    fn successful_worker_unshare() -> io::Result<()> {
        Ok(())
    }

    static WORKER_UNSHARE_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn counting_worker_unshare() -> io::Result<()> {
        WORKER_UNSHARE_CALLS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn rejected_preflight() -> Result<()> {
        Err(Error::UnshareCloneFs(io::Error::from_raw_os_error(
            libc::EPERM,
        )))
    }

    fn build_backend() -> VhostUserFsBackend<TestFs> {
        VhostUserFsBackendBuilder::default()
            .set_tag(Some("testfs".to_string()))
            .build(TestFs)
            .unwrap()
    }

    fn run_current_test_in_child(test_name: &str, child_env: &str) -> ExitStatus {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--nocapture"])
            .env(child_env, "1")
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);

        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("child test {} did not terminate", test_name);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn backend_with_queue_count(num_request_queues: u16) -> VhostUserFsBackend<TestFs> {
        VhostUserFsBackendBuilder::default()
            .set_tag(Some("testfs".to_string()))
            .set_num_request_queues(num_request_queues)
            .build_with_unshare(TestFs, successful_preflight, successful_worker_unshare)
            .unwrap()
    }

    #[test]
    fn default_queue_topology_is_unchanged() {
        let backend = build_backend();

        assert_eq!(backend.num_queues(), 2);
        assert_eq!(backend.queues_per_thread(), [0b11]);
        assert_eq!(backend.queue_index_for_event(0, 0, 2), Some((0, 0)));
        assert_eq!(backend.queue_index_for_event(0, 1, 2), Some((1, 1)));
        assert_eq!(backend.queue_index_for_event(1, 0, 2), None);
        assert_eq!(backend.queue_index_for_event(0, 2, 2), None);

        let config = backend.get_config(MAX_TAG_LEN as u32, 4);
        assert_eq!(u32::from_le_bytes(config.try_into().unwrap()), 1);
    }

    #[test]
    fn multiqueue_uses_one_worker_per_queue() {
        let backend = backend_with_queue_count(4);

        assert_eq!(backend.num_queues(), 5);
        assert_eq!(backend.queues_per_thread(), [1, 2, 4, 8, 16]);
        for global_queue_index in 0..backend.num_queues() {
            assert_eq!(
                backend.queue_index_for_event(global_queue_index, 0, 1),
                Some((0, global_queue_index))
            );
        }
        assert_eq!(backend.queue_index_for_event(0, 1, 1), None);
        assert_eq!(backend.queue_index_for_event(5, 0, 1), None);

        let config = backend.get_config(MAX_TAG_LEN as u32, 4);
        assert_eq!(u32::from_le_bytes(config.try_into().unwrap()), 4);
    }

    #[test]
    fn multiqueue_daemon_creates_one_epoll_worker_per_queue() {
        let backend = Arc::new(backend_with_queue_count(4));
        let daemon = vhost_user_backend::VhostUserDaemon::new(
            "virtiofsd-multiqueue-test".to_string(),
            backend,
            LoggedMemoryAtomic::new(LoggedMemory::new()),
        )
        .unwrap();

        assert_eq!(daemon.get_epoll_handlers().len(), 5);
        // VhostUserHandler::drop() signals and joins all five workers.
        drop(daemon);
    }

    #[test]
    fn maximum_queue_count_uses_all_bitmap_bits() {
        let backend = backend_with_queue_count(MAX_REQUEST_QUEUES);

        assert_eq!(backend.num_queues(), 64);
        assert_eq!(backend.queues_per_thread().len(), 64);
        assert_eq!(backend.queues_per_thread()[63], 1u64 << 63);
        assert_eq!(backend.queue_index_for_event(63, 0, 1), Some((0, 63)));
    }

    #[test]
    fn thread_pool_preserves_default_single_worker_topology() {
        let backend = VhostUserFsBackendBuilder::default()
            .set_thread_pool_size(2)
            .build_with_unshare(TestFs, successful_preflight, successful_worker_unshare)
            .unwrap();

        assert_eq!(backend.queues_per_thread(), [0b11]);
    }

    #[test]
    fn rejects_unsupported_request_queue_counts() {
        assert!(matches!(
            VhostUserFsBackendBuilder::default()
                .set_num_request_queues(0)
                .build(TestFs),
            Err(Error::InvalidRequestQueueCount(0))
        ));
        assert!(matches!(
            VhostUserFsBackendBuilder::default()
                .set_num_request_queues(MAX_REQUEST_QUEUES + 1)
                .build(TestFs),
            Err(Error::InvalidRequestQueueCount(64))
        ));
    }

    #[test]
    fn multiqueue_builder_runs_unshare_preflight() {
        assert!(matches!(
            VhostUserFsBackendBuilder::default()
                .set_num_request_queues(2)
                .build_with_unshare(TestFs, rejected_preflight, successful_worker_unshare),
            Err(Error::UnshareCloneFs(error)) if error.raw_os_error() == Some(libc::EPERM)
        ));
    }

    #[test]
    fn default_direct_backend_does_not_run_unshare_preflight() {
        VhostUserFsBackendBuilder::default()
            .build_with_unshare(TestFs, rejected_preflight, successful_worker_unshare)
            .expect("the compatible single-worker direct topology does not require CLONE_FS");
    }

    #[test]
    fn direct_multiqueue_worker_uses_injected_unshare() {
        const DESC_TABLE: u64 = 0x1000;
        const AVAIL_RING: u64 = 0x2000;
        const USED_RING: u64 = 0x3000;

        let backend = VhostUserFsBackendBuilder::default()
            .set_num_request_queues(2)
            .build_with_unshare(TestFs, successful_preflight, counting_worker_unshare)
            .unwrap();
        let mem = LoggedMemoryAtomic::new(
            LoggedMemory::from_ranges(&[(GuestAddress(0), 0x4000)]).unwrap(),
        );
        backend.update_memory(mem.clone()).unwrap();
        let vring = FsVring::new(mem, 8).unwrap();
        vring
            .set_queue_info(DESC_TABLE, AVAIL_RING, USED_RING)
            .unwrap();
        vring.set_queue_size(8);
        vring.set_queue_ready(true);
        vring.set_enabled(true);

        WORKER_UNSHARE_CALLS.store(0, Ordering::Relaxed);
        FS_UNSHARED.with(|unshared| unshared.set(false));
        backend
            .handle_event(0, EventSet::IN, std::slice::from_ref(&vring), 0)
            .unwrap();
        FS_UNSHARED.with(|unshared| unshared.set(false));
        assert_eq!(WORKER_UNSHARE_CALLS.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn event_idx_and_memory_use_lock_free_backend_state() {
        let backend = build_backend();
        backend.set_event_idx(true);
        assert!(backend.thread.event_idx.load(Ordering::Acquire));

        let initial = LoggedMemoryAtomic::new(LoggedMemory::new());
        backend.update_memory(initial).unwrap();
        assert_eq!(backend.thread.mem.get().unwrap().memory().num_regions(), 0);

        // A completely different GuestMemoryAtomic must replace the map observed by the backend,
        // not be silently ignored by OnceLock.
        let replacement = LoggedMemoryAtomic::new(
            LoggedMemory::from_ranges(&[(GuestAddress(0), 0x1000)]).unwrap(),
        );
        backend.update_memory(replacement).unwrap();
        assert_eq!(backend.thread.mem.get().unwrap().memory().num_regions(), 1);
    }

    #[test]
    fn stopping_vring_drains_pool_request_without_holding_vring_state_lock() {
        const DESC_TABLE: u64 = 0x1000;
        const AVAIL_RING: u64 = 0x2000;
        const USED_RING: u64 = 0x3000;
        const QUEUE_ENTRIES: u16 = 8;

        let mem = LoggedMemoryAtomic::new(
            LoggedMemory::from_ranges(&[(GuestAddress(0), 0x5000)]).unwrap(),
        );
        let vring = FsVring::new(mem.clone(), QUEUE_ENTRIES).unwrap();
        vring
            .set_queue_info(DESC_TABLE, AVAIL_RING, USED_RING)
            .unwrap();
        vring.set_queue_size(QUEUE_ENTRIES);
        vring.set_queue_ready(true);
        vring.set_enabled(true);
        {
            let mut vring_state = vring.get_mut();
            vring_state.get_queue_mut().set_event_idx(true);
            vring_state.enable_notification().unwrap();
            vring_state.disable_notification().unwrap();
        }
        vring.set_queue_next_avail(5);

        // Model the callback-level permit held from disable_notification() through the matching
        // enable_notification() call.
        let callback_permit = vring
            .try_begin_pool_request()
            .expect("running vring must admit an EVENT_IDX callback");

        let stopping_vring = vring.clone();
        let (stopped_tx, stopped_rx) = mpsc::channel();
        let stopper = thread::spawn(move || {
            // This is the first operation performed by vhost-user-backend for GET_VRING_BASE.
            stopping_vring.set_queue_ready(false);
            stopped_tx.send(()).unwrap();
        });

        // Wait until the stop gate is closed.  At that point set_queue_ready(false) must remain
        // blocked on the outstanding asynchronous request.
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if !vring.inflight.state.lock().unwrap().ready {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "vring stop did not close admission"
            );
            thread::yield_now();
        }
        assert!(stopped_rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert!(vring.try_begin_pool_request().is_none());

        // The stop waiter must neither retain VringState's mutex nor invalidate the underlying
        // queue while the callback still has to re-arm notifications on the old ring.
        assert!(vring.get_ref().get_queue().ready());
        vring.get_mut().enable_notification().unwrap();

        drop(callback_permit);
        stopped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("GET_VRING_BASE stop must finish after the final request completes");
        stopper.join().unwrap();

        let avail_event = mem
            .memory()
            .read_obj::<u16>(GuestAddress(USED_RING + 4 + 8 * u64::from(QUEUE_ENTRIES)))
            .unwrap();
        assert_eq!(u16::from_le(avail_event), 5);
        assert!(!vring.get_ref().get_queue().ready());

        // Reconfiguration reuses this object; reopening the ready gate must admit new work.
        mem.memory()
            .write_obj(5_u16, GuestAddress(AVAIL_RING + 2))
            .unwrap();
        vring.set_queue_ready(true);
        assert!(vring.try_begin_pool_request().is_some());
    }

    #[test]
    fn restarting_vring_self_kicks_pending_work() {
        const DESC_TABLE: u64 = 0x1000;
        const AVAIL_RING: u64 = 0x2000;
        const USED_RING: u64 = 0x3000;

        let mem = LoggedMemoryAtomic::new(
            LoggedMemory::from_ranges(&[(GuestAddress(0), 0x5000)]).unwrap(),
        );
        let vring = FsVring::new(mem.clone(), 8).unwrap();
        vring
            .set_queue_info(DESC_TABLE, AVAIL_RING, USED_RING)
            .unwrap();
        vring.set_queue_size(8);

        let (kick_consumer, _kick_notifier) =
            new_event_consumer_and_notifier(EventFlag::NONBLOCK).unwrap();
        let kick_observer = kick_consumer.try_clone().unwrap();
        vring.set_kick(Some(unsafe {
            // SAFETY: set_kick() takes ownership of this valid eventfd descriptor.
            File::from_raw_fd(kick_consumer.into_raw_fd())
        }));
        vring.set_queue_ready(true);
        vring.set_enabled(true);
        assert!(kick_observer.consume().is_err());

        // Model epoll returning the last kick just before SET_VRING_ENABLE(0), so the eventfd has
        // been consumed while the descriptor itself remains available.
        mem.memory()
            .write_obj(1_u16, GuestAddress(AVAIL_RING + 2))
            .unwrap();
        vring.set_enabled(false);
        vring.set_enabled(true);
        kick_observer.consume().unwrap();

        // GET_VRING_BASE clears and later replaces the kick fd.  Reopening the ready gate must
        // perform the same pending-work double-check with the replacement eventfd.
        vring.set_queue_next_avail(1);
        mem.memory()
            .write_obj(2_u16, GuestAddress(AVAIL_RING + 2))
            .unwrap();
        vring.set_queue_ready(false);
        vring.set_kick(None);
        let (replacement_consumer, _replacement_notifier) =
            new_event_consumer_and_notifier(EventFlag::NONBLOCK).unwrap();
        let replacement_observer = replacement_consumer.try_clone().unwrap();
        vring.set_kick(Some(unsafe {
            // SAFETY: set_kick() takes ownership of this valid eventfd descriptor.
            File::from_raw_fd(replacement_consumer.into_raw_fd())
        }));
        vring.set_queue_ready(true);
        replacement_observer.consume().unwrap();
    }

    #[test]
    fn stale_event_for_stopped_vring_without_kick_is_ignored() {
        let backend = build_backend();
        let mem = LoggedMemoryAtomic::new(LoggedMemory::new());
        let vring = FsVring::new(mem, 8).unwrap();
        // GET_VRING_BASE clears ready before removing the kick fd but does not necessarily clear
        // the separate enabled bit.  An event already returned by epoll may still reach us.
        vring.set_enabled(true);
        vring.set_queue_ready(false);

        backend.thread.lifecycle.pause();
        assert!(backend
            .handle_event(0, EventSet::IN, std::slice::from_ref(&vring), 0)
            .is_ok());
        backend.thread.lifecycle.resume();
        assert!(VhostUserFsThread::<TestFs>::clone_queue_kick(0, &vring).is_none());
    }

    #[test]
    fn request_gate_is_atomic_and_drain_has_no_lost_wakeup() {
        let lifecycle = Arc::new(RequestLifecycle::new());
        let active = lifecycle.start_request().unwrap();

        lifecycle.pause();
        assert!(lifecycle.start_request().is_none());
        assert_eq!(
            lifecycle.active_state.load(Ordering::Acquire),
            REQUESTS_PAUSED | 1
        );

        let drain_lifecycle = Arc::clone(&lifecycle);
        let (drained_tx, drained_rx) = mpsc::channel();
        let drainer = thread::spawn(move || {
            drain_lifecycle.wait_for_drain();
            drained_tx.send(()).unwrap();
        });

        assert!(drained_rx.recv_timeout(Duration::from_millis(20)).is_err());
        drop(active);
        drained_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            lifecycle.active_state.load(Ordering::Acquire),
            REQUESTS_PAUSED
        );

        lifecycle.resume();
        assert_eq!(lifecycle.active_state.load(Ordering::Acquire), 0);
        assert!(lifecycle.start_request().is_some());
        drainer.join().unwrap();
    }

    #[test]
    fn reset_cancels_premigration_and_clears_the_thread() {
        let backend = build_backend();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_observer = Arc::clone(&cancel);
        let worker_cancel = Arc::clone(&cancel);
        let handle = thread::spawn(move || {
            while !worker_cancel.load(Ordering::Relaxed) {
                thread::yield_now();
            }
        });
        *backend.premigration_thread.lock().unwrap() = Some(PremigrationThread { handle, cancel });

        backend.reset_device();
        assert!(cancel_observer.load(Ordering::Relaxed));
        assert!(backend.premigration_thread.lock().unwrap().is_none());
    }

    #[test]
    fn reset_takes_over_a_completed_transfer_that_was_not_checked() {
        let serialized = Arc::new(AtomicBool::new(false));
        let destroyed = Arc::new(AtomicBool::new(false));
        let backend = VhostUserFsBackendBuilder::default()
            .build(FinishedTransferFs {
                serialized: Arc::clone(&serialized),
                destroyed: Arc::clone(&destroyed),
            })
            .unwrap();

        backend
            .do_set_device_state_fd(
                VhostTransferStateDirection::SAVE,
                VhostTransferStatePhase::STOPPED,
                TempFile::new().unwrap().into_file(),
            )
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let finished = backend
                .migration_thread
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_finished();
            if finished {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "device-state transfer did not finish"
            );
            thread::yield_now();
        }

        backend.reset_device();

        assert!(serialized.load(Ordering::Acquire));
        assert!(destroyed.load(Ordering::Acquire));
        assert!(backend.migration_thread.lock().unwrap().is_none());
        assert!(backend.thread.lifecycle.start_request().is_some());
    }

    #[test]
    fn reset_during_a_running_transfer_fails_the_daemon() {
        const CHILD_ENV: &str = "VIRTIOFSD_TEST_RESET_DURING_TRANSFER_CHILD";
        const TEST_NAME: &str =
            "vhost_user::tests::reset_during_a_running_transfer_fails_the_daemon";

        if std::env::var_os(CHILD_ENV).is_some() {
            let serialization_started = Arc::new(AtomicBool::new(false));
            let backend = VhostUserFsBackendBuilder::default()
                .build(BlockingTransferFs {
                    serialization_started: Arc::clone(&serialization_started),
                })
                .unwrap();
            backend
                .do_set_device_state_fd(
                    VhostTransferStateDirection::SAVE,
                    VhostTransferStatePhase::STOPPED,
                    TempFile::new().unwrap().into_file(),
                )
                .unwrap();

            let deadline = Instant::now() + Duration::from_secs(1);
            while !serialization_started.load(Ordering::Acquire) {
                assert!(
                    Instant::now() < deadline,
                    "device-state transfer did not start"
                );
                thread::yield_now();
            }

            backend.reset_device();
            panic!("RESET_DEVICE incorrectly returned while state transfer was running");
        }

        let status = run_current_test_in_child(TEST_NAME, CHILD_ENV);
        assert_eq!(status.code(), Some(1));
    }

    #[test]
    fn stale_event_idx_callback_replays_pending_work_for_current_session() {
        const DESC_TABLE: u64 = 0x1000;
        const AVAIL_RING: u64 = 0x2000;
        const USED_RING: u64 = 0x3000;

        let backend = VhostUserFsBackendBuilder::default()
            .set_thread_pool_size(1)
            .build_with_unshare(TestFs, successful_preflight, successful_worker_unshare)
            .unwrap();
        let lifecycle = Arc::clone(&backend.thread.lifecycle);
        let old_session_generation = lifecycle.current_session_generation();
        let mem = LoggedMemoryAtomic::new(
            LoggedMemory::from_ranges(&[(GuestAddress(0), 0x5000)]).unwrap(),
        );
        backend.update_memory(mem.clone()).unwrap();
        backend.set_event_idx(true);

        let vring = FsVring::new(mem.clone(), 8).unwrap();
        vring
            .set_queue_info(DESC_TABLE, AVAIL_RING, USED_RING)
            .unwrap();
        vring.set_queue_size(8);
        vring.set_queue_ready(true);
        vring.set_enabled(true);
        vring.get_mut().get_queue_mut().set_event_idx(true);

        let (kick_consumer, _kick_writer) =
            new_event_consumer_and_notifier(EventFlag::NONBLOCK).unwrap();
        let kick_observer = kick_consumer.try_clone().unwrap();
        vring.set_kick(Some(unsafe {
            // SAFETY: set_kick() takes ownership of this valid eventfd descriptor.
            File::from_raw_fd(kick_consumer.into_raw_fd())
        }));

        lifecycle.pause();
        lifecycle.discard_queued_kicks();
        lifecycle.resume();
        mem.memory()
            .write_obj(1_u16, GuestAddress(AVAIL_RING + 2))
            .unwrap();

        backend
            .thread
            .handle_event_pool(
                0,
                REQUEST_QUEUE_INDEX_BASE,
                std::slice::from_ref(&vring),
                old_session_generation,
            )
            .unwrap();

        // The stale callback must not consume the descriptor, but enable_notification() observes
        // it and the callback re-signals the current kick for a new-generation dispatcher.
        assert_eq!(vring.queue_next_avail(), 0);
        kick_observer.consume().unwrap();

        // Also model reset advancing the generation only after process_queue_pool() had chosen an
        // outcome.  The post-enable generation check is outcome-independent, so retry=true still
        // transfers pending work to a current-session callback.
        assert!(backend.thread.replay_event_idx_after_session_change(
            REQUEST_QUEUE_INDEX_BASE,
            &vring,
            old_session_generation,
            true,
        ));
        kick_observer.consume().unwrap();
    }

    #[test]
    fn deferred_kicks_are_deduplicated_refreshed_and_replayed_after_final_resume() {
        let lifecycle = RequestLifecycle::new();
        lifecycle.pause();
        lifecycle.pause();

        let (first_consumer, first_notifier) =
            new_event_consumer_and_notifier(EventFlag::NONBLOCK).unwrap();
        let (duplicate_consumer, duplicate_notifier) =
            new_event_consumer_and_notifier(EventFlag::NONBLOCK).unwrap();
        lifecycle.defer_kick(7, first_notifier);
        lifecycle.defer_kick(7, duplicate_notifier);
        assert_eq!(lifecycle.state.lock().unwrap().deferred_kicks.len(), 1);

        lifecycle.resume();
        assert!(first_consumer.consume().is_err());
        assert!(duplicate_consumer.consume().is_err());
        lifecycle.resume();
        // The latest kick generation replaces the old notifier for the same queue key.
        assert!(first_consumer.consume().is_err());
        duplicate_consumer.consume().unwrap();
    }

    #[test]
    fn defer_kick_after_resume_replays_immediately() {
        let lifecycle = RequestLifecycle::new();
        lifecycle.pause();
        lifecycle.resume();

        let (consumer, notifier) = new_event_consumer_and_notifier(EventFlag::NONBLOCK).unwrap();
        lifecycle.defer_kick(11, notifier);
        consumer.consume().unwrap();
    }
}
