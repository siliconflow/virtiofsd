// Copyright 2026 The Virtiofs Project Developers
//
// SPDX-License-Identifier: (Apache-2.0 AND BSD-3-Clause)

use std::collections::BTreeMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::Handle;

/// Match the maximum number of virtqueues supported by vhost-user-backend's
/// queue-to-worker bitmap. Handles are allocated sequentially, so masking the
/// low bits distributes concurrently opened handles evenly across shards.
const HANDLE_SHARD_COUNT: usize = 64;

pub(super) struct HandleStore<T> {
    shards: [RwLock<BTreeMap<Handle, T>>; HANDLE_SHARD_COUNT],
}

impl<T> Default for HandleStore<T> {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(BTreeMap::new())),
        }
    }
}

impl<T> HandleStore<T> {
    fn shard_index(handle: Handle) -> usize {
        handle as usize & (HANDLE_SHARD_COUNT - 1)
    }

    fn shard(&self, handle: Handle) -> &RwLock<BTreeMap<Handle, T>> {
        &self.shards[Self::shard_index(handle)]
    }

    /// Lock every shard in ascending order.
    ///
    /// Regular handle operations lock only one shard.  Cold-path operations
    /// that need a point-in-time view lock all shards in this fixed order, so
    /// two such operations cannot deadlock with each other.
    fn read_all(&self) -> Vec<RwLockReadGuard<'_, BTreeMap<Handle, T>>> {
        self.shards
            .iter()
            .map(|shard| shard.read().unwrap())
            .collect()
    }

    fn write_all(&self) -> Vec<RwLockWriteGuard<'_, BTreeMap<Handle, T>>> {
        self.shards
            .iter()
            .map(|shard| shard.write().unwrap())
            .collect()
    }

    pub(super) fn insert(&self, handle: Handle, value: T) -> Option<T> {
        self.shard(handle).write().unwrap().insert(handle, value)
    }

    pub(super) fn remove_if<P>(&self, handle: Handle, predicate: P) -> bool
    where
        P: FnOnce(&T) -> bool,
    {
        let mut shard = self.shard(handle).write().unwrap();
        let should_remove = match shard.get(&handle) {
            Some(value) => predicate(value),
            None => false,
        };

        if should_remove {
            shard.remove(&handle);
            true
        } else {
            false
        }
    }

    pub(super) fn clear(&self) {
        // Acquire all locks before changing the first shard.  This makes clear
        // linearizable with insert(), get(), remove_if(), and snapshotting.
        let mut shards = self.write_all();
        for shard in &mut shards {
            shard.clear();
        }
    }
}

impl<T: Clone> HandleStore<T> {
    pub(super) fn get(&self, handle: Handle) -> Option<T> {
        self.shard(handle).read().unwrap().get(&handle).cloned()
    }

    /// Return a deterministic migration snapshot.
    pub(super) fn snapshot(&self) -> Vec<(Handle, T)> {
        let shards = self.read_all();
        let mut entries = Vec::new();
        for shard in &shards {
            entries.extend(shard.iter().map(|(handle, value)| (*handle, value.clone())));
        }
        entries.sort_unstable_by_key(|(handle, _)| *handle);
        entries
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn basic_operations() {
        let store = HandleStore::default();

        assert_eq!(store.insert(1, 10), None);
        assert_eq!(store.insert(65, 20), None);
        assert_eq!(store.insert(2, 30), None);
        assert_eq!(store.get(1), Some(10));
        assert_eq!(store.get(65), Some(20));
        assert_eq!(store.get(2), Some(30));
        assert!(!store.remove_if(1, |value| *value == 99));
        assert!(store.remove_if(1, |value| *value == 10));
        assert_eq!(store.get(1), None);

        store.clear();
        assert_eq!(store.get(65), None);
        assert_eq!(store.get(2), None);
    }

    #[test]
    fn snapshot_is_sorted_across_shards() {
        let store = HandleStore::default();
        for handle in [129, 63, 2, 64, 1, 0, 65] {
            store.insert(handle, handle * 10);
        }

        let handles: Vec<_> = store
            .snapshot()
            .into_iter()
            .map(|(handle, _)| handle)
            .collect();
        assert_eq!(handles, [0, 1, 2, 63, 64, 65, 129]);
    }

    #[test]
    fn supports_concurrent_shard_access() {
        let store = Arc::new(HandleStore::default());
        let threads: Vec<_> = (0..8)
            .map(|thread_index| {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    for offset in 0..128 {
                        let handle = thread_index * 128 + offset;
                        store.insert(handle, handle);
                        assert_eq!(store.get(handle), Some(handle));
                    }
                })
            })
            .collect();

        for thread in threads {
            thread.join().unwrap();
        }

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 8 * 128);
    }

    #[test]
    fn conditional_removal_is_atomic_for_same_key() {
        let store = Arc::new(HandleStore::default());
        store.insert(63, 17);

        // A release carrying the wrong inode must not consume the handle.
        assert!(!store.remove_if(63, |inode| *inode == 18));
        assert_eq!(store.get(63), Some(17));

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let store = Arc::clone(&store);
                thread::spawn(move || store.remove_if(63, |inode| *inode == 17))
            })
            .collect();
        let removed = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(|removed| *removed)
            .count();

        assert_eq!(removed, 1);
        assert_eq!(store.get(63), None);
    }

    struct BlockingClone {
        clone_started: Arc<Barrier>,
        clone_release: Arc<Barrier>,
        block: bool,
    }

    impl Clone for BlockingClone {
        fn clone(&self) -> Self {
            if self.block {
                self.clone_started.wait();
                self.clone_release.wait();
            }
            Self {
                clone_started: Arc::clone(&self.clone_started),
                clone_release: Arc::clone(&self.clone_release),
                block: false,
            }
        }
    }

    #[test]
    fn snapshot_locks_all_shards_before_cloning() {
        let store = Arc::new(HandleStore::default());
        let clone_started = Arc::new(Barrier::new(2));
        let clone_release = Arc::new(Barrier::new(2));
        store.insert(
            0,
            BlockingClone {
                clone_started: Arc::clone(&clone_started),
                clone_release: Arc::clone(&clone_release),
                block: true,
            },
        );
        store.insert(
            63,
            BlockingClone {
                clone_started: Arc::clone(&clone_started),
                clone_release: Arc::clone(&clone_release),
                block: false,
            },
        );

        let snapshot_store = Arc::clone(&store);
        let snapshot_thread = thread::spawn(move || snapshot_store.snapshot());
        clone_started.wait();

        let (attempted_tx, attempted_rx) = mpsc::channel();
        let (removed_tx, removed_rx) = mpsc::channel();
        let remove_store = Arc::clone(&store);
        let remove_thread = thread::spawn(move || {
            attempted_tx.send(()).unwrap();
            let removed = remove_store.remove_if(63, |_| true);
            removed_tx.send(removed).unwrap();
        });

        attempted_rx.recv().unwrap();
        let removal_while_snapshotting = removed_rx.recv_timeout(Duration::from_millis(100));
        clone_release.wait();

        let snapshot = snapshot_thread.join().unwrap();
        assert_eq!(snapshot.len(), 2);
        assert!(matches!(
            removal_while_snapshotting,
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(removed_rx.recv().unwrap());
        remove_thread.join().unwrap();
    }

    struct BlockingDrop {
        drop_started: Option<Arc<Barrier>>,
        drop_release: Option<Arc<Barrier>>,
    }

    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            if let (Some(started), Some(release)) = (&self.drop_started, &self.drop_release) {
                started.wait();
                release.wait();
            }
        }
    }

    #[test]
    fn clear_holds_all_shards_while_dropping_entries() {
        let store = Arc::new(HandleStore::default());
        let drop_started = Arc::new(Barrier::new(2));
        let drop_release = Arc::new(Barrier::new(2));
        store.insert(
            0,
            BlockingDrop {
                drop_started: Some(Arc::clone(&drop_started)),
                drop_release: Some(Arc::clone(&drop_release)),
            },
        );
        store.insert(
            63,
            BlockingDrop {
                drop_started: None,
                drop_release: None,
            },
        );

        let clear_store = Arc::clone(&store);
        let clear_thread = thread::spawn(move || clear_store.clear());
        drop_started.wait();

        let (attempted_tx, attempted_rx) = mpsc::channel();
        let (removed_tx, removed_rx) = mpsc::channel();
        let remove_store = Arc::clone(&store);
        let remove_thread = thread::spawn(move || {
            attempted_tx.send(()).unwrap();
            let removed = remove_store.remove_if(63, |_| true);
            removed_tx.send(removed).unwrap();
        });

        attempted_rx.recv().unwrap();
        let removal_while_clearing = removed_rx.recv_timeout(Duration::from_millis(100));
        drop_release.wait();

        clear_thread.join().unwrap();
        assert!(matches!(
            removal_while_clearing,
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(!removed_rx.recv().unwrap());
        remove_thread.join().unwrap();
    }
}
