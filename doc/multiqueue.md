# Rust virtiofsd 多请求队列设计与实现

本文说明本仓库的 virtio-fs 多请求队列实现、上下游现状、并发设计、兼容边界和验证方法。

文档状态：2026-09-03。这里描述的是本仓库分支，不代表功能已合入 virtiofsd 上游。

## 协议模型

[Virtio FS 规范]定义：

- queue 0 是 high-priority（hiprio）队列；
- queue 1..N 是功能相同的 request 队列；
- config space 的 `num_request_queues` 表示 request 队列数，至少为 1；
- request 队列之间不保证完成顺序，guest 必须自行维护有依赖请求的先后关系。

本实现没有协商可选的 notification queue，因此配置 N 个 request queue 时，
backend 总共暴露 N+1 个 virtqueue：

| 全局队列号 | 用途 |
| --- | --- |
| 0 | hiprio |
| 1 | request[0] |
| 2 | request[1] |
| ... | ... |
| N | request[N-1] |

规范把 `FUSE_FORGET`、`FUSE_BATCH_FORGET` 和 `FUSE_INTERRUPT` 列为 hiprio
用途。需要注意，当前 Linux 上游驱动实际会从 hiprio 发送 FORGET，
但 `virtio_fs_send_interrupt()` 仍是 TODO/no-op。因此 INTERRUPT 目前只能视为
协议设计能力，不能作为 Linux guest 集成测试的通过条件。

## 上下游现状

### Linux guest

Linux commit [529395d2ae6456] 加入按 CPU/transport affinity 选择 request queue 的
逻辑，首次进入上游 Linux 6.10。驱动先尝试 PCI MSI-X 等 transport affinity，
失败时按 CPU 均匀分组，再失败则让所有 CPU 使用 request[0]。

不能只看 `uname -r`：发行版可能回移植，也可能没有回移植。确认正在运行的 kernel
package 所对应源码包含以下特征：

- `struct virtio_fs` 含 `mq_map`；
- 存在 `virtio_fs_map_queues()`；
- 普通请求发送路径按当前 CPU 从 `mq_map` 选择 queue。

当前上游发送函数是 `virtio_fs_send_req()`，其中 debug 日志带 `queue_id`。
早期提交或部分 backport 可能把同一逻辑放在
`virtio_fs_wake_pending_and_unlock()`，检查发行版源码时应同时搜索两个函数名。

运行时可用两种方式确认实际分发：

1. 对当前源码中的发送函数启用 dynamic debug，固定 fio job 到不同 vCPU，检查日志
   是否出现多个 request queue ID。
2. 比较 I/O 前后的 `/proc/interrupts`，确认多条 virtio-fs request queue 的中断
   计数都在增长。若多个队列共享 IRQ，以 dynamic debug 或 trace 为准。

不含该提交的 guest 通常仍可挂载：旧驱动只使用 request[0]，额外队列闲置。这是功能
回退，不会自动获得多队列性能。

### QEMU

QEMU 的 `vhost-user-fs-pci`/vhost-user-fs 设备已有
`num-request-queues` 属性，默认 1。QEMU 创建一条 hiprio queue 和指定数量的
request queue，并把 QEMU 配置值写入 guest 可见的 Virtio FS config space。

QEMU 通过 vhost-user `GET_QUEUE_NUM` 查询 backend 上限。协议检查是：

```text
backend total queues >= QEMU total queues
```

因此 daemon 暴露的队列多于 QEMU 时可以启动，只是多余 worker/FD 不会被使用；
daemon 少于 QEMU 时初始化失败。生产配置仍建议两端填写相同值，以避免误配和资源
浪费。

迁移时 source/destination 的 QEMU 设备拓扑必须兼容；每个 daemon 暴露的队列数必须
不少于其对应 QEMU。四端数值完全相同是最简单、最可审计的推荐配置，但不是
`GET_QUEUE_NUM` 协议强制的等式。

### virtiofsd 上游

截至本文日期，virtiofsd 上游 `main` 的 `vhost_user.rs` 仍固定为一个 request
queue，上游 [#159 Multiqueue] 仍在跟踪该功能。已有的
[Allow multiqueue 原型]使用 `queues_per_thread()` 证明了一队列一 worker 的可行性，
但测试出现较高方差和部分吞吐回退，说明仅增加队列不足以消除 backend 内部竞争。

## 本仓库的实现

### 配置与上限

daemon 新增：

```text
--num-request-queues <1..=63>
```

默认值为 1。builder API：

```rust
VhostUserFsBackendBuilder::default()
    .set_num_request_queues(4)
    .build(fs)?;
```

上限 63 来自当前 `vhost-user-backend 0.22` 的 `u64` queue mask：bit 0 留给
hiprio，最多剩 63 个 request queue。

### Worker 拓扑

默认 N=1 时保持原拓扑，无论是否启用 request thread pool：

```text
queues_per_thread() = [0b11]

worker 0
  +-- local event 0 -> global queue 0 -> hiprio
  `-- local event 1 -> global queue 1 -> request[0]
```

N>1 时每条队列使用独立 epoll worker。例如 N=4：

```text
num_queues()      = 5
queues_per_thread = [0b00001, 0b00010, 0b00100, 0b01000, 0b10000]

worker 0 -> hiprio
worker 1 -> request[0]
worker 2 -> request[1]
worker 3 -> request[2]
worker 4 -> request[3]
```

`handle_event()` 收到的是 worker 内的 local event index，不是全局 queue index：

- 默认拓扑用 local event 0/1 映射 hiprio/request[0]；
- N>1 时每个 worker 只有 local event 0，`thread_id` 就是全局 queue index；
- 越界组合返回错误，不会访问错误 vring。

### Direct 与 request pool

| 配置 | vring worker | FUSE 请求执行位置 | 执行并发 |
| --- | --- | --- | --- |
| N=1, pool=0 | 1 个共享 worker | vring worker | 1 |
| N>1, pool=0 | 每条 queue 一个 worker | 各 vring worker | 最多 N+1 |
| 任意 N, pool>0 | N=1 共享，否则每 queue 一个 | 共享 futures pool | 受 pool size 限制 |

pool 模式保留原有调度语义：vring worker 从 ring 取出请求后提交到共享 futures
`ThreadPool`，不另加 backend 全局容量锁或第二套 waiter/backpressure。这样不会在
多队列热路径重新引入单一 `Mutex`；代价是 pool 的待执行队列仍可增长，而且 hiprio
与普通请求共享执行线程，不提供严格的优先级或延迟保证。

需要 hiprio 与普通请求真正独立执行时，应使用 `--thread-pool-size=0` 的多队列
direct 模式，并在目标 workload 上验证。

### CLONE_FS

Linux 线程可能共享 cwd、root 和 umask 所在的 `fs_struct`。virtiofsd 的部分
xattr/ACL/凭据路径会临时改变进程上下文，多条 direct worker 不能共享该状态。

- N>1 且 pool=0：每个 vring worker 首次处理事件前执行一次
  `unshare(CLONE_FS)`。
- pool>0：各 pool worker 启动时执行 `unshare(CLONE_FS)`，沿用原语义。
- 构建 backend 时在临时线程预检 syscall；容器策略拒绝时启动立即失败。
- 默认 N=1、pool=0 不执行预检，保持原配置兼容。

预检在线程中运行，避免永久改变调用 builder 的线程。测试注入同时覆盖预检函数和
worker 函数，不会在 Rust test harness 线程执行真实 `unshare`。

### 热路径锁

原实现用一个 `RwLock<VhostUserFsThread>` 包住 backend 状态。现在按字段拆分：

| 状态 | 实现 |
| --- | --- |
| guest memory | 一次发布的 `OnceLock<GuestMemoryAtomic>` |
| EVENT_IDX | `AtomicBool` |
| backend request channel | 独立窄粒度 `RwLock<Option<Backend>>` |
| FUSE server / pool | 构建后不可变共享 |

迁移/reset 的全局请求排空用原子 pause+active 计数。全局 lifecycle 的普通请求
admission 和 completion 只操作原子值；控制面 `Mutex` 只在 pause、等待排空和暂存
pause 期间的 kick 时使用。pool 模式还会取得当前 vring 的局部 permit，但不同队列不
共享这把锁。这避免了所有 request queue 在每个 descriptor 上争用同一把锁。

passthrough 的 handle map 从一个
`RwLock<BTreeMap<...>>` 改为 64 个固定 shard：

```text
shard = handle & 63
```

lookup/insert/release 只锁目标 shard。迁移快照、恢复和 destroy 是冷路径，按固定顺序
获取所有 shard；快照按 handle ID 排序，保持原 BTreeMap 的确定性序列化顺序。
V1/V2 wire format、字段和版本均未改变。

### Vring 停止与排空

request pool 的任务会在 vring worker 返回 epoll 后继续执行。若 frontend 此时执行
`GET_VRING_BASE`、`SET_VRING_ENABLE(0)` 或 reset，不能在异步任务写 used ring
之前回收/重配 vring。

`DrainingVring` 为每条 vring 维护独立 ready/enabled gate 和 in-flight 计数：

1. pool 请求在推进 `next_avail` 前取得该 vring 的 permit；
2. 任务写完 used ring 并通知 guest 后释放 permit；
3. 停止队列先关闭 admission，再等待本 vring 的 permit 清零并重新启用 notification；
4. 队列重新变为 ready+enabled 时再次比较 `avail_idx` 和 `next_avail`，若仍有 pending
   descriptor，则 self-kick 当前 eventfd。

不同队列没有共享这把锁。direct 模式仍由 vring state lock 线性化 descriptor 消费与
队列停止。

EVENT_IDX 路径在重新启用通知后复查 pending work 和 device session generation，避免
reset 前已进入的旧 callback 消费重配置后的 ring。迁移/reset pause 期间已经被 epoll
消费的 kick 按 queue 去重暂存；恢复时重放，reset 则丢弃旧 session 的 kick。

## 使用示例

```shell
virtiofsd \
    --socket-path=/tmp/vfsd.sock \
    --shared-dir=/mnt \
    --num-request-queues=4

qemu-system-x86_64 \
    ... \
    -chardev socket,id=char0,path=/tmp/vfsd.sock \
    -device vhost-user-fs-pci,chardev=char0,tag=myfs,num-request-queues=4
```

建议从 `min(vCPU 数, 并行 I/O job 数)` 附近开始测试，不要直接使用 63。多队列会
增加 epoll/exit event、virtqueue eventfd 和 direct 请求的临时 FD 需求；默认单队列
仍沿用原 FD reserve，只有新增队列按增量计入预算。

## 已验证与待验证

单元测试覆盖：

- 1/4/63 request queue 的总数、mask、config bytes 和 event 映射；
- 默认 N=1 在 pool=0/pool>0 下都保持一个 vring worker；
- 0/64 被拒绝；
- direct worker 的 `CLONE_FS` 预检和注入；
- EVENT_IDX、memory 更新和 pause/drain 竞态；
- pool 异步请求在 vring stop 前完成，重启时 pending descriptor 会触发 self-kick；
- HandleStore 跨 shard 操作、并发访问和确定性迁移快照；
- 默认与多队列的 FD reserve 计算。

当前仍缺少真实 QEMU/KVM/guest 集成覆盖。合入前应至少验证：

- 1/2/4 request queue 均可启动、挂载和卸载；
- 新 guest 的多 vCPU 固定并发 I/O 确实使用多条 queue；
- 旧 guest 安全回退到 request[0]；
- pool=0 和 pool>0；
- read/write/create/unlink/xattr/ACL/FORGET；
- 相同 QEMU queue 拓扑下的迁移。

Linux 当前不发送 virtio-fs INTERRUPT，所以不能用它验证 hiprio；可通过 FORGET 流量、
queue tracing 和饱和普通 request queue 时的 hiprio 完成情况验证。

性能没有硬件无关的“裸盘百分比”。建议同一主机每组至少运行 5 次 30 秒 fio，比较
1 queue/1 job、1 queue/4 jobs、4 queues/4 jobs，并记录 median、p99、virtiofsd CPU、
context switch 和每队列分布。再用 `perf record`、`perf lock contention`、
`perf stat` 确认外层 backend 锁消失且 handle lock 等待分散。

## 已知边界

- 不自动设置 CPU affinity 或 NUMA pinning。
- 不使用 io_uring。
- 不重构 inode 多索引 store 和 per-handle 文件锁。
- pool 模式没有新增有界提交队列或 hiprio 专用执行池。
- 本 PR 不改变迁移 wire format，也不解决所有既存迁移语义问题。
- FD reserve 只覆盖可由本实现枚举的 queue/worker/并发增量；file-handle 模式按
  mount 长期保留的 MountFd 等既存资源仍需通过实际 workload 监控。

## 参考资料

- [Virtio FS 规范]
- [Linux multiqueue commit 529395d2ae6456]
- [Linux virtio-fs driver 当前源码]
- [QEMU vhost-user-fs 当前源码]
- [QEMU vhost-user protocol]
- [virtiofsd 上游 #159 Multiqueue][#159 Multiqueue]
- [virtiofsd 上游 vhost_user.rs]
- [Allow multiqueue 原型]
- [vhost-user-backend 0.22 queues_per_thread()]
- [unshare(2) / CLONE_FS]

[Virtio FS 规范]: https://github.com/oasis-tcs/virtio-spec/blob/master/device-types/fs/description.tex
[529395d2ae6456]: https://github.com/torvalds/linux/commit/529395d2ae6456c556405016ea0c43081fe607f3
[Linux multiqueue commit 529395d2ae6456]: https://github.com/torvalds/linux/commit/529395d2ae6456c556405016ea0c43081fe607f3
[Linux virtio-fs driver 当前源码]: https://github.com/torvalds/linux/blob/master/fs/fuse/virtio_fs.c
[QEMU vhost-user-fs 当前源码]: https://github.com/qemu/qemu/blob/master/hw/virtio/vhost-user-fs.c
[QEMU vhost-user protocol]: https://www.qemu.org/docs/master/interop/vhost-user.html
[#159 Multiqueue]: https://gitlab.com/virtio-fs/virtiofsd/-/issues/159
[virtiofsd 上游 vhost_user.rs]: https://gitlab.com/virtio-fs/virtiofsd/-/blob/main/src/vhost_user.rs
[Allow multiqueue 原型]: https://gitlab.com/hreitz/virtiofsd-rs/-/commit/4f0dc95f3edd316e5a75f386f1d9ac9c711665e7
[vhost-user-backend 0.22 queues_per_thread()]: https://docs.rs/vhost-user-backend/0.22.0/vhost_user_backend/trait.VhostUserBackend.html#method.queues_per_thread
[unshare(2) / CLONE_FS]: https://man7.org/linux/man-pages/man2/unshare.2.html
