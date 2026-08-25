virtio-fs 原理与实现架构
=======================

本文介绍 virtio-fs 如何把宿主机目录共享给虚拟机，包括 Guest VFS/FUSE、Virtio
队列、QEMU `vhost-user-fs`、virtiofsd 和 Host 文件系统之间的关系，以及请求、数据、
缓存和通知的完整路径。

一句话概括：

> virtio-fs 使用 FUSE 协议表达文件操作，使用 Virtio virtqueue 传输请求，再通过
> vhost-user 把队列交给独立的 virtiofsd 进程；virtiofsd 最终调用宿主机文件系统
> syscall 完成操作。

它共享的是宿主机目录和目录中的文件对象，不是一块虚拟磁盘。

术语
----

| 名称 | 作用 |
| --- | --- |
| VFS | Guest 内核统一的文件系统接口，承接应用的 `open/read/write/stat` 等操作 |
| FUSE | 定义 LOOKUP、OPEN、READ、WRITE 等文件操作的请求和响应格式 |
| Virtio | Guest 与虚拟设备之间的标准队列和 feature 协商机制 |
| virtio-fs | 使用 FUSE over Virtio 共享目录的虚拟文件系统设备 |
| vhost | 将 Virtio 数据面处理从 VMM 主线程移到专用 backend 的架构 |
| vhost-user | QEMU 与用户态 backend 之间的 Unix socket 控制协议 |
| vhost-user-fs | QEMU 中由 vhost-user backend 提供服务的 virtio-fs 设备 |
| virtiofsd | 解析 FUSE 请求并操作宿主机目录的用户态 backend |

整体架构
--------

```text
Guest userspace
  application
      │ open/read/write/stat/mmap
      ▼
Guest kernel
  VFS -> FUSE -> virtio_fs driver
      │ FUSE request/response in virtqueue
      ▼
Virtual machine boundary
  QEMU vhost-user-fs frontend
      │
      ├── Unix socket: feature、memory、vring 和迁移控制消息
      ├── shared memory: virtqueue descriptor 与请求/响应数据
      `── eventfd: queue kick 和 completion notification
      │
      ▼
Host userspace
  virtiofsd
    descriptor parser -> FUSE server -> passthrough filesystem
      │ openat/readv/writev/statx/getdents64/xattr/...
      ▼
Host kernel
  Host VFS -> ext4/xfs/btrfs/NFS/... -> SSD/remote storage
```

整个系统可以分成三个平面：

* 控制面：QEMU 与 virtiofsd 通过 vhost-user Unix socket 协商设备能力、Guest
  memory table、virtqueue 地址、eventfd 和迁移状态。
* 数据面：FUSE 请求、响应和文件数据位于 Guest 共享内存中的 virtqueue
  descriptor chain。QEMU 通常不解析这些请求。
* 通知面：Guest/QEMU 和 virtiofsd 使用 kick/call eventfd 通知对方队列中出现了新
  descriptor 或 completion。

Guest 侧工作原理
----------------

### 挂载和初始化

QEMU 在 virtio-fs config space 中向 Guest 暴露 tag，例如 `myfs`。Guest 使用该 tag
挂载设备：

```shell
mount -t virtiofs myfs /mnt
```

挂载过程中：

1. Guest virtio bus 发现 virtio-fs 设备。
2. `virtio_fs` driver 建立 FUSE connection。
3. Guest 在一个 request queue 上发送一次 `FUSE_INIT`。
4. Guest 与 virtiofsd 协商 FUSE feature，例如大请求、writeback cache、POSIX ACL
   和 readdirplus。
5. 同一设备的全部 request queue 共享这个 FUSE session，不需要每队列分别发送
   `FUSE_INIT`。

### 从 VFS 操作到 FUSE 请求

应用执行：

```c
fd = open("/mnt/data/file", O_RDONLY);
read(fd, buffer, 4096);
```

Guest VFS 会把路径解析和读操作转换为一组 FUSE 请求，典型顺序是：

```text
FUSE_LOOKUP("data")
FUSE_LOOKUP("file")
FUSE_OPEN(inode)
FUSE_READ(inode, file_handle, offset, size)
FUSE_RELEASE(inode, file_handle)
```

每个请求包含公共 `fuse_in_header`，其中有：

* `unique`：匹配请求和响应的唯一 ID。
* `opcode`：LOOKUP、READ、WRITE 等操作码。
* `nodeid`：Guest 当前使用的 inode ID。
* `uid/gid/pid`：发起操作的 Guest 身份。
* 操作特有的输入数据。

virtio-fs 没有重新设计一套文件操作协议，而是把原本通过 `/dev/fuse` 传输的 FUSE
消息放进 Virtio descriptor chain。

Virtqueue
---------

### virtqueue 是什么

virtqueue 是 Virtio 设备的共享内存消息队列。Guest driver 和 device/backend 通过它
交换 descriptor，而不是为每个请求调用一次 QEMU API 或通过 Unix socket 复制消息。

一条 descriptor 描述一段 Guest 内存：

```text
+----------------+----------------+----------------+----------------+
| guest address  | length         | flags          | next           |
+----------------+----------------+----------------+----------------+
```

主要字段含义：

* `address`：buffer 在 Guest physical address space 中的地址。
* `length`：buffer 长度。
* `flags`：buffer 方向、是否还有下一个 descriptor，以及是否使用 indirect table。
* `next`：descriptor chain 中下一个 descriptor 的下标。

一个请求可能跨越多段不连续内存，因此通常使用 descriptor chain：

```text
descriptor 7
  address -> fuse_in_header
  flags   -> NEXT
  next    -> 12
       │
       ▼
descriptor 12
  address -> operation input / WRITE data
  flags   -> NEXT
  next    -> 3
       │
       ▼
descriptor 3
  address -> response buffer
  flags   -> WRITE
```

这里的 `WRITE` 表示 device 可以写入该 buffer。没有 `WRITE` 的 descriptor 是
device-readable，也就是 Guest 提供给 device 的输入。

### Split virtqueue 的内存布局

Virtio 规范定义 split 和 packed 两种 virtqueue 格式。split virtqueue 把状态分成
三部分：

```text
+----------------------+     Guest 填充 buffer 地址和 chain
| Descriptor Table     |
+----------------------+
            │ head descriptor index
            ▼
+----------------------+     Guest -> device：哪些 chain 可处理
| Available Ring       |
| flags / idx / ring[] |
+----------------------+
            │ backend consumes
            ▼
+----------------------+     device -> Guest：哪些 chain 已完成
| Used Ring            |
| flags / idx / ring[] |
+----------------------+
```

* Descriptor Table 保存 buffer 描述和 descriptor chain。
* Available Ring 由 Guest driver 推进，里面存放待处理 chain 的 head index。
* Used Ring 由 device/backend 推进，里面存放已完成 chain 的 head index 和写入长度。

`avail.idx` 和 `used.idx` 是自然回绕的 16-bit counter，访问 ring entry 时再对 queue
size 取模。双方根据上次处理到的位置和新的 `idx` 判断有多少条目可用，而不是在每个
请求后清空整个 ring。queue size 表示主 Descriptor Table 中的 entry 数，不是请求
字节数；split ring 的 queue size 是 2 的幂，例如 1024。

Guest 在 descriptor 被放入 used ring 之前不能复用对应 buffer。device 也只能处理
Guest 已经发布到 available ring 的 chain。实现必须在发布 `idx` 前完成 descriptor
内容写入，并使用 Virtio 要求的 memory barrier，避免另一端看到未初始化或过期内容。

### 一次请求的所有权转换

一次完整请求的队列时序如下：

```text
Guest driver                              virtiofsd
    │                                         │
    │ 1. 填充 descriptor chain                │
    │ 2. head 写入 available ring             │
    │ 3. 更新 avail.idx                       │
    │ 4. queue kick -------------------------->│
    │                                         │ 5. 读取 descriptor
    │                                         │ 6. 执行 FUSE 请求
    │                                         │ 7. 写响应 buffer
    │                                         │ 8. 写 used ring
    │                                         │ 9. 更新 used.idx
    │<---------------- completion notification│
    │ 10. 回收 descriptor 和 buffer           │
```

在 vhost-user-fs 中：

* queue kick 通常由 Guest notifier/eventfd 送到对应 virtiofsd worker。
* completion notification 通过 call eventfd 触发 Guest notifier。QEMU/KVM 最终将
  它呈现为 Guest MSI-X 中断；配置 irqfd fast path 后不必经过 QEMU 主循环处理每次
  completion。
* 请求和文件数据仍位于共享 Guest memory；eventfd 只传递“队列状态变化”的通知，
  不携带 FUSE payload。

通知不是每个请求都必须触发。Guest 和 device 可以通过 ring flags 抑制通知；协商
`VIRTIO_RING_F_EVENT_IDX` 后，还可以指定处理到哪个 ring index 时才需要通知，从而
减少 eventfd、VM exit 和中断开销。backend 在重新启用通知时必须再次检查 available
ring，避免“检查队列为空”和“启用通知”之间到达的新请求被遗漏。

### Indirect descriptor

一个请求需要很多 scatter-gather buffer 时，可以使用 indirect descriptor：主
Descriptor Table 中只占一个 entry，该 entry 指向内存中的另一张 descriptor table。
这能减少主 ring 的 descriptor 消耗。本仓库会协商
`VIRTIO_RING_F_INDIRECT_DESC`，请求解析逻辑仍把它展开为普通 descriptor chain。

### Packed virtqueue

Packed virtqueue 把 available/used 状态和 descriptor 合并到同一个 ring，通过
available/used bit 和 wrap counter 表示所有权，目标是提高 cache locality 并减少
内存访问。

当前本仓库的 Rust backend 没有发布 `VIRTIO_F_RING_PACKED`，所以实际使用 split
virtqueue。无论使用哪种 ring 格式，上层 FUSE request/response 格式以及 hiprio、
request queue 的职责都不变。

virtqueue 本身只是通信和 buffer ownership 机制，并不等同于线程。一个 worker
可以处理多个 virtqueue，一个 virtqueue 也可以把取出的请求提交到共享 thread pool。
本仓库的多队列实现通过 `queues_per_thread()` 显式决定 queue 与 vring worker 的映射。

### 队列类型

未启用可选 notification queue 时，队列布局是：

| 队列 | 内容 |
| --- | --- |
| queue 0 | hiprio |
| queue 1..N | request queues |

普通 LOOKUP、OPEN、READ、WRITE 和元数据请求进入 request queue。

hiprio queue 主要传输：

* `FUSE_INTERRUPT`
* `FUSE_FORGET`
* `FUSE_BATCH_FORGET`

独立 hiprio queue 的目的，是在普通 request queue 已满或存在慢请求时，仍能处理请求
取消和 inode 引用释放。这里的 INTERRUPT 是 Virtio FS 规范定义的用途；当前 Linux
上游 `virtio_fs_send_interrupt()` 仍是 TODO/no-op，实际使用 hiprio 的主要是 FORGET。

多 request queue 可以把不同 vCPU 发出的请求分配给不同 backend worker。具体设计
参见 [Rust virtiofsd 多请求队列设计与实现](multiqueue.md)。

### virtio-fs descriptor chain

一个请求通常由 device-readable 和 device-writable descriptor 组成：

```text
device-readable
  fuse_in_header
  operation-specific input
  optional write data

device-writable
  fuse_out_header
  operation-specific output
  optional read data
```

“readable/writable”是从 Virtio device，也就是 virtiofsd 的视角描述：

* device-readable：virtiofsd 从 Guest 内存读取。
* device-writable：virtiofsd 向 Guest 内存写入。

Guest 把 descriptor 放入 available ring 后触发 queue kick。virtiofsd 完成请求后把
descriptor head 放入 used ring，并在需要时触发 completion notification。

QEMU vhost-user-fs 的作用
------------------------

QEMU 中的 `vhost-user-fs-pci` 是 Guest 可见设备的 frontend，主要负责：

* 创建 virtio-fs PCI/MMIO 设备和 config space。
* 创建 hiprio/request virtqueue。
* 分配 MSI-X 中断并处理 Guest notifier。
* 协商 Virtio 和 vhost-user protocol feature。
* 将 Guest memory regions 和 virtqueue 地址交给 virtiofsd。
* 将 queue kick/call eventfd 交给 virtiofsd。
* 管理设备启动、停止、reset 和迁移控制流程。

QEMU 通常不解析 FUSE LOOKUP/READ/WRITE，也不替 virtiofsd 调用宿主机文件 syscall。

典型 QEMU 配置为：

```shell
-chardev socket,id=char0,path=/tmp/vfsd.sock \
-device vhost-user-fs-pci,chardev=char0,tag=myfs \
-object memory-backend-memfd,id=mem,size=4G,share=on \
-numa node,memdev=mem
```

`share=on` 很重要：virtiofsd 需要 mmap Guest RAM，才能读取 virtqueue descriptor 和
访问 descriptor 指向的请求/响应 buffer。

vhost-user 控制面
-----------------

virtiofsd 通常先监听 Unix socket：

```shell
virtiofsd \
    --socket-path=/tmp/vfsd.sock \
    --shared-dir=/srv/share
```

QEMU 连接后，通过 vhost-user protocol 发送的主要是控制消息：

```text
GET/SET_FEATURES
GET/SET_PROTOCOL_FEATURES
SET_MEM_TABLE
SET_VRING_NUM
SET_VRING_ADDR
SET_VRING_KICK
SET_VRING_CALL
SET_VRING_ENABLE
```

因此，不能把 vhost-user 理解为“把每个 READ/WRITE 请求通过 Unix socket 发送”。
Unix socket 建立和配置共享数据面，实际 FUSE 消息位于共享 Guest memory 中。

virtiofsd 的处理流程
-------------------

virtiofsd 收到 queue kick 后执行：

1. 从 available ring 获取 descriptor chain。
2. 根据 Guest memory table 把 descriptor 地址转换为 virtiofsd 可访问的地址。
3. 构造请求 `Reader` 和响应 `Writer`。
4. 解析 `fuse_in_header` 和 operation-specific 数据。
5. 根据 opcode 调用 FUSE server 对应方法。
6. passthrough backend 把 Guest inode/handle 转换为宿主机文件对象。
7. 在正确的 Guest UID/GID 语义下执行宿主机 syscall。
8. 将 `fuse_out_header` 和返回数据写入 Guest buffer。
9. 更新 used ring，并根据 EVENT_IDX 等 feature 判断是否需要通知 Guest。

以 READ 为例：

```text
FUSE_READ(nodeid, fh, offset, size)
        │
        ▼
Server::read()
        │ nodeid/fh -> Host File
        ▼
PassthroughFs::read()
        │ preadv/readv directly into Guest-backed iovec
        ▼
Guest response buffer
        │
        ▼
used ring + interrupt
```

这里的 zero-copy reader/writer 主要表示 virtiofsd 不需要先分配一个中间 userspace
buffer 再复制。宿主机内核仍可能在 Host page cache、存储设备和 Guest memory 之间
搬运数据，因此不能简单理解为端到端完全零拷贝。

Inode 与 file handle 映射
-------------------------

Guest 不应直接持有宿主机路径或 FD，所以 virtiofsd 维护自己的对象 ID：

```text
Guest nodeid -> inode data -> O_PATH FD 或 filesystem file handle
Guest fh     -> handle data -> 已打开的 Host file/directory FD
```

inode 表用于 LOOKUP、GETATTR、OPEN 等操作。file handle 表用于已经打开的文件或目录，
READ/WRITE/READDIR 可以直接复用 Host FD。

这种设计有几个作用：

* 避免每次请求都从共享目录根重新解析完整路径。
* 文件 rename 后，已打开对象仍可通过 FD 访问。
* 减少路径解析过程中的 TOCTOU 风险。
* 为 FUSE FORGET、RELEASE 和迁移恢复提供稳定的对象标识。

Guest 执行 `FUSE_FORGET` 时减少 inode lookup 引用；执行 `FUSE_RELEASE` 时释放对应的
open handle。当最后一个 `Arc`/FD 被释放后，宿主机文件描述符自动关闭。

身份、权限与隔离
----------------

FUSE request 带有 Guest UID/GID。virtiofsd 会根据配置执行 UID/GID 映射，并让宿主机
权限检查尽量反映 Guest 发起者身份。

由于 virtiofsd 可以访问 Guest RAM 和导出目录，生产环境通常还需要：

* mount namespace/chroot 或 namespace sandbox。
* seccomp syscall allowlist。
* 尽量少的 Linux capabilities。
* 限制导出目录和文件描述符数量。
* 正确配置 xattr、POSIX ACL 和 security label 映射。

passthrough backend 优先通过目录 FD、`openat`、`O_PATH` 和 `/proc/self/fd` 操作对象，
避免不受约束地解析宿主机绝对路径。

缓存与一致性
------------

virtio-fs 同时可能涉及多层缓存：

```text
Guest dentry/inode/attribute cache
Guest page cache
Host page cache
Host storage cache
```

FUSE reply 中的 entry timeout 和 attribute timeout 控制 Guest 元数据缓存时间。文件
打开响应中的 `DIRECT_IO`、`KEEP_CACHE` 等 flag 控制 Guest 文件数据缓存行为。

本仓库提供以下 cache policy：

| 策略 | 语义 |
| --- | --- |
| `never` | 普通文件 I/O 尽量直接发送给 virtiofsd，不长期缓存数据 |
| `metadata` | 普通文件类似 `never`，但缓存目录、dentry 和 attribute |
| `auto` | 默认 close-to-open consistency，由 Guest 决定何时缓存 |
| `always` | 尽量保留 Guest 文件数据缓存，要求共享目录由 Guest/virtiofsd 独占 |

如果宿主机上的其他进程绕过 virtiofsd 修改共享目录，Guest cache 不一定立即得知。
共享目录存在第三方写入者时，应选择更保守的 cache policy，不能把 `always` 当作通用
性能开关。

DAX
---

Virtio FS 架构支持可选 DAX window。其目标是把 Host 文件页映射到 Guest 地址空间：

```text
Host file page
      │ mapping
      ▼
Guest DAX window
      │
      ▼
Guest application mapping
```

DAX 可以减少 Guest page cache 和普通 READ/WRITE 数据搬运，但会引入映射生命周期、
失效、一致性和内存窗口管理等复杂度。

当前本仓库的 Rust virtiofsd 虽然能解析 `FUSE_SETUPMAPPING` 和
`FUSE_REMOVEMAPPING` opcode，但 server 返回 `ENOSYS`，因此这里描述的是 virtio-fs
架构能力，不是当前实现已经启用的功能。

多队列与并发
------------

单 request queue 容易形成以下瓶颈：

```text
多个 Guest vCPU -> 一个 request queue -> 一个 vring worker
```

包含上游 commit
[`529395d2ae6456`](https://github.com/torvalds/linux/commit/529395d2ae6456c556405016ea0c43081fe607f3)
的 guest driver 可以根据 MSI-X affinity 或 CPU 映射选择 request queue（该提交
首次进入上游 Linux 6.10，发行版也可能回移植）。backend 为每个队列提供独立 worker
后，请求路径可以变为：

```text
vCPU 0 -> request[0] -> worker 1
vCPU 1 -> request[1] -> worker 2
vCPU 2 -> request[2] -> worker 3
vCPU 3 -> request[3] -> worker 4
                    hiprio -> worker 0
```

启用 request thread pool 时，各 vring worker 从 ring 取出请求后提交到同一个
futures `ThreadPool`。本实现没有再叠加一套全局容量锁或 waiter/backpressure，避免
多 request queue 在每个 descriptor 上重新争用同一把 `Mutex`。pool size 限制实际
执行线程数，但待执行任务队列可以增长；hiprio 也共享该 pool，因此这种模式不承诺
严格的 hiprio 优先级。需要队列之间完全独立执行时，应使用 pool=0 的 direct 模式。

worker 在推进 `next_avail` 的同一个 vring 锁域内重新检查 `enabled && ready`，使
descriptor claim 与 `GET_VRING_BASE` 线性化：teardown 若先取得锁，backend 就不会在
frontend 已收到旧 base 后继续消费该 descriptor。serial 和 pool 路径使用相同规则。
pool 请求另持有当前 vring 的局部 permit，队列停止会等待已经取出的异步请求完成；
队列重新变为 ready+enabled 时会双检 `avail_idx` 和 `next_avail`，必要时 self-kick，避免
停止竞态中已经被 epoll 消费的最后一次通知造成 pending descriptor 滞留。
EVENT_IDX 重新启用通知后还会复查 session generation；若旧 callback 跨过 reset 且
double-check 发现 pending descriptor，会重新触发当前 kick，让新 callback 接管。
迁移/reset pause 期间已经被 epoll 消费的 kick 按 queue 去重暂存并在恢复时重放，避免
descriptor 因 guest 不再产生新 kick 而滞留。

多队列提高的是并发上限，不保证吞吐按队列数线性增长。virtiofsd 内部锁、Guest
FUSE 锁、Host filesystem 锁、page cache、SSD queue depth、NUMA 和 CPU 调度都可能
成为新的瓶颈。

不包含该提交的 guest 通常仍能挂载多队列设备，但普通请求固定使用 `request[0]`，
其他 request queue 空闲。如何通过精确内核源码和运行时 IRQ 计数确认能力，参见
[多队列文档](multiqueue.md#linux-guest)。

普通 I/O、Direct I/O 与 DAX
--------------------------

三种路径不要混淆：

| 模式 | 请求控制 | 文件数据路径 |
| --- | --- | --- |
| 普通 I/O | FUSE request queue | Host page cache 与 Guest memory 之间读写 |
| FUSE Direct I/O | FUSE request queue | 绕过 Guest page cache；Host cache 是否绕过取决于 Host FD 是否使用 `O_DIRECT` |
| DAX | FUSE 控制请求 + mapping | Host 文件页映射进 Guest DAX window |

无论数据如何移动，LOOKUP、OPEN、GETATTR、SETATTR、映射建立和释放等控制操作仍需要
FUSE 协议。

错误、取消与中断
----------------

每个普通 FUSE 请求使用 `unique` ID 匹配响应。发生错误时，virtiofsd 在
`fuse_out_header.error` 中返回负 errno。

Virtio FS 协议允许 guest 在 hiprio queue 提交 `FUSE_INTERRUPT(unique)` 来取消慢请求，
但当前 Linux 上游的 virtio-fs driver 尚未实现发送该请求。未来 driver 实现后，是否
能真正取消正在执行的 Host syscall 仍取决于请求类型和执行阶段。

迁移
----

虚拟机迁移不仅要迁移 Guest RAM 和 Virtio 设备状态，还需要恢复 virtiofsd 中的：

* inode ID 与 Host 文件对象的关系。
* Guest open handle 与 Host FD 的关系。
* 已协商的 FUSE session 状态。

virtiofsd 不迁移共享目录中的文件数据。source 和 destination 必须看到一致的目录
内容，具体机制参见 [Migration with virtio-fs](migration.md)。

在 `STOPPED` 阶段传输设备状态时，backend 会暂停新的请求并等待已经接收的请求完成，
避免序列化或恢复状态与请求处理并发。pause 期间已经被 epoll 消费的 virtqueue kick
会在传输完成后重放。设备 reset 会开启新的 session，因此会推进 backend session
generation 并丢弃旧 vring generation 暂存的 kick；延迟到 reset 之后的旧 callback
不能修改或唤醒新队列。普通迁移取消保持原有实现语义；reset 会先取消 preparation，
并清除不能带入新 FUSE session 的 migration tracking 状态。

与其他虚拟存储方案的区别
------------------------

| 方案 | Guest 看到的对象 | Host 导出对象 | 协议层次 |
| --- | --- | --- | --- |
| virtio-blk | 块设备 | 磁盘镜像或块设备 | sector/block I/O |
| virtio-scsi | SCSI 设备 | 磁盘/LUN | SCSI command |
| virtio-9p | 文件系统 | Host 目录 | 9P 文件协议 |
| virtio-fs | 文件系统 | Host 目录 | FUSE over Virtio |

virtio-blk 不理解目录、inode、xattr 或 POSIX ACL，Guest 必须在虚拟块设备上维护自己的
文件系统。virtio-fs 则直接暴露 Host 文件对象，因此更适合 VM 与 Host 共享目录。

最小启动示例
------------

启动 backend：

```shell
virtiofsd \
    --socket-path=/tmp/vfsd.sock \
    --shared-dir=/srv/share
```

启动 QEMU：

```shell
qemu-system-x86_64 \
    ... \
    -chardev socket,id=char0,path=/tmp/vfsd.sock \
    -device vhost-user-fs-pci,chardev=char0,tag=myfs \
    -object memory-backend-memfd,id=mem,size=4G,share=on \
    -numa node,memdev=mem
```

Guest 挂载：

```shell
mount -t virtiofs myfs /mnt
```

本仓库代码位置
--------------

* vhost-user backend、virtqueue 和 event handling：
  [`src/vhost_user.rs`](../src/vhost_user.rs)
* virtqueue descriptor reader/writer：
  [`src/descriptor_utils.rs`](../src/descriptor_utils.rs)
* FUSE opcode dispatch 和请求编解码：
  [`src/server.rs`](../src/server.rs)
* FUSE 协议结构和常量：[`src/fuse.rs`](../src/fuse.rs)
* Host filesystem passthrough：
  [`src/passthrough/mod.rs`](../src/passthrough/mod.rs)
* sandbox 和 namespace：[`src/sandbox.rs`](../src/sandbox.rs)
* 多请求队列设计：[`doc/multiqueue.md`](multiqueue.md)
* 迁移设计：[`doc/migration.md`](migration.md)

参考资料
--------

* [Virtio FS specification]
* [Virtio split virtqueue specification]
* [Virtio packed virtqueue specification]
* [FUSE kernel documentation]
* [Linux virtio-fs driver]
* [QEMU vhost-user-fs source]
* [QEMU vhost-user protocol]
* [virtio-fs project]

[Virtio FS specification]: https://github.com/oasis-tcs/virtio-spec/blob/master/device-types/fs/description.tex
[Virtio split virtqueue specification]: https://github.com/oasis-tcs/virtio-spec/blob/master/split-ring.tex
[Virtio packed virtqueue specification]: https://github.com/oasis-tcs/virtio-spec/blob/master/packed-ring.tex
[FUSE kernel documentation]: https://docs.kernel.org/filesystems/fuse/fuse.html
[Linux virtio-fs driver]: https://github.com/torvalds/linux/blob/master/fs/fuse/virtio_fs.c
[QEMU vhost-user-fs source]: https://github.com/qemu/qemu/blob/master/hw/virtio/vhost-user-fs.c
[QEMU vhost-user protocol]: https://www.qemu.org/docs/master/interop/vhost-user.html
[virtio-fs project]: https://virtio-fs.gitlab.io/
