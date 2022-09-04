# 跨进程共享内存表

## 场景

把 DolphinDB 计算的结果，转发到其它进程

已有方式是通过 tcp 发到其它进程，转发 （待测） 条数据，每条（待测）大小，  延迟太大（十几毫秒） ， 

tcp  localhost socket 的延迟，按照

TCP loopback connection vs Unix Domain Socket performance 

GitHub - rigtorp/ipc-bench: Latency benchmarks of Unix IPC mechanisms 

在 6 微秒左右，

改成 其它 ipc （unix domain socket, pipe sem）的话理论上能降低 66%  参考

Lowest latency notification method between process under Linux 

 http://stackoverflow.com/questions/7979164/lowest-latency-notification-method-between-process-under-linux 

实现原理 ： mmap 出 ddb server 和 api 进程共享的 shm ,  server 端 数据就绪时，调用 libc 的 sem_post,  ( 在Linux 上是 syscall futex FUTEX_WAKE ) 唤醒  sem_wait 的 api 侧调用进程

功能：

从 dolphindb 流表侧通知 其它进程（api侧）数据就绪

目前 api 侧还有 bug

server 侧
## 架构

shm + mmap + semaphore

<数据交互流程图>


## 使用示例

dolphindb + api-cplusplus demo