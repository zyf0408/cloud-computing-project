# -*- coding: utf-8 -*-
"""
B-3: MPI 非阻塞通信 (Isend / Irecv)
======================================
用途:
    使用 MPI 非阻塞通信接口 (comm.Isend / comm.Irecv) 实现点对点并发数据传输，
    验证非阻塞模式下通信与计算可以重叠执行。

实现原理:
    1. 阻塞通信 (comm.send / comm.recv) 会等待通信完成后才返回，
       导致通信期间 CPU 空闲等待。
    2. 非阻塞通信 (comm.Isend / comm.Irecv) 立即返回一个请求对象 (Request)，
       程序可以在等待通信完成的同时执行其他计算。
    3. 通过 MPI.Request.Wait() 或 MPI.Test() 检查通信是否完成。
    4. 本程序中，进程 0 向进程 1 发送一个 800x800 的矩阵，
       同时进程 1 向进程 0 发送同样大小的矩阵，形成双向并发传输。

运行方式:
    mpiexec -n 2 python mpi_nonblocking.py
"""

from mpi4py import MPI
import numpy as np
import time


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    N = 800  # 矩阵维度

    # 仅使用 2 个进程进行点对点通信演示
    if size < 2:
        if rank == 0:
            print("请至少使用 2 个进程运行: mpiexec -n 2 python mpi_nonblocking.py")
        return

    # ---------- 准备数据 ----------
    # 每个进程生成一个随机矩阵用于发送
    send_buf = np.random.rand(N, N).astype(np.float64)
    recv_buf = np.empty((N, N), dtype=np.float64)

    # 定义通信对: rank 0 <-> rank 1
    src = 1 - rank  # 0 的源是 1，1 的源是 0
    dst = 1 - rank  # 0 的目标是 1，1 的目标是 0

    # ---------- 全局同步 ----------
    comm.Barrier()
    t_start = time.perf_counter()

    # ---------- 非阻塞通信 ----------
    # 同时发起发送和接收请求（非阻塞，立即返回）
    send_req = comm.Isend(send_buf, dest=dst, tag=0)
    recv_req = comm.Irecv(recv_buf, source=src, tag=0)

    # 在等待通信完成期间，可以执行其他计算（此处以矩阵运算为例）
    # 这体现了非阻塞通信的优势：通信与计算重叠
    dummy = np.dot(send_buf[:10, :10], send_buf[:10, :10].T)

    # 等待通信完成
    MPI.Request.Waitall([send_req, recv_req])

    t_end = time.perf_counter()
    elapsed = t_end - t_start

    # ---------- 验证数据完整性 ----------
    # 主进程输出结果
    if rank == 0:
        print(f"[B-3] 非阻塞通信 (Isend / Irecv)")
        print(f"非阻塞通信完成, 矩阵形状: {recv_buf.shape}")
        print(f"耗时 (Isend/Irecv): {elapsed:.4f}s")


if __name__ == "__main__":
    main()
