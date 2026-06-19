# -*- coding: utf-8 -*-
"""
B-1: MPI 矩阵向量乘法
======================
用途:
    使用 MPI 的 Scatter / Gather 通信模式，并行计算矩阵-向量乘法 y = A * x。

实现原理:
    1. 主进程 (rank=0) 生成一个 N x N 的随机矩阵 A 和长度为 N 的随机向量 x。
    2. 通过 comm.Scatter 将矩阵 A 按行均匀分片，每个 Worker 进程获得 N/size 行。
    3. 向量 x 通过 comm.Bcast 广播到所有进程（因为每个 Worker 都需要完整的 x）。
    4. 各进程独立计算自己那部分矩阵与 x 的乘积，得到局部结果。
    5. 通过 comm.Gather 将所有局部结果汇总到主进程，得到完整的乘积向量 y。

运行方式:
    mpiexec -n 4 python mpi_matrix.py
"""

from mpi4py import MPI
import numpy as np
import time


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    N = 800  # 矩阵维度

    # ---------- 主进程生成数据 ----------
    if rank == 0:
        A = np.random.rand(N, N).astype(np.float64)
        x = np.random.rand(N).astype(np.float64)
    else:
        A = None
        x = np.empty(N, dtype=np.float64)

    # ---------- 计时开始 ----------
    t_start = time.perf_counter()

    # ---------- Scatter: 将矩阵按行分片到各进程 ----------
    # 每个进程接收 rows_per_proc 行
    rows_per_proc = N // size
    local_A = np.empty((rows_per_proc, N), dtype=np.float64)
    comm.Scatter(A, local_A, root=0)

    # ---------- Bcast: 广播向量 x 到所有进程 ----------
    comm.Bcast(x, root=0)

    # ---------- 各进程计算局部矩阵-向量乘积 ----------
    local_y = local_A @ x  # 矩阵乘法: (rows_per_proc x N) @ (N,) -> (rows_per_proc,)

    # ---------- Gather: 汇总结果到主进程 ----------
    y = np.empty(N, dtype=np.float64) if rank == 0 else None
    comm.Gather(local_y, y, root=0)

    # ---------- 计时结束 ----------
    t_end = time.perf_counter()
    elapsed = t_end - t_start

    # ---------- 主进程输出结果 ----------
    if rank == 0:
        print(f"[B-1] 矩阵向量乘法: N={N}, 进程数={size}")
        print(f"结果长度: {len(y)}")
        print(f"前 5 个元素: [{y[0]:.2f}, {y[1]:.2f}, {y[2]:.2f}, {y[3]:.2f}, {y[4]:.2f}]")
        print(f"耗时 (Scatter/Gather): {elapsed:.4f}s")


if __name__ == "__main__":
    main()
