# -*- coding: utf-8 -*-
"""
B-2: MPI Amdahl 定律分析
=========================
用途:
    在同一 MPI 程序中同时执行矩阵向量乘法和蒙特卡洛 Pi 计算，
    分别测量两种任务的执行耗时，用于验证 Amdahl 定律。

实现原理:
    Amdahl 定律指出，程序的可加速比受限于其串行部分的比例：
        S(p) = 1 / (s + (1-s)/p)
    其中 s 为串行比例，p 为处理器数。

    本程序通过以下步骤进行实验：
    1. 所有进程先同步 (comm.Barrier)。
    2. 分别计时执行矩阵向量乘法 (通信密集型) 和 Pi 计算 (计算密集型)。
    3. 主进程汇总并输出两种任务在不同进程数下的耗时，
       可用于分析加速比和并行效率。

运行方式:
    mpiexec -n 2  python mpi_amdahl.py
    mpiexec -n 4  python mpi_amdahl.py
    mpiexec -n 8  python mpi_amdahl.py
"""

from mpi4py import MPI
import numpy as np
import random
import time


def compute_pi(comm, N=10_000_000):
    """蒙特卡洛方法计算 Pi，返回 (pi值, 耗时)"""
    rank = comm.Get_rank()
    size = comm.Get_size()

    t0 = time.perf_counter()

    # 每个进程独立采样
    local_count = sum(
        1 for _ in range(N)
        if random.random() ** 2 + random.random() ** 2 <= 1.0
    )

    # 汇总到主进程
    total = comm.reduce(local_count, op=MPI.SUM, root=0)

    t1 = time.perf_counter()
    elapsed = t1 - t0

    if rank == 0:
        pi = 4.0 * total / (N * size)
        return pi, elapsed
    return None, elapsed


def compute_matrix_vector(comm, N=800):
    """MPI 矩阵向量乘法，返回耗时"""
    rank = comm.Get_rank()
    size = comm.Get_size()

    t0 = time.perf_counter()

    # 主进程生成数据
    if rank == 0:
        A = np.random.rand(N, N).astype(np.float64)
        x = np.random.rand(N).astype(np.float64)
    else:
        A = None
        x = np.empty(N, dtype=np.float64)

    # Scatter 矩阵行
    rows_per_proc = N // size
    local_A = np.empty((rows_per_proc, N), dtype=np.float64)
    comm.Scatter(A, local_A, root=0)

    # Bcast 向量
    comm.Bcast(x, root=0)

    # 局部计算
    local_y = local_A @ x

    # Gather 结果
    y = np.empty(N, dtype=np.float64) if rank == 0 else None
    comm.Gather(local_y, y, root=0)

    t1 = time.perf_counter()
    elapsed = t1 - t0

    return elapsed


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # 全局同步，确保所有进程同时开始
    comm.Barrier()

    # ---------- 任务 1: Pi 计算 ----------
    pi_val, t_pi = compute_pi(comm, N=10_000_000)

    # 全局同步
    comm.Barrier()

    # ---------- 任务 2: 矩阵向量乘法 ----------
    t_mat = compute_matrix_vector(comm, N=800)

    # ---------- 主进程输出 ----------
    if rank == 0:
        print(f"MPI Amdahl Test: p={size}")
        if pi_val is not None:
            error = abs(pi_val - 3.141592653589793)
            print(f"π ≈ {pi_val:.6f}, error={error:.6e}")
        print(f"矩阵向量乘法耗时: T_mat({size}) = {t_mat:.4f}s")
        print(f"π 计算耗时: T_pi({size}) = {t_pi:.4f}s")


if __name__ == "__main__":
    main()
