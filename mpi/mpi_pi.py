# -*- coding: utf-8 -*-
"""
B-0: MPI Pi 计算 (蒙特卡洛方法)
================================
用途:
    使用蒙特卡洛 (Monte Carlo) 方法，通过 MPI 并行计算圆周率 π 的近似值。

实现原理:
    1. 在单位正方形 [0,1] x [0,1] 内随机撒点。
    2. 判断每个点是否落在单位圆内 (x^2 + y^2 <= 1)。
    3. 圆内点数 / 总点数 ≈ π/4，因此 π ≈ 4 * (圆内点数 / 总点数)。
    4. 每个进程独立采样 N 次，然后通过 comm.reduce(MPI.SUM) 将各进程的
       圆内计数汇总到主进程 (rank=0)，由主进程计算最终的 π 近似值。

运行方式:
    mpiexec -n 4 python mpi_pi.py
"""

from mpi4py import MPI
import random

def main():
    # 初始化 MPI 通信器
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()   # 当前进程编号
    size = comm.Get_size()   # 总进程数

    # 总采样次数（每个进程各采样 N 次）
    N = 10_000_000

    # ---------- 蒙特卡洛采样 ----------
    # 在单位正方形内随机生成点，统计落入单位圆内的数量
    local_count = sum(
        1 for _ in range(N)
        if random.random() ** 2 + random.random() ** 2 <= 1.0
    )

    # ---------- 汇总结果 ----------
    # 使用 reduce 将所有进程的 local_count 求和到主进程
    total = comm.reduce(local_count, op=MPI.SUM, root=0)

    # ---------- 主进程输出结果 ----------
    if rank == 0:
        pi = 4.0 * total / (N * size)
        error = abs(pi - 3.141592653589793)
        print(f"[{size} processes] π ≈ {pi:.6f} Error: {error:.6f}")


if __name__ == "__main__":
    main()
