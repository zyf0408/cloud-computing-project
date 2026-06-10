"""
=============================================================================
A-3 性能对比分析 (5分)
=============================================================================
对比三种方式执行"按年份GROUP BY统计电影数量和平均评分"的性能：
  1. Pandas 单机版 —— 纯Python单机处理
  2. PySpark executorInstances=1 —— Spark单执行器分布式
  3. PySpark executorInstances=2 —— Spark双执行器分布式

包含 Amdahl 定律分析：
  Amdahl定律: Speedup = 1 / ((1-P) + P/N)
  其中 P 为可并行化比例，N 为处理器数量。
  
  在GROUP BY聚合场景中，数据分区、局部聚合(shuffle前)是高并行化的，
  但shuffle操作涉及网络IO是串行瓶颈，最终合并阶段也受限。
  因此从1个executor增加到2个，理论加速比上限为：
    若 P=0.7: Speedup = 1/(0.3 + 0.7/2) = 1/(0.65) ≈ 1.54x
    若 P=0.5: Speedup = 1/(0.5 + 0.5/2) = 1/(0.75) ≈ 1.33x
  实际加速比通常低于理论值，因为还有以下因素：
  - 任务调度开销
  - 数据序列化/反序列化开销
  - 网络传输延迟
  - GC和内存管理开销
=============================================================================
"""

import time
import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round


def create_spark_session(app_name, executor_instances=1):
    """
    创建SparkSession，可指定executor数量（用于对比测试）
    """
    builder = SparkSession.builder.appName(app_name)

    if os.environ.get("SPARK_LOCAL", "").lower() == "true":
        # 本地模式：通过 master 参数模拟 executor 数量
        # local[1] 模拟单executor, local[2] 模拟双executor
        builder = builder \
            .master(f"local[{executor_instances}]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", str(executor_instances * 2))
    else:
        # 集群模式：通过 spark.executor.instances 设置
        builder = builder \
            .config("spark.executor.instances", str(executor_instances)) \
            .config("spark.executor.cores", "1") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.shuffle.partitions", str(executor_instances * 2))

    return builder.getOrCreate()


def get_csv_path(filename="douban_movies.csv"):
    """获取CSV文件路径"""
    obs_path = os.environ.get("OBS_PATH", "")
    if obs_path:
        return obs_path.rstrip("/") + "/" + filename
    else:
        # 本地路径
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            filename
        )


def benchmark_pandas(csv_path):
    """
    方法1: Pandas 单机版
    使用Pandas读取CSV，执行groupby聚合
    记录完整执行时间（读取 + 处理）
    """
    print(f"\n{'='*70}")
    print("方法1: Pandas 单机版")
    print(f"{'='*70}")

    # 记录开始时间
    t_start = time.time()

    # 读取CSV（模拟完整流程）
    print(f"[Pandas] 正在读取CSV文件: {csv_path}")
    t_read_start = time.time()
    df = pd.read_csv(
        csv_path,
        encoding='utf-8',
        low_memory=False,
        # 指定dtype以加速读取
        dtype={
            'year': 'float64',
            'rating_score': 'float64',
            'rating_count': 'float64',
            'collect_count': 'float64'
        }
    )
    t_read_end = time.time()

    print(f"[Pandas] 数据加载完成: {df.shape[0]} 行 x {df.shape[1]} 列")
    print(f"[Pandas] 读取耗时: {t_read_end - t_read_start:.2f} 秒")

    # 数据清洗（与Spark版本保持一致）
    df = df.dropna(subset=['rating_score'])
    df = df[df['year'] > 1800]

    # 执行 GROUP BY 聚合
    t_group_start = time.time()
    result = df.groupby('year').agg(
        movie_count=('movie_id', 'count'),
        avg_rating=('rating_score', 'mean'),
        avg_rating_count=('rating_count', 'mean')
    ).reset_index()
    result = result.sort_values('year', ascending=False)
    t_group_end = time.time()

    t_end = time.time()

    # 输出结果摘要
    print(f"\n[Pandas] 结果（前10行）:")
    print(result.head(10).to_string(index=False))
    print(f"\n[Pandas] 聚合计算耗时: {t_group_end - t_group_start:.2f} 秒")
    print(f"[Pandas] 总耗时: {t_end - t_start:.2f} 秒")

    return {
        "method": "Pandas 单机",
        "read_time": t_read_end - t_read_start,
        "compute_time": t_group_end - t_group_start,
        "total_time": t_end - t_start
    }


def benchmark_spark(csv_path, executor_instances):
    """
    方法2/3: PySpark 版
    使用Spark DataFrame执行groupBy聚合
    executor_instances: 执行器数量（1或2）
    """
    method_name = f"PySpark (executorInstances={executor_instances})"
    print(f"\n{'='*70}")
    print(f"方法{'3' if executor_instances == 2 else '2'}: {method_name}")
    print(f"{'='*70}")

    t_start = time.time()
    spark = None

    try:
        # 创建 SparkSession
        spark = create_spark_session(
            f"PerfCompare-{executor_instances}exec",
            executor_instances
        )

        # 读取CSV
        print(f"[{method_name}] 正在读取CSV文件...")
        t_read_start = time.time()

        # 判断路径类型
        if csv_path.startswith("s3a://"):
            spark_csv_path = csv_path
        else:
            spark_csv_path = "file:///" + csv_path.replace("\\", "/")

        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .option("multiLine", "true") \
            .csv(spark_csv_path)

        # 处理BOM
        first_col = df.columns[0]
        if first_col.startswith("\ufeff"):
            df = df.withColumnRenamed(first_col, first_col.replace("\ufeff", ""))

        # 清洗
        df = df.filter(
            col("rating_score").isNotNull() &
            (col("year") > 1800)
        ).cache()  # 缓存数据以保证公平比较

        # 触发缓存
        row_count = df.count()
        t_read_end = time.time()
        print(f"[{method_name}] 数据加载完成: {row_count} 行")
        print(f"[{method_name}] 读取+缓存耗时: {t_read_end - t_read_start:.2f} 秒")

        # 执行 GROUP BY 聚合
        print(f"[{method_name}] 正在执行 GROUP BY 聚合...")
        t_group_start = time.time()

        result = df.groupBy("year") \
            .agg(
                count("*").alias("movie_count"),
                spark_round(avg("rating_score"), 2).alias("avg_rating"),
                spark_round(avg("rating_count"), 0).cast("int").alias("avg_rating_count")
            ) \
            .orderBy(col("year").desc())

        # 触发计算（action操作）
        result_rows = result.count()
        t_group_end = time.time()

        # 显示部分结果
        print(f"\n[{method_name}] 结果（前10行）:")
        result.show(10, truncate=False)

        t_end = time.time()

        print(f"\n[{method_name}] 聚合计算耗时: {t_group_end - t_group_start:.2f} 秒")
        print(f"[{method_name}] 总耗时: {t_end - t_start:.2f} 秒")

        return {
            "method": method_name,
            "executor_instances": executor_instances,
            "read_time": t_read_end - t_read_start,
            "compute_time": t_group_end - t_group_start,
            "total_time": t_end - t_start
        }

    finally:
        if spark is not None:
            spark.stop()


def print_comparison_table(results, csv_path):
    """
    输出性能对比结果表格，包含Amdahl定律分析
    """
    print(f"\n{'='*70}")
    print("性能对比结果汇总")
    print(f"{'='*70}")

    # 表格头
    print(f"\n{'方法':<35} {'读取耗时':>10} {'计算耗时':>10} {'总耗时':>10} {'加速比':>10}")
    print("-" * 80)

    baseline_time = None
    for r in results:
        method = r["method"]
        read_t = r["read_time"]
        comp_t = r["compute_time"]
        total_t = r["total_time"]

        if baseline_time is None:
            # Pandas 作为基准
            baseline_time = total_t
            speedup = "1.00x (基准)"
        else:
            speedup_val = baseline_time / total_t if total_t > 0 else 0
            speedup = f"{speedup_val:.2f}x"

        print(f"{method:<35} {read_t:>8.2f}s {comp_t:>8.2f}s {total_t:>8.2f}s {speedup:>10}")

    print("-" * 80)

    # Amdahl 定律分析
    print(f"\n{'='*70}")
    print("Amdahl 定律分析")
    print(f"{'='*70}")
    print("""
    Amdahl定律公式:  Speedup = 1 / ((1-P) + P/N)

    其中:
      P = 可并行化的计算比例
      N = 处理器/执行器数量
      (1-P) = 不可并行化的串行部分

    在本场景中:
    - 可并行化部分(P): 数据读取分片、Map端局部聚合
      （Spark可将CSV文件分片并行读取，各executor独立处理各自分区）
    - 串行部分(1-P): Shuffle阶段（数据网络传输）、最终Reduce聚合、
      结果收集到Driver、任务调度开销

    理论加速比估算:
      - 若 P=0.6, N=2:  Speedup = 1/(0.4+0.6/2) = 1/0.70 = 1.43x
      - 若 P=0.7, N=2:  Speedup = 1/(0.3+0.7/2) = 1/0.65 = 1.54x
      - 若 P=0.8, N=2:  Speedup = 1/(0.2+0.8/2) = 1/0.60 = 1.67x
      - 若 P=0.9, N=2:  Speedup = 1/(0.1+0.9/2) = 1/0.55 = 1.82x

    实际加速比通常低于理论值，原因包括:
      1. 数据倾斜 —— 某些分区数据量远大于其他分区
      2. 序列化开销 —— Python对象与JVM间的数据转换
      3. GC开销 —— 分布式计算中的垃圾回收
      4. 网络延迟 —— shuffle阶段的数据传输延迟
      5. 冷启动 —— Spark任务初始化和资源分配

    结论:
    - 从1个executor增加到2个executor，理论最大加速比约1.5x~1.8x
    - 由于GROUP BY聚合中shuffle占比较高，实际加速比可能更低
    - 随着数据量增大，并行化收益会更明显
    - 对于计算密集型操作（非shuffle），加速比会接近线性
    """)

    print(f"\n数据源: {csv_path}")
    print(f"查询: 按年份GROUP BY统计电影数量、平均评分、平均评价数")


def main():
    print("=" * 70)
    print("A-3 性能对比分析: Pandas vs PySpark(1exec) vs PySpark(2exec)")
    print("查询: 按年份GROUP BY统计电影数量和平均评分")
    print("=" * 70)

    # 获取CSV路径
    csv_path = get_csv_path("douban_movies.csv")
    print(f"\n数据文件路径: {csv_path}")

    # 检查文件是否存在（仅本地模式）
    if not csv_path.startswith("s3a://"):
        if not os.path.exists(csv_path):
            print(f"\n[错误] CSV文件不存在: {csv_path}")
            print("请确保 douban_movies.csv 在正确的位置")
            sys.exit(1)
        file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
        print(f"文件大小: {file_size_mb:.2f} MB")

    results = []

    try:
        # 方法1: Pandas 单机版
        r1 = benchmark_pandas(csv_path)
        results.append(r1)

        # 方法2: PySpark executorInstances=1
        r2 = benchmark_spark(csv_path, executor_instances=1)
        results.append(r2)

        # 方法3: PySpark executorInstances=2
        r3 = benchmark_spark(csv_path, executor_instances=2)
        results.append(r3)

        # 输出对比结果
        print_comparison_table(results, csv_path)

        # 保存结果到文件
        output_dir = os.path.dirname(os.path.abspath(__file__))
        result_file = os.path.join(output_dir, "performance_result.txt")
        with open(result_file, "w", encoding="utf-8") as f:
            f.write("性能对比结果\n")
            f.write("=" * 60 + "\n")
            f.write(f"{'方法':<35} {'总耗时(s)':>12}\n")
            f.write("-" * 50 + "\n")
            for r in results:
                f.write(f"{r['method']:<35} {r['total_time']:>10.2f}\n")
        print(f"\n[信息] 结果已保存到: {result_file}")

    except Exception as e:
        print(f"\n[错误] 性能对比测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
