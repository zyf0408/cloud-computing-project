"""
=============================================================================
PySpark WordCount 示例
用途：Spark Operator 环境验证，测试集群能否正常执行PySpark任务
支持模式：
  - 本地运行：从本地文件读取文本
  - OBS/S3运行：从s3a协议路径读取文本
=============================================================================
"""

from pyspark.sql import SparkSession
import sys
import os


def create_spark_session(app_name="WordCount"):
    """
    创建SparkSession，兼容本地和集群模式
    """
    builder = SparkSession.builder.appName(app_name)

    # 本地模式配置（用于开发测试）
    # 如果设置 SPARK_LOCAL=true 环境变量，则使用本地模式
    if os.environ.get("SPARK_LOCAL", "").lower() == "true":
        builder = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "2")

    return builder.getOrCreate()


def get_input_path():
    """
    获取输入文件路径，支持两种模式：
    1. OBS/S3模式：通过环境变量 OBS_PATH 传入 s3a://bucket/path
    2. 本地模式：使用默认的本地文件路径
    """
    obs_path = os.environ.get("OBS_PATH", "")
    if obs_path:
        # OBS/S3 远程路径
        return obs_path.rstrip("/") + "/input/sample.txt"
    else:
        # 本地文件路径
        return "file:///" + os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "sample.txt"
        ).replace("\\", "/")


def main():
    spark = None
    try:
        # 1. 创建SparkSession
        print("=" * 60)
        print("正在创建 SparkSession...")
        spark = create_spark_session("PySpark-WordCount")
        print(f"Spark 版本: {spark.version}")
        print(f"应用 ID: {spark.sparkContext.applicationId}")
        print(f"执行器数量: {spark.sparkContext.defaultParallelism}")
        print("=" * 60)

        # 2. 读取文本文件
        input_path = get_input_path()
        print(f"\n[信息] 正在从以下路径读取文本: {input_path}")

        # 尝试读取文件，如果文件不存在则使用内置示例数据
        try:
            text_rdd = spark.sparkContext.textFile(input_path)
            # 检查是否有数据
            first_line = text_rdd.first()
            print(f"[信息] 第一行数据: {first_line[:100] if len(first_line) > 100 else first_line}")
        except Exception as e:
            print(f"[警告] 无法从 {input_path} 读取文件: {e}")
            print("[信息] 使用内置示例文本数据进行WordCount演示...")
            # 使用内置示例文本
            sample_text = [
                "Hello Spark Hello World",
                "Spark is great for big data processing",
                "Big data processing with Spark is fast",
                "Hello from PySpark wordcount example",
                "Spark Spark Spark is the engine for big data",
                "Word count is a classic example",
                "PySpark is the Python API for Apache Spark",
                "Distributed computing makes big data processing possible",
                "Spark provides high-level APIs in Java Scala Python and R",
                "Hello big data world"
            ]
            text_rdd = spark.sparkContext.parallelize(sample_text)

        # 3. 执行WordCount
        print("\n[信息] 正在执行 WordCount...")
        word_counts = (
            text_rdd
            # 将每行文本按空格分割成单词
            .flatMap(lambda line: line.split())
            # 将每个单词映射为 (word, 1) 键值对
            .map(lambda word: (word, 1))
            # 按单词聚合计数
            .reduceByKey(lambda a, b: a + b)
        )

        # 4. 获取Top 10
        # 按计数降序排列，取前10个
        top10 = (
            word_counts
            # 交换键值对，按计数值排序
            .map(lambda x: (x[1], x[0]))
            .sortByKey(ascending=False)
            # 恢复为 (word, count) 格式
            .map(lambda x: (x[1], x[0]))
            .take(10)
        )

        # 5. 输出结果
        print("\n" + "=" * 60)
        print("WordCount - Top 10 结果")
        print("=" * 60)
        print(f"{'排名':<6} {'单词':<20} {'出现次数':<10}")
        print("-" * 40)
        for rank, (word, count) in enumerate(top10, 1):
            print(f"{rank:<6} {word:<20} {count:<10}")

        # 6. 统计信息
        total_words = word_counts.count()
        print("-" * 40)
        print(f"总计不重复单词数: {total_words}")

        # 7. 将结果保存到文件
        output_path = os.environ.get("OUTPUT_PATH", "")
        if output_path:
            output_full = output_path.rstrip("/") + "/wordcount_output"
            print(f"\n[信息] 正在将结果保存到: {output_full}")
            spark.sparkContext.parallelize(
                [f"{word}\t{count}" for word, count in top10]
            ).saveAsTextFile(output_full)
            print("[信息] 结果已保存成功")

        print("\n[成功] WordCount 执行完成!")
        return top10

    except Exception as e:
        print(f"\n[错误] WordCount 执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # 8. 关闭SparkSession
        if spark is not None:
            spark.stop()
            print("\n[信息] SparkSession 已关闭")


if __name__ == "__main__":
    main()
