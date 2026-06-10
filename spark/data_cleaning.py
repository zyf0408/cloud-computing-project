"""
=============================================================================
A-1 数据清洗 (10分)
=============================================================================
功能说明：
  1. 加载 douban_movies.csv 到 PySpark DataFrame
  2. 打印 Schema 和前5行数据
  3. 统计各字段缺失值比例
  4. 对至少2个有缺失值字段采用不同的处理策略
  5. 输出清洗前后行数对比
  6. 输出各数值字段基本统计信息

处理策略选择原因（注释中标明）：
  - year:        用中位数填充 —— 年份数据为正态偏分布，中位数对异常年份鲁棒
  - genres:      填充"未知"   —— 类型字段为类别数据，无法数值估算
  - rating_score: 删除缺失行   —— 评分是核心指标，缺失时整条记录价值低
  - directors:   填充"未知"   —— 导演为文本字段，缺失时无法推断
  - summary:     填充空字符串 —— 摘要缺失不影响其他分析，空字符串即可

支持模式：
  - 本地模式：file:/// 协议读取本地CSV
  - 集群模式：s3a:// 协议读取OBS数据
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, lit,
    mean, stddev, min, max, percentile_approx
)
from pyspark.sql.types import IntegerType, FloatType, DoubleType
import os
import sys
import time


def create_spark_session(app_name="DataCleaning"):
    """
    创建 SparkSession，兼容本地和K8s集群模式
    """
    builder = SparkSession.builder.appName(app_name)

    # 本地模式配置
    if os.environ.get("SPARK_LOCAL", "").lower() == "true":
        builder = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "4")

    return builder.getOrCreate()


def get_data_path(filename="douban_movies.csv"):
    """
    获取数据文件路径，支持两种模式：
    1. OBS/S3模式：通过环境变量 OBS_PATH 传入 s3a://bucket/path/
    2. 本地模式：使用本地文件路径
    """
    obs_path = os.environ.get("OBS_PATH", "")
    if obs_path:
        # OBS/S3 远程路径（Spark on K8s 模式）
        return obs_path.rstrip("/") + "/" + filename
    else:
        # 本地文件路径（开发测试模式）
        # BOM处理：CSV文件可能包含BOM头，需要在读取时处理
        local_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            filename
        ).replace("\\", "/")
        return "file:///" + local_path


def load_data(spark, data_path):
    """
    加载CSV数据到DataFrame
    注意：CSV第一列可能包含BOM字符(\ufeff)，做相应处理
    """
    print(f"\n[信息] 正在从以下路径加载数据: {data_path}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .csv(data_path)

    # 处理BOM字符 —— CSV文件可能包含UTF-8 BOM在首列名中
    first_col = df.columns[0]
    if first_col.startswith("\ufeff"):
        new_name = first_col.replace("\ufeff", "")
        df = df.withColumnRenamed(first_col, new_name)
        print(f"[信息] 已处理BOM字符，列名 '{first_col}' -> '{new_name}'")

    return df


def analyze_missing_values(df):
    """
    统计各字段的缺失值数量和比例
    同时处理 null 和特殊值（如 year=0 视为缺失）
    """
    total_rows = df.count()
    print(f"\n{'='*70}")
    print(f"缺失值统计分析（总行数: {total_rows}）")
    print(f"{'='*70}")
    print(f"{'字段名':<20} {'缺失数':<10} {'缺失比例':<12} {'说明'}")
    print("-" * 70)

    missing_stats = {}
    for column in df.columns:
        # 统计null值的数量
        null_count = df.filter(
            col(column).isNull() |
            isnan(col(column)) |
            (col(column) == "")
        ).count()

        # 对 year 字段额外检查：year=0 也视为缺失
        if column == "year":
            zero_count = df.filter(col("year") == 0).count()
            null_count += zero_count

        ratio = null_count / total_rows * 100 if total_rows > 0 else 0
        missing_stats[column] = {
            "count": null_count,
            "ratio": round(ratio, 2)
        }

        # 说明列
        note = ""
        if column in ["rating_score", "rating_count"]:
            note = "核心评分字段"
        elif column in ["year"]:
            note = "含year=0的情况"
        elif column in ["summary"]:
            note = "摘要信息，非核心字段"
        elif column in ["genres", "directors", "countries"]:
            note = "分类/文本字段"

        print(f"{column:<20} {null_count:<10} {ratio:>6.2f}%      {note}")

    print("-" * 70)
    return missing_stats


def clean_data(df):
    """
    执行数据清洗，对至少2个字段采用不同处理策略

    策略1 —— year: 用中位数填充
      原因：年份数据可能存在录入错误（如年份为0或未来年份），
      中位数对极端值鲁棒，比均值更可靠。
      中位数比均值更不受极端异常年份影响。

    策略2 —— genres: 设置为"未知"
      原因：类型是分类文本字段，缺失时无法进行数值估算，
      填充"未知"可在后续分析中单独统计。

    策略3 —— rating_score: 删除缺失行（dropna）
      原因：评分是电影数据中最核心的指标之一。
      评分缺失意味着该条记录在分析中价值极低，
      删除这些行可以保证后续评分分析的准确性。
      同时也能过滤掉评分计数也缺失的数据。

    策略4 —— directors: 设置为"未知"
      原因：导演字段也是分类文本，同genres，缺失无法推理。

    策略5 —— summary: 设置为空字符串
      原因：摘要不是核心分析字段，填充空字符串即可，
      避免占用额外存储空间。
    """
    original_count = df.count()
    print(f"\n{'='*70}")
    print(f"开始数据清洗（原始行数: {original_count}）")
    print(f"{'='*70}")

    # 策略3前置：删除rating_score为null的行（在计算year中位数之前执行）
    print("\n[策略3] rating_score: 删除缺失行（dropna）")
    print("  原因: 评分为核心指标，缺失评分的数据分析价值极低")
    df_cleaned = df.filter(
        col("rating_score").isNotNull() &
        ~isnan(col("rating_score"))
    )
    after_dropna_count = df_cleaned.count()
    print(f"  删除行数: {original_count - after_dropna_count}")

    # 策略1：year 用中位数填充
    print("\n[策略1] year: 用中位数填充（fillna）")
    print("  原因: 中位数对异常年份（如0、未来年份）更鲁棒")
    # 先计算有效值的中位数（排除null和0值）
    year_median = df_cleaned.filter(
        col("year").isNotNull() & (col("year") > 1800)
    ).stat.approxQuantile("year", [0.5], 0.001)
    median_year = year_median[0] if year_median else 2000
    print(f"  年份中位数: {median_year}")

    # 将 year=0 或 year<1800 的值设为null，然后填充中位数
    df_cleaned = df_cleaned.withColumn(
        "year",
        when((col("year").isNull()) | (col("year") < 1800), lit(None))
        .otherwise(col("year"))
    )
    df_cleaned = df_cleaned.fillna({"year": median_year})

    # 策略2：genres 填充"未知"
    print("\n[策略2] genres: 设置为'未知'（fillna）")
    print("  原因: 类型字段为分类数据，缺失时无法数值估算")
    df_cleaned = df_cleaned.fillna({"genres": "未知"})
    # 同时处理空字符串
    df_cleaned = df_cleaned.withColumn(
        "genres",
        when((col("genres") == "") | (col("genres").isNull()), lit("未知"))
        .otherwise(col("genres"))
    )

    # 策略4：directors 填充"未知"
    print("\n[策略4] directors: 设置为'未知'（fillna）")
    print("  原因: 导演为文本字段，缺失时无法推断")
    df_cleaned = df_cleaned.fillna({"directors": "未知"})
    df_cleaned = df_cleaned.withColumn(
        "directors",
        when((col("directors") == "") | (col("directors").isNull()), lit("未知"))
        .otherwise(col("directors"))
    )

    # 策略5：summary 填充空字符串
    print("\n[策略5] summary: 设置为空字符串（fillna）")
    print("  原因: 摘要非核心分析字段，空字符串足够，不占额外空间")
    df_cleaned = df_cleaned.fillna({"summary": ""})
    df_cleaned = df_cleaned.withColumn(
        "summary",
        when(col("summary").isNull(), lit(""))
        .otherwise(col("summary"))
    )

    # 同时处理 countries 字段（如果存在缺失值）
    df_cleaned = df_cleaned.fillna({"countries": "未知"})
    df_cleaned = df_cleaned.withColumn(
        "countries",
        when((col("countries") == "") | (col("countries").isNull()), lit("未知"))
        .otherwise(col("countries"))
    )

    cleaned_count = df_cleaned.count()
    print(f"\n{'='*70}")
    print(f"清洗前后行数对比:")
    print(f"  清洗前: {original_count} 行")
    print(f"  清洗后: {cleaned_count} 行")
    print(f"  减少:   {original_count - cleaned_count} 行 ({(original_count - cleaned_count)/original_count*100:.2f}%)")
    print(f"{'='*70}")

    return df_cleaned


def compute_statistics(df):
    """
    输出各数值字段的基本统计信息
    包括：count, mean, stddev, min, max, 25%, 50%, 75%
    """
    print(f"\n{'='*70}")
    print("数值字段基本统计信息")
    print(f"{'='*70}")

    numeric_cols = ["year", "rating_score", "rating_count", "collect_count"]

    for ncol in numeric_cols:
        if ncol in df.columns:
            print(f"\n--- {ncol} ---")
            # 计算基本统计量
            stats = df.select(
                count(col(ncol)).alias("count"),
                mean(col(ncol)).alias("mean"),
                stddev(col(ncol)).alias("stddev"),
                min(col(ncol)).alias("min"),
                max(col(ncol)).alias("max")
            ).collect()[0]

            print(f"  计数(count):  {stats['count']}")
            print(f"  均值(mean):   {stats['mean']:.4f}" if stats['mean'] else "  均值(mean):   N/A")
            print(f"  标准差(stddev): {stats['stddev']:.4f}" if stats['stddev'] else "  标准差(stddev): N/A")
            print(f"  最小值(min):  {stats['min']}")
            print(f"  最大值(max):  {stats['max']}")

            # 计算四分位数
            try:
                quantiles = df.stat.approxQuantile(
                    ncol, [0.25, 0.5, 0.75], 0.01
                )
                print(f"  25%分位:      {quantiles[0]}")
                print(f"  50%分位(中位数): {quantiles[1]}")
                print(f"  75%分位:      {quantiles[2]}")
            except Exception:
                pass

    return df


def main():
    spark = None
    start_time = time.time()

    try:
        # 1. 创建SparkSession
        spark = create_spark_session("DataCleaning-A1")
        print(f"Spark 版本: {spark.version}")
        print(f"应用 ID: {spark.sparkContext.applicationId}")

        # 2. 加载数据
        data_path = get_data_path("douban_movies.csv")
        df = load_data(spark, data_path)

        # 3. 打印 Schema
        print(f"\n{'='*70}")
        print("DataFrame Schema")
        print(f"{'='*70}")
        df.printSchema()

        # 4. 打印前5行
        print(f"\n{'='*70}")
        print("前5行数据预览")
        print(f"{'='*70}")
        df.show(5, truncate=80)

        # 5. 统计缺失值
        missing_stats = analyze_missing_values(df)

        # 6. 执行数据清洗
        df_cleaned = clean_data(df)

        # 7. 输出数值字段统计信息
        compute_statistics(df_cleaned)

        # 8. 可选：保存清洗后的数据
        output_path = os.environ.get("OUTPUT_PATH", "")
        if output_path:
            clean_output = output_path.rstrip("/") + "/douban_cleaned"
            print(f"\n[信息] 正在保存清洗后数据到: {clean_output}")
            df_cleaned.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(clean_output)
            print("[信息] 清洗后数据已保存")

        elapsed = time.time() - start_time
        print(f"\n{'='*70}")
        print(f"数据清洗任务完成! 总耗时: {elapsed:.2f} 秒")
        print(f"{'='*70}")

    except Exception as e:
        print(f"\n[错误] 数据清洗任务失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        if spark is not None:
            spark.stop()
            print("\n[信息] SparkSession 已关闭")


if __name__ == "__main__":
    main()
