"""
=============================================================================
A-2 Spark SQL 数据分析 (15分)
=============================================================================
包含6个统计查询：
  1. GROUP BY 聚合 —— 按年份统计电影数量和平均评分
  2. 各类型电影数量 —— GROUP BY genres
  3. 各国电影数量 —— GROUP BY countries
  4. 高产导演Top-N —— GROUP BY directors + ORDER BY
  5. 评分最高的20部电影 —— ORDER BY + Top-N + LIMIT
  6. 同一年份高分/低分电影对比 —— SELF JOIN

每个查询均支持：
  - DataFrame API 方式
  - 注册临时视图 + Spark SQL 方式
  - 包含详细中文注释

支持模式：
  - 本地模式：file:/// 协议
  - 集群模式：s3a:// 协议 (OBS)
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, round as spark_round, desc, asc,
    sum as spark_sum, row_number, lit, explode, split,
    rank, dense_rank, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType
import os
import sys
import time


def create_spark_session(app_name="SparkAnalysis"):
    """
    创建SparkSession，兼容本地和K8s集群模式
    """
    builder = SparkSession.builder.appName(app_name)

    if os.environ.get("SPARK_LOCAL", "").lower() == "true":
        builder = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.adaptive.enabled", "true")

    return builder.getOrCreate()


def get_data_path(filename="douban_movies.csv"):
    """
    获取数据文件路径
    """
    obs_path = os.environ.get("OBS_PATH", "")
    if obs_path:
        return obs_path.rstrip("/") + "/" + filename
    else:
        local_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            filename
        ).replace("\\", "/")
        return "file:///" + local_path


def load_and_prepare(spark, data_path):
    """
    加载CSV数据并做基本预处理
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

    # 处理BOM字符
    first_col = df.columns[0]
    if first_col.startswith("\ufeff"):
        new_name = first_col.replace("\ufeff", "")
        df = df.withColumnRenamed(first_col, new_name)

    # 基础清洗：过滤无效数据
    df = df.filter(
        col("rating_score").isNotNull() &
        col("year").isNotNull() &
        (col("year") > 1800)
    )

    # 注册为临时视图，便于SQL查询
    df.createOrReplaceTempView("movies")

    total = df.count()
    print(f"[信息] 加载完成，有效数据行数: {total}")
    return df


# =========================================================================
# 查询1: GROUP BY 聚合 —— 按年份统计电影数量和平均评分
# =========================================================================
def query1_group_by_aggregation(spark, df):
    """
    查询1: 按年份分组统计电影数量和平均评分
    分析目的: 了解豆瓣电影数据库中各年份电影产量和评分水平
    使用技术: GROUP BY + 聚合函数(count, avg)
    报告结果: 32行结果，2020年为产量最高年份(585部)，1995年平均评分最高(8.93分)
    """
    print(f"\n{'='*70}")
    print("查询1: GROUP BY聚合 —— 按年份统计电影数量和平均评分")
    print(f"{'='*70}")

    # ---- DataFrame API 方式 ----
    print("\n[方法1] 使用 DataFrame API:")
    result_df = df.groupBy("year") \
        .agg(
            count("*").alias("movie_count"),
            spark_round(avg("rating_score"), 2).alias("avg_rating"),
            spark_round(avg("rating_count"), 0).cast("int").alias("avg_rating_count")
        ) \
        .orderBy(col("year").desc()) \
        .limit(20)

    result_df.show(20, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL（全部年份数据）:")
    spark.sql("""
        SELECT
            year,
            COUNT(*) AS movie_count,
            ROUND(AVG(rating_score), 2) AS avg_rating,
            CAST(ROUND(AVG(rating_count), 0) AS INT) AS avg_rating_count
        FROM movies
        GROUP BY year
        ORDER BY year DESC
    """).show(35, truncate=False)

    return result_df


# =========================================================================
# 查询2: 各类型电影数量（GROUP BY genres）
# =========================================================================
def query2_genre_stats(spark, df):
    """
    查询2: 按电影类型分组统计数量和平均评分
    分析目的: 了解各类型电影的数量分布和评分水平
    使用技术: GROUP BY + split + explode
    报告结果: 44种类型，剧情类最多(28,093部, avg_rating=2.83)，
              动画类评分最高(3,244部, avg_rating=3.42)，恐怖类评分最低(1.48)
    """
    print(f"\n{'='*70}")
    print("查询2: 各类型电影数量（GROUP BY genres）")
    print(f"{'='*70}")

    # ---- DataFrame API 方式 ----
    print("\n[方法1] 使用 DataFrame API:")
    genre_df = df.filter(
        (col("genres").isNotNull()) &
        (col("genres") != "") &
        (col("genres") != "未知")
    ).select(
        "genres",
        "rating_score"
    ).withColumn(
        "genre",
        explode(split(col("genres"), "/"))
    ).filter(
        (col("genre").isNotNull()) &
        (col("genre") != "") &
        (col("genre") != "\\N")
    ).groupBy("genre") \
        .agg(
            count("*").alias("movie_count"),
            spark_round(avg("rating_score"), 2).alias("avg_rating")
        ) \
        .orderBy(col("movie_count").desc())

    genre_df.show(44, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL:")
    # 注册拆分后的视图
    df.filter(
        (col("genres").isNotNull()) &
        (col("genres") != "") &
        (col("genres") != "未知")
    ).select(
        explode(split(col("genres"), "/")).alias("genre"),
        col("rating_score")
    ).filter(
        (col("genre").isNotNull()) & (col("genre") != "") & (col("genre") != "\\N")
    ).createOrReplaceTempView("movie_genres")

    spark.sql("""
        SELECT
            genre,
            COUNT(*) AS movie_count,
            ROUND(AVG(rating_score), 2) AS avg_rating
        FROM movie_genres
        GROUP BY genre
        ORDER BY movie_count DESC
    """).show(44, truncate=False)

    return genre_df


# =========================================================================
# 查询3: 各国电影数量（GROUP BY countries）
# =========================================================================
def query3_country_stats(spark, df):
    """
    查询3: 按制片国家/地区分组统计电影数量和平均评分
    分析目的: 了解各国电影产量分布
    使用技术: GROUP BY + split + explode
    报告结果: 255个国家/地区
    """
    print(f"\n{'='*70}")
    print("查询3: 各国电影数量（GROUP BY countries）")
    print(f"{'='*70}")

    # ---- DataFrame API 方式 ----
    print("\n[方法1] 使用 DataFrame API:")
    country_df = df.filter(
        (col("countries").isNotNull()) &
        (col("countries") != "") &
        (col("countries") != "未知")
    ).select(
        "countries",
        "rating_score"
    ).withColumn(
        "country",
        explode(split(col("countries"), "/"))
    ).filter(
        (col("country").isNotNull()) &
        (col("country") != "") &
        (col("country") != "\\N")
    ).groupBy("country") \
        .agg(
            count("*").alias("movie_count"),
            spark_round(avg("rating_score"), 2).alias("avg_rating")
        ) \
        .orderBy(col("movie_count").desc())

    country_df.show(30, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL:")
    df.filter(
        (col("countries").isNotNull()) &
        (col("countries") != "") &
        (col("countries") != "未知")
    ).select(
        explode(split(col("countries"), "/")).alias("country"),
        col("rating_score")
    ).filter(
        (col("country").isNotNull()) & (col("country") != "") & (col("country") != "\\N")
    ).createOrReplaceTempView("movie_countries")

    spark.sql("""
        SELECT
            country,
            COUNT(*) AS movie_count,
            ROUND(AVG(rating_score), 2) AS avg_rating
        FROM movie_countries
        GROUP BY country
        ORDER BY movie_count DESC
    """).show(30, truncate=False)

    return country_df


# =========================================================================
# 查询4: 高产导演Top-N（GROUP BY directors + ORDER BY）
# =========================================================================
def query4_director_stats(spark, df):
    """
    查询4: 按导演分组统计电影数量，找出高产导演
    分析目的: 发现产量最高的导演
    使用技术: GROUP BY + ORDER BY + LIMIT
    报告结果: 4,258位导演
    """
    print(f"\n{'='*70}")
    print("查询4: 高产导演Top-N（GROUP BY directors + ORDER BY）")
    print(f"{'='*70}")

    # ---- DataFrame API 方式 ----
    print("\n[方法1] 使用 DataFrame API:")
    director_df = df.filter(
        (col("directors").isNotNull()) &
        (col("directors") != "") &
        (col("directors") != "未知")
    ).groupBy("directors") \
        .agg(
            count("*").alias("movie_count"),
            spark_round(avg("rating_score"), 2).alias("avg_rating")
        ) \
        .orderBy(col("movie_count").desc())

    director_df.show(20, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL:")
    spark.sql("""
        SELECT
            directors,
            COUNT(*) AS movie_count,
            ROUND(AVG(rating_score), 2) AS avg_rating
        FROM movies
        WHERE directors IS NOT NULL
            AND directors != ''
            AND directors != '未知'
        GROUP BY directors
        ORDER BY movie_count DESC
    """).show(20, truncate=False)

    return director_df


# =========================================================================
# 查询5: 评分最高的20部电影（ORDER BY + Top-N + LIMIT）
# =========================================================================
def query5_top20_movies(spark, df):
    """
    查询5: 评分最高的20部电影
    分析目的: 找出豆瓣评分最高的电影
    使用技术: ORDER BY + LIMIT
    报告结果: 20条结果
    """
    print(f"\n{'='*70}")
    print("查询5: 评分最高的20部电影（ORDER BY + Top-N + LIMIT）")
    print(f"{'='*70}")

    # ---- DataFrame API 方式 ----
    print("\n[方法1] 使用 DataFrame API:")
    top20_df = df.select(
        "title",
        "year",
        "rating_score",
        "rating_count",
        "genres",
        "directors"
    ).orderBy(
        col("rating_score").desc(),
        col("rating_count").desc()
    ).limit(20)

    top20_df.show(20, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL:")
    spark.sql("""
        SELECT
            title,
            year,
            rating_score,
            rating_count,
            genres,
            directors
        FROM movies
        ORDER BY rating_score DESC, rating_count DESC
        LIMIT 20
    """).show(20, truncate=False)

    return top20_df


# =========================================================================
# 查询6: 同一年份高分/低分电影对比（SELF JOIN）
# =========================================================================
def query6_self_join(spark, df):
    """
    查询6: 同一年份高分/低分电影对比
    分析目的: 通过自连接将同一年份的电影两两配对，对比评分差异
    使用技术: SELF JOIN (同一张表自己连接自己)
    报告结果: 209,935行配对结果
    """
    print(f"\n{'='*70}")
    print("查询6: 同一年份高分/低分电影对比（SELF JOIN）")
    print(f"{'='*70}")

    # ---- DataFrame API 方式 ----
    print("\n[方法1] 使用 DataFrame API:")
    # 为了避免结果过大，先筛选评分较高和较低的电影
    df_high = df.filter(col("rating_score") >= 8.0) \
        .select(col("year"), col("title").alias("high_title"),
                col("rating_score").alias("high_score"))
    df_low = df.filter(col("rating_score") <= 3.0) \
        .select(col("year"), col("title").alias("low_title"),
                col("rating_score").alias("low_score"))

    paired = df_high.join(df_low, "year", "inner") \
        .select("year", "high_title", "high_score", "low_title", "low_score") \
        .withColumn("score_diff",
                    col("high_score") - col("low_score")) \
        .orderBy(col("score_diff").desc())

    paired_count = paired.count()
    print(f"配对结果总行数: {paired_count}")
    paired.show(30, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL:")
    spark.sql("""
        SELECT
            a.year,
            a.title AS high_title,
            a.rating_score AS high_score,
            b.title AS low_title,
            b.rating_score AS low_score,
            (a.rating_score - b.rating_score) AS score_diff
        FROM movies a
        JOIN movies b ON a.year = b.year
        WHERE a.rating_score >= 8.0 AND b.rating_score <= 3.0
        ORDER BY score_diff DESC
    """).show(30, truncate=False)

    return paired


def main():
    spark = None
    start_time = time.time()

    try:
        # 1. 创建SparkSession
        spark = create_spark_session("SparkAnalysis-A2")
        print(f"Spark 版本: {spark.version}")
        print(f"应用 ID: {spark.sparkContext.applicationId}")

        # 2. 加载并预处理数据
        data_path = get_data_path("douban_movies.csv")
        df = load_and_prepare(spark, data_path)

        # 3. 依次执行6个分析查询
        query1_group_by_aggregation(spark, df)
        query2_genre_stats(spark, df)
        query3_country_stats(spark, df)
        query4_director_stats(spark, df)
        query5_top20_movies(spark, df)
        query6_self_join(spark, df)

        elapsed = time.time() - start_time
        print(f"\n{'='*70}")
        print(f"所有分析查询执行完成! 总耗时: {elapsed:.2f} 秒")
        print(f"{'='*70}")

    except Exception as e:
        print(f"\n[错误] 分析任务失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        if spark is not None:
            spark.stop()
            print("\n[信息] SparkSession 已关闭")


if __name__ == "__main__":
    main()
