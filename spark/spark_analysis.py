"""
=============================================================================
A-2 Spark SQL 数据分析 (15分)
=============================================================================
包含4个统计查询：
  1. GROUP BY 聚合 —— 按年份统计电影数量和平均评分
  2. ORDER BY Top-N —— 评分最高的10部电影（至少有1000条评价）
  3. 时间维度趋势 —— 按年份统计各类型电影产量变化
  4. 窗口函数 —— 计算每个国家电影的评分排名

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
    """
    print(f"\n{'='*70}")
    print("查询1: GROUP BY聚合 —— 按年份统计电影数量和平均评分")
    print(f"{'='*70}")

    # ---- DataFrame API 方式 ----
    print("\n[方法1] 使用 DataFrame API:")
    result_df = df.groupBy("year") \
        .agg(
            count("*").alias("movie_count"),                    # 电影数量
            spark_round(avg("rating_score"), 2).alias("avg_rating"),  # 平均评分
            spark_round(avg("rating_count"), 0).cast("int").alias("avg_rating_count")  # 平均评价数
        ) \
        .orderBy(col("year").desc()) \
        .limit(20)

    result_df.show(20, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL（最近20年数据）:")
    spark.sql("""
        SELECT
            year,
            COUNT(*) AS movie_count,
            ROUND(AVG(rating_score), 2) AS avg_rating,
            CAST(ROUND(AVG(rating_count), 0) AS INT) AS avg_rating_count
        FROM movies
        WHERE year >= 2000
        GROUP BY year
        ORDER BY year DESC
    """).show(25, truncate=False)

    return result_df


# =========================================================================
# 查询2: ORDER BY Top-N —— 评分最高的10部电影（至少有1000条评价）
# =========================================================================
def query2_top_n_rating(spark, df):
    """
    查询2: 评分最高的10部电影（至少1000条评价）
    分析目的: 找出高口碑且有一定热门度的电影
    过滤条件: rating_count >= 1000 —— 确保有足够的评价样本
    排序: 按 rating_score 降序
    """
    print(f"\n{'='*70}")
    print("查询2: ORDER BY Top-N —— 评分最高的10部电影（>=1000条评价）")
    print(f"{'='*70}")

    # ---- DataFrame API 方式 ----
    print("\n[方法1] 使用 DataFrame API:")
    top10_df = df.filter(col("rating_count") >= 1000) \
        .select(
            "title",
            "year",
            "rating_score",
            "rating_count",
            "genres",
            "countries"
        ) \
        .orderBy(
            col("rating_score").desc(),      # 先按评分降序
            col("rating_count").desc()        # 评分相同时按评价数降序
        ) \
        .limit(10)

    top10_df.show(10, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL:")
    spark.sql("""
        SELECT
            title,
            year,
            rating_score,
            rating_count,
            genres,
            countries
        FROM movies
        WHERE rating_count >= 1000
        ORDER BY rating_score DESC, rating_count DESC
        LIMIT 10
    """).show(10, truncate=False)

    return top10_df


# =========================================================================
# 查询3: 时间维度趋势分析 —— 按年份统计各类型电影产量变化
# =========================================================================
def query3_genre_trend(spark, df):
    """
    查询3: 按年份统计各类型电影产量变化
    分析目的: 观察不同电影类型随时间的变化趋势
    技术要点:
      - genres字段为 "剧情/爱情/喜剧" 格式的斜杠分隔字符串
      - 需要使用 split + explode 将多类型拆分为多行
      - 按年份和类型分别计数
    """
    print(f"\n{'='*70}")
    print("查询3: 时间维度趋势 —— 按年份统计各类型电影产量变化")
    print(f"{'='*70}")

    # ---- 数据准备：拆分多类型字段 ----
    # 一部电影可能属于多个类型（如"剧情/爱情"），需要拆分
    df_genres = df.filter(
        (col("genres").isNotNull()) &
        (col("genres") != "") &
        (col("genres") != "未知")
    ).select(
        "year",
        # explode: 将数组展开为多行，每个类型一行
        explode(split(col("genres"), "/")).alias("genre")  # 按"/"拆分类型
    )

    # 过滤掉空类型
    df_genres = df_genres.filter(
        (col("genre").isNotNull()) &
        (col("genre") != "") &
        (col("genre") != "\\N")
    )

    # ---- DataFrame API 方式 ----
    print("\n[方法1] 使用 DataFrame API（近10年各类型电影数量）:")
    recent_years_genre = df_genres.filter(col("year") >= 2015) \
        .groupBy("year", "genre") \
        .agg(count("*").alias("movie_count")) \
        .orderBy(col("year").desc(), col("movie_count").desc())

    recent_years_genre.show(30, truncate=False)

    # ---- 统计各类型总体排名 ----
    print("\n[各类型电影总体数量排名]:")
    genre_total = df_genres.groupBy("genre") \
        .agg(count("*").alias("total_count")) \
        .orderBy(col("total_count").desc()) \
        .limit(15)

    genre_total.show(15, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL（Top 5 类型按年趋势）:")

    # 先将拆分后的数据注册为临时视图
    df_genres.createOrReplaceTempView("movie_genres")

    # 找出Top5类型
    top5_genres = spark.sql("""
        SELECT genre, COUNT(*) as cnt
        FROM movie_genres
        GROUP BY genre
        ORDER BY cnt DESC
        LIMIT 5
    """)
    top5_list = [row["genre"] for row in top5_genres.collect()]
    top5_str = "', '".join(top5_list)

    print(f"\nTop5 类型: {top5_str}")

    # 按年份透视Top5类型
    spark.sql(f"""
        SELECT
            year,
            SUM(CASE WHEN genre = '{top5_list[0]}' THEN 1 ELSE 0 END) AS `{top5_list[0]}`,
            SUM(CASE WHEN genre = '{top5_list[1]}' THEN 1 ELSE 0 END) AS `{top5_list[1]}`,
            SUM(CASE WHEN genre = '{top5_list[2]}' THEN 1 ELSE 0 END) AS `{top5_list[2]}`,
            SUM(CASE WHEN genre = '{top5_list[3]}' THEN 1 ELSE 0 END) AS `{top5_list[3]}`,
            SUM(CASE WHEN genre = '{top5_list[4]}' THEN 1 ELSE 0 END) AS `{top5_list[4]}`
        FROM movie_genres
        WHERE year >= 2000
        GROUP BY year
        ORDER BY year DESC
    """).show(25, truncate=False)

    return df_genres


# =========================================================================
# 查询4: 窗口函数 —— 计算每个国家电影评分排名
# =========================================================================
def query4_window_function(spark, df):
    """
    查询4: 使用窗口函数计算每个国家内部电影的评分排名
    分析目的: 找出各国评分最高的电影，进行跨国对比
    技术要点:
      - 使用 row_number() / rank() / dense_rank() 窗口函数
      - PARTITION BY countries: 按国家分组
      - ORDER BY rating_score DESC: 组内按评分排序
    """
    print(f"\n{'='*70}")
    print("查询4: 窗口函数 —— 计算每个国家电影评分排名")
    print(f"{'='*70}")

    # 过滤条件：至少有一定数量的电影和评价
    df_filtered = df.filter(
        (col("rating_count") >= 100) &     # 至少100条评价
        (col("countries").isNotNull()) &
        (col("countries") != "") &
        (col("countries") != "未知")
    )

    # ---- 定义窗口规范 ----
    # PARTITION BY countries: 按国家分组
    # ORDER BY rating_score DESC: 组内按评分降序
    window_spec = Window.partitionBy("countries") \
        .orderBy(
            col("rating_score").desc(),
            col("rating_count").desc()      # 评分相同时按评价数
        )

    # ---- DataFrame API 方式：添加多种排名 ----
    print("\n[方法1] 使用 DataFrame API + 窗口函数:")
    df_ranked = df_filtered.select(
        "title",
        "year",
        "rating_score",
        "rating_count",
        "countries"
    ).withColumn(
        "rank_in_country",                              # 排名（row_number: 严格连续）
        row_number().over(window_spec)
    ).withColumn(
        "dense_rank_in_country",                        # 密集排名（dense_rank: 无间隙）
        dense_rank().over(window_spec)
    )

    # 注册临时视图用于SQL查询
    df_ranked.createOrReplaceTempView("ranked_movies")

    # 显示各国家Top3电影
    print("\n各国家评分前3的电影:")
    df_ranked.filter(col("rank_in_country") <= 3) \
        .orderBy("countries", "rank_in_country") \
        .show(50, truncate=False)

    # ---- Spark SQL 方式 ----
    print("\n[方法2] 使用 Spark SQL + 窗口函数:")
    spark.sql("""
        SELECT
            countries,
            title,
            year,
            rating_score,
            rating_count,
            ROW_NUMBER() OVER (
                PARTITION BY countries
                ORDER BY rating_score DESC, rating_count DESC
            ) AS `rank`,
            DENSE_RANK() OVER (
                PARTITION BY countries
                ORDER BY rating_score DESC
            ) AS dense_rank
        FROM movies
        WHERE rating_count >= 500
            AND countries IS NOT NULL
            AND countries != ''
            AND countries != '未知'
    """).createOrReplaceTempView("ranked_by_country")

    # 统计每个国家上榜电影数量
    print("\n各国家TOP1电影摘要（部分）:")
    spark.sql("""
        SELECT
            countries,
            title,
            rating_score,
            rating_count,
            year,
            `rank`
        FROM ranked_by_country
        WHERE `rank` = 1
        ORDER BY rating_score DESC
    """).show(20, truncate=False)

    return df_ranked


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

        # 3. 依次执行4个分析查询
        query1_group_by_aggregation(spark, df)
        query2_top_n_rating(spark, df)
        query3_genre_trend(spark, df)
        query4_window_function(spark, df)

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
