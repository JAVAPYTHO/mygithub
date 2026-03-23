import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    avg,
    datediff,
    to_date,
    from_unixtime,
    lag,
    countDistinct,
    max,
    hour,
    dayofweek,
    unix_timestamp,
    current_date,
    to_timestamp,
)
from pyspark.sql.window import Window


def create_spark_session():
    """创建优化配置的Spark会话"""
    return (
        SparkSession.builder
        .appName("UserBehaviorFeatureEngineering")
        .config("spark.master", os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )


def calculate_features(df):
    """计算用户行为特征"""
    df = df.withColumn("timestamp_ts", to_timestamp(from_unixtime(col("timestamp"))))
    df = df.withColumn("hour", hour(col("timestamp_ts")))
    df = df.withColumn("day_of_week", dayofweek(col("timestamp_ts")))

    window_spec_lag = Window.partitionBy("user_id").orderBy("timestamp_ts")
    user_window = Window.partitionBy("user_id")

    window_spec_1 = Window.partitionBy("user_id").orderBy("timestamp_ts").rowsBetween(-1, 0)
    window_spec_7 = Window.partitionBy("user_id").orderBy("timestamp_ts").rowsBetween(-7, 0)
    window_spec_30 = Window.partitionBy("user_id").orderBy("timestamp_ts").rowsBetween(-30, 0)

    df = df.withColumn("pv_count_1d", count("behavior_type").over(window_spec_1))
    df = df.withColumn("pv_count_7d", count("behavior_type").over(window_spec_7))
    df = df.withColumn("pv_count_30d", count("behavior_type").over(window_spec_30))

    df = df.withColumn("item_pv_count_1d", count(when(col("behavior_type") == "pv", 1)).over(window_spec_1))
    df = df.withColumn("item_cart_count_1d", count(when(col("behavior_type") == "cart", 1)).over(window_spec_1))
    df = df.withColumn("item_buy_count_1d", count(when(col("behavior_type") == "buy", 1)).over(window_spec_1))

    df = df.withColumn(
        "time_since_last_behavior",
        unix_timestamp("timestamp_ts") - lag(unix_timestamp("timestamp_ts")).over(window_spec_lag),
    )

    df = df.withColumn("item_category_count", countDistinct("item_id").over(user_window))
    df = df.withColumn("active_days", countDistinct(to_date(col("timestamp_ts"))).over(user_window))

    df = df.withColumn("last_behavior_time", max(col("timestamp_ts")).over(user_window))
    df = df.withColumn("days_since_last_behavior", datediff(current_date(), to_date(col("last_behavior_time"))))

    total_behaviors = count("behavior_type").over(user_window)
    df = df.withColumn("pv_ratio", count(when(col("behavior_type") == "pv", 1)).over(user_window) / total_behaviors)
    df = df.withColumn("cart_ratio", count(when(col("behavior_type") == "cart", 1)).over(user_window) / total_behaviors)
    df = df.withColumn("buy_ratio", count(when(col("behavior_type") == "buy", 1)).over(user_window) / total_behaviors)
    df = df.withColumn("fav_ratio", count(when(col("behavior_type") == "fav", 1)).over(user_window) / total_behaviors)

    cart_count = count(when(col("behavior_type") == "cart", 1)).over(user_window)
    buy_count = count(when(col("behavior_type") == "buy", 1)).over(user_window)
    df = df.withColumn("cart2buy_rate", when(cart_count > 0, buy_count / cart_count).otherwise(0))

    df = df.withColumn(
        "behavior_interval",
        unix_timestamp("timestamp_ts") - lag(unix_timestamp("timestamp_ts")).over(window_spec_lag),
    )
    df = df.withColumn("avg_behavior_interval", avg("behavior_interval").over(user_window))

    recent_7d_buy = max(
        when(
            (col("behavior_type") == "buy") & (datediff(current_date(), to_date(col("timestamp_ts"))) <= 7),
            1,
        ).otherwise(0)
    ).over(user_window)
    df = df.withColumn("recent_7d_buy", recent_7d_buy)

    return df


def main():
    start_time = time.time()
    spark = create_spark_session()

    input_path = os.getenv("HDFS_CLEANED_INPUT", "hdfs://namenode:9000/user/behavior/cleaned/user_behavior_cleaned")
    output_path = os.getenv("HDFS_FEATURE_OUTPUT", "hdfs://namenode:9000/user/behavior/features/user_behavior_features.parquet")

    try:
        print("Reading cleaned data...")
        df = spark.read.parquet(input_path)

        print("Calculating features...")
        features_df = calculate_features(df)

        print("Writing features to HDFS...")
        features_df.write.mode("overwrite").parquet(output_path)

        print("Feature engineering completed successfully!")
        print(f"总耗时: {(time.time() - start_time) / 60:.2f} 分钟")
    except Exception as e:
        print(f"Error during feature engineering: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
