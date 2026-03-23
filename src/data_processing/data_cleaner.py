import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, count, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

def create_spark_session():
    """创建Spark会话"""
    return SparkSession.builder \
        .appName("UserBehaviorAnalysis") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

def load_data(spark, input_path):
    """加载数据并定义schema"""
    schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("item_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("behavior_type", StringType(), True),
        StructField("timestamp", LongType(), True)
    ])
    
    return spark.read.csv(input_path, schema=schema, header=False)

def clean_data(df):
    """数据清洗"""
    # 转换时间戳为可读时间
    df = df.withColumn("datetime", from_unixtime(col("timestamp")))
    
    # 删除重复数据
    df = df.dropDuplicates(["user_id", "item_id", "behavior_type", "timestamp"])
    
    # 删除无效数据
    df = df.filter(col("user_id").isNotNull() & 
                  col("item_id").isNotNull() & 
                  col("category_id").isNotNull() & 
                  col("behavior_type").isNotNull() & 
                  col("timestamp").isNotNull())
    
    # 验证行为类型
    valid_behaviors = ["pv", "buy", "cart", "fav"]
    df = df.filter(col("behavior_type").isin(valid_behaviors))
    
    return df

def validate_data(df):
    """数据验证"""
    # 计算基本统计信息
    total_records = df.count()
    user_count = df.select("user_id").distinct().count()
    item_count = df.select("item_id").distinct().count()
    category_count = df.select("category_id").distinct().count()
    
    # 行为类型分布
    behavior_stats = df.groupBy("behavior_type").count()
    
    print(f"数据验证结果:")
    print(f"总记录数: {total_records}")
    print(f"独立用户数: {user_count}")
    print(f"独立商品数: {item_count}")
    print(f"独立类别数: {category_count}")
    print("\n行为类型分布:")
    behavior_stats.show()

def main():
    # 创建Spark会话
    spark = create_spark_session()
    
    # 设置输入输出路径（支持环境变量覆盖）
    input_path = os.getenv("HDFS_RAW_INPUT", "hdfs://namenode:9000/user/behavior/raw/UserBehavior.csv")
    output_path = os.getenv("HDFS_CLEANED_OUTPUT", "hdfs://namenode:9000/user/behavior/cleaned/user_behavior_cleaned")
    
    try:
        # 加载数据
        print("正在加载数据...")
        df = load_data(spark, input_path)
        
        # 清洗数据
        print("正在清洗数据...")
        cleaned_df = clean_data(df)
        
        # 验证数据
        print("正在验证数据...")
        validate_data(cleaned_df)
        
        # 保存清洗后的数据
        print("正在保存清洗后的数据...")
        cleaned_df.write.mode("overwrite").parquet(output_path)
        
        print("数据处理完成！")
        
    except Exception as e:
        print(f"处理过程中出现错误: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 