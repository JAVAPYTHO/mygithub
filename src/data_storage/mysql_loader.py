import argparse
import os

from pyspark.sql import SparkSession
import pymysql
from pymysql.cursors import DictCursor


MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'hive'),
    'password': os.getenv('MYSQL_PASSWORD', 'hive'),
    'database': os.getenv('MYSQL_DATABASE', 'metastore'),
    'charset': os.getenv('MYSQL_CHARSET', 'utf8mb4')
}


def create_spark_session(master='local[2]'):
    return SparkSession.builder.master(master).appName('MySQLLoader').getOrCreate()


def create_mysql_table():
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            sql = """
            CREATE TABLE IF NOT EXISTS user_features (
                user_id BIGINT PRIMARY KEY,
                active_days INT,
                days_since_last_behavior INT,
                pv_ratio DOUBLE,
                cart_ratio DOUBLE,
                buy_ratio DOUBLE,
                fav_ratio DOUBLE,
                cart2buy_rate DOUBLE,
                avg_behavior_interval DOUBLE,
                recent_7d_buy INT,
                item_category_count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(sql)
        conn.commit()
        print('MySQL表创建成功')
    finally:
        conn.close()


def _partition_upsert(rows):
    sql = """
    INSERT INTO user_features (
        user_id, active_days, days_since_last_behavior, pv_ratio, cart_ratio,
        buy_ratio, fav_ratio, cart2buy_rate, avg_behavior_interval,
        recent_7d_buy, item_category_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        active_days = VALUES(active_days),
        days_since_last_behavior = VALUES(days_since_last_behavior),
        pv_ratio = VALUES(pv_ratio),
        cart_ratio = VALUES(cart_ratio),
        buy_ratio = VALUES(buy_ratio),
        fav_ratio = VALUES(fav_ratio),
        cart2buy_rate = VALUES(cart2buy_rate),
        avg_behavior_interval = VALUES(avg_behavior_interval),
        recent_7d_buy = VALUES(recent_7d_buy),
        item_category_count = VALUES(item_category_count)
    """

    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            batch = []
            batch_size = 1000
            for row in rows:
                batch.append((
                    int(row['user_id']),
                    int(row['active_days'] or 0),
                    int(row['days_since_last_behavior'] or 0),
                    float(row['pv_ratio'] or 0),
                    float(row['cart_ratio'] or 0),
                    float(row['buy_ratio'] or 0),
                    float(row['fav_ratio'] or 0),
                    float(row['cart2buy_rate'] or 0),
                    float(row['avg_behavior_interval'] or 0),
                    int(row['recent_7d_buy'] or 0),
                    int(row['item_category_count'] or 0),
                ))
                if len(batch) >= batch_size:
                    cursor.executemany(sql, batch)
                    conn.commit()
                    batch.clear()

            if batch:
                cursor.executemany(sql, batch)
                conn.commit()
    finally:
        conn.close()


def load_to_mysql(input_path, spark_master='local[2]'):
    spark = create_spark_session(spark_master)
    try:
        df = spark.read.parquet(input_path)
        df = df.select(
            'user_id',
            'active_days',
            'days_since_last_behavior',
            'pv_ratio',
            'cart_ratio',
            'buy_ratio',
            'fav_ratio',
            'cart2buy_rate',
            'avg_behavior_interval',
            'recent_7d_buy',
            'item_category_count'
        ).dropDuplicates(['user_id'])

        print('开始分区写入 MySQL...')
        df.foreachPartition(_partition_upsert)
        print('MySQL 导入完成')
    finally:
        spark.stop()


def verify_mysql_data(limit=5):
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute('SELECT COUNT(*) as count FROM user_features')
            count = cursor.fetchone()['count']
            print(f'MySQL表中共有 {count} 条记录')

            cursor.execute(
                f"SELECT * FROM user_features ORDER BY active_days DESC LIMIT {int(limit)}"
            )
            rows = cursor.fetchall()
            print(f'\n活跃天数最多的{limit}个用户:')
            for row in rows:
                print(row)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='将特征数据导入MySQL')
    parser.add_argument('--input', default=os.getenv('HDFS_FEATURE_INPUT', 'hdfs://namenode:9000/user/behavior/features/user_behavior_features.parquet'))
    parser.add_argument('--spark-master', default=os.getenv('SPARK_MASTER', 'local[2]'))
    parser.add_argument('--verify-limit', type=int, default=5)
    args = parser.parse_args()

    print('开始创建MySQL表...')
    create_mysql_table()

    print('\n开始导入数据到MySQL...')
    load_to_mysql(args.input, args.spark_master)

    print('\n开始验证MySQL数据...')
    verify_mysql_data(args.verify_limit)


if __name__ == '__main__':
    main()
