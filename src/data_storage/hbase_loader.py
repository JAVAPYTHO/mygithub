import argparse
import os

from pyspark.sql import SparkSession
import happybase


HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', '9090'))
TABLE_NAME = os.getenv('HBASE_TABLE', 'user_features')
COLUMN_FAMILY = os.getenv('HBASE_COLUMN_FAMILY', 'f')


def create_spark_session(master='local[2]'):
    return SparkSession.builder.master(master).appName('HBaseLoader').getOrCreate()


def create_hbase_table():
    connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
    try:
        tables = connection.tables()
        if TABLE_NAME.encode() not in tables:
            connection.create_table(TABLE_NAME, {COLUMN_FAMILY: dict()})
            print(f"HBase表 {TABLE_NAME} 创建成功")
        else:
            print(f"HBase表 {TABLE_NAME} 已存在")
    finally:
        connection.close()


def _partition_put(rows):
    connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
    try:
        table = connection.table(TABLE_NAME)
        batch_size = 1000
        with table.batch(batch_size=batch_size) as b:
            for row in rows:
                user_id = str(row['user_id'])
                data = {
                    f'{COLUMN_FAMILY}:active_days': str(int(row['active_days'] or 0)),
                    f'{COLUMN_FAMILY}:days_since_last_behavior': str(int(row['days_since_last_behavior'] or 0)),
                    f'{COLUMN_FAMILY}:pv_ratio': str(float(row['pv_ratio'] or 0)),
                    f'{COLUMN_FAMILY}:cart_ratio': str(float(row['cart_ratio'] or 0)),
                    f'{COLUMN_FAMILY}:buy_ratio': str(float(row['buy_ratio'] or 0)),
                    f'{COLUMN_FAMILY}:fav_ratio': str(float(row['fav_ratio'] or 0)),
                    f'{COLUMN_FAMILY}:cart2buy_rate': str(float(row['cart2buy_rate'] or 0)),
                    f'{COLUMN_FAMILY}:avg_behavior_interval': str(float(row['avg_behavior_interval'] or 0)),
                    f'{COLUMN_FAMILY}:recent_7d_buy': str(int(row['recent_7d_buy'] or 0)),
                    f'{COLUMN_FAMILY}:item_category_count': str(int(row['item_category_count'] or 0)),
                }
                b.put(user_id, data)
    finally:
        connection.close()


def load_to_hbase(input_path, spark_master='local[2]'):
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

        print('开始分区写入 HBase...')
        df.foreachPartition(_partition_put)
        print('HBase 导入完成')
    finally:
        spark.stop()


def verify_hbase_data(limit=5):
    connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
    try:
        table = connection.table(TABLE_NAME)
        count = sum(1 for _ in table.scan())
        print(f'HBase表中共有 {count} 条记录')

        print(f'\n随机查询{limit}条记录:')
        for i, (key, data) in enumerate(table.scan(limit=limit), 1):
            print(f'--- 记录 {i} ---')
            print(f'user_id: {key.decode()}')
            for k, v in data.items():
                print(f"{k.decode()}: {v.decode()}")
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description='将特征数据导入HBase')
    parser.add_argument('--input', default=os.getenv('HDFS_FEATURE_INPUT', 'hdfs://namenode:9000/user/behavior/features/user_behavior_features.parquet'))
    parser.add_argument('--spark-master', default=os.getenv('SPARK_MASTER', 'local[2]'))
    parser.add_argument('--verify-limit', type=int, default=5)
    args = parser.parse_args()

    print('开始创建HBase表...')
    create_hbase_table()

    print('\n开始导入数据到HBase...')
    load_to_hbase(args.input, args.spark_master)

    print('\n开始验证HBase数据...')
    verify_hbase_data(args.verify_limit)


if __name__ == '__main__':
    main()
