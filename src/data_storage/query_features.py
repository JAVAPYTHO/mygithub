import argparse
import os
import pymysql
from pymysql.cursors import DictCursor
import happybase

# MySQL配置
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'hive'),
    'password': os.getenv('MYSQL_PASSWORD', 'hive'),
    'database': os.getenv('MYSQL_DATABASE', 'metastore'),
    'charset': os.getenv('MYSQL_CHARSET', 'utf8mb4')
}

# HBase配置
HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', '9090'))
TABLE_NAME = 'user_features'
COLUMN_FAMILY = 'f'

def query_user_features_mysql(user_id=None, top_n=10, min_active_days=None, max_active_days=None,
                             min_buy_ratio=None, max_buy_ratio=None, order_by=None, desc=False,
                             fields=None, limit=None, offset=None):
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor(DictCursor) as cursor:
            # 字段选择
            select_fields = ', '.join(fields) if fields else '*'
            sql = f"SELECT {select_fields} FROM user_features"
            where = []
            params = []
            if user_id is not None:
                where.append("user_id = %s")
                params.append(user_id)
            if min_active_days is not None:
                where.append("active_days >= %s")
                params.append(min_active_days)
            if max_active_days is not None:
                where.append("active_days <= %s")
                params.append(max_active_days)
            if min_buy_ratio is not None:
                where.append("buy_ratio >= %s")
                params.append(min_buy_ratio)
            if max_buy_ratio is not None:
                where.append("buy_ratio <= %s")
                params.append(max_buy_ratio)
            if where:
                sql += " WHERE " + " AND ".join(where)
            if order_by:
                sql += f" ORDER BY {order_by} {'DESC' if desc else 'ASC'}"
            if limit is not None:
                sql += f" LIMIT {limit}"
                if offset is not None:
                    sql += f" OFFSET {offset}"
            elif top_n is not None:
                sql += f" LIMIT {top_n}"
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
            return rows
    except Exception as e:
        print(f"MySQL查询失败: {str(e)}")
        return []
    finally:
        conn.close()

def query_user_features_hbase(user_id=None, top_n=10, min_active_days=None, max_active_days=None,
                             min_buy_ratio=None, max_buy_ratio=None, order_by=None, desc=False,
                             fields=None, limit=None, offset=None):
    connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
    try:
        table = connection.table(TABLE_NAME)
        results = []
        # 只支持rowkey精确查找或全表扫描
        if user_id is not None:
            row = table.row(str(user_id))
            if row:
                row_dict = {'user_id': user_id}
                for k, v in row.items():
                    row_dict[k.decode().split(':')[1]] = v.decode()
                results.append(row_dict)
        else:
            for key, data in table.scan():
                row_dict = {'user_id': key.decode()}
                for k, v in data.items():
                    row_dict[k.decode().split(':')[1]] = v.decode()
                results.append(row_dict)
            # 内存过滤
            if min_active_days is not None:
                results = [r for r in results if float(r.get('active_days', 0)) >= float(min_active_days)]
            if max_active_days is not None:
                results = [r for r in results if float(r.get('active_days', 0)) <= float(max_active_days)]
            if min_buy_ratio is not None:
                results = [r for r in results if float(r.get('buy_ratio', 0)) >= float(min_buy_ratio)]
            if max_buy_ratio is not None:
                results = [r for r in results if float(r.get('buy_ratio', 0)) <= float(max_buy_ratio)]
            if order_by:
                results.sort(key=lambda x: float(x.get(order_by, 0)), reverse=desc)
            if offset is not None:
                results = results[offset:]
            if limit is not None:
                results = results[:limit]
            elif top_n is not None:
                results = results[:top_n]
        # 字段筛选
        if fields:
            results = [ {k: v for k, v in r.items() if k in fields or k=='user_id'} for r in results ]
        return results
    except Exception as e:
        print(f"HBase查询失败: {str(e)}")
        return []
    finally:
        connection.close()

def pretty_print(results):
    if not results:
        print("无查询结果！")
        return
    for i, row in enumerate(results, 1):
        print(f"--- 记录 {i} ---")
        for k, v in row.items():
            print(f"{k}: {v}")
        print()

def main():
    parser = argparse.ArgumentParser(description="用户特征数据查询工具（MySQL/HBase）")
    parser.add_argument('--db', choices=['mysql', 'hbase'], required=True, help='查询数据库类型')
    parser.add_argument('--user_id', type=str, help='指定用户ID')
    parser.add_argument('--topn', type=int, default=10, help='返回前N条（默认10）')
    parser.add_argument('--min_active_days', type=float, help='最小活跃天数')
    parser.add_argument('--max_active_days', type=float, help='最大活跃天数')
    parser.add_argument('--min_buy_ratio', type=float, help='最小购买比率')
    parser.add_argument('--max_buy_ratio', type=float, help='最大购买比率')
    parser.add_argument('--order_by', type=str, help='排序字段')
    parser.add_argument('--desc', action='store_true', help='降序排序')
    parser.add_argument('--fields', type=str, help='只返回指定字段，逗号分隔')
    parser.add_argument('--limit', type=int, help='返回条数')
    parser.add_argument('--offset', type=int, help='跳过前N条')
    args = parser.parse_args()

    fields = [f.strip() for f in args.fields.split(',')] if args.fields else None

    query_args = dict(
        user_id=args.user_id,
        top_n=args.topn,
        min_active_days=args.min_active_days,
        max_active_days=args.max_active_days,
        min_buy_ratio=args.min_buy_ratio,
        max_buy_ratio=args.max_buy_ratio,
        order_by=args.order_by,
        desc=args.desc,
        fields=fields,
        limit=args.limit,
        offset=args.offset
    )

    if args.db == 'mysql':
        results = query_user_features_mysql(**query_args)
    else:
        results = query_user_features_hbase(**query_args)
    pretty_print(results)

if __name__ == "__main__":
    main() 