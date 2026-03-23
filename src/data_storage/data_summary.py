import os
import pymysql
from pymysql.cursors import DictCursor

# MySQL配置（优先读取环境变量）
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'user_behavior'),
    'charset': os.getenv('MYSQL_CHARSET', 'utf8mb4')
}

def get_data_summary():
    """获取数据概览统计"""
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor(DictCursor) as cursor:
            print("=== 用户行为数据概览 ===\n")

            cursor.execute("SELECT COUNT(*) as total_users FROM user_features")
            total_users = cursor.fetchone()['total_users']
            print(f"总用户数: {total_users:,}")

            cursor.execute("""
                SELECT
                    MIN(active_days) as min_active_days,
                    MAX(active_days) as max_active_days,
                    AVG(active_days) as avg_active_days,
                    COUNT(CASE WHEN active_days = 1 THEN 1 END) as single_day_users,
                    COUNT(CASE WHEN active_days >= 5 THEN 1 END) as high_active_users
                FROM user_features
            """)
            active_stats = cursor.fetchone()
            print(f"\n活跃天数统计:")
            print(f"  最小活跃天数: {active_stats['min_active_days']}")
            print(f"  最大活跃天数: {active_stats['max_active_days']}")
            print(f"  平均活跃天数: {active_stats['avg_active_days']:.2f}")
            print(f"  单日活跃用户: {active_stats['single_day_users']:,}")
            print(f"  高活跃用户(≥5天): {active_stats['high_active_users']:,}")

            cursor.execute("""
                SELECT
                    COUNT(CASE WHEN buy_ratio > 0 THEN 1 END) as buyers,
                    COUNT(CASE WHEN buy_ratio = 0 THEN 1 END) as non_buyers,
                    AVG(buy_ratio) as avg_buy_ratio,
                    MAX(buy_ratio) as max_buy_ratio
                FROM user_features
            """)
            buy_stats = cursor.fetchone()
            print(f"\n购买行为统计:")
            print(f"  有购买行为的用户: {buy_stats['buyers']:,}")
            print(f"  无购买行为的用户: {buy_stats['non_buyers']:,}")
            print(f"  平均购买比率: {buy_stats['avg_buy_ratio']:.4f}")
            print(f"  最高购买比率: {buy_stats['max_buy_ratio']}")

    except Exception as e:
        print(f"数据统计失败: {str(e)}")
    finally:
        conn.close()

def get_user_segments():
    """获取用户分群统计"""
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor(DictCursor) as cursor:
            print("\n=== 用户分群统计 ===\n")
            cursor.execute("""
                SELECT
                    CASE
                        WHEN active_days = 1 THEN '单日用户'
                        WHEN active_days BETWEEN 2 AND 4 THEN '低频用户'
                        WHEN active_days BETWEEN 5 AND 9 THEN '中频用户'
                        ELSE '高频用户'
                    END as user_segment,
                    COUNT(*) as user_count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM user_features), 2) as percentage
                FROM user_features
                GROUP BY
                    CASE
                        WHEN active_days = 1 THEN '单日用户'
                        WHEN active_days BETWEEN 2 AND 4 THEN '低频用户'
                        WHEN active_days BETWEEN 5 AND 9 THEN '中频用户'
                        ELSE '高频用户'
                    END
                ORDER BY user_count DESC
            """)
            segments = cursor.fetchall()
            print("按活跃天数分群:")
            for segment in segments:
                print(f"  {segment['user_segment']}: {segment['user_count']:,} 用户 ({segment['percentage']}%)")

    except Exception as e:
        print(f"用户分群统计失败: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    get_data_summary()
    get_user_segments()
