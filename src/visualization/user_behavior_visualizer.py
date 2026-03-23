import argparse
import os

import happybase
import matplotlib.pyplot as plt
import pandas as pd
import pymysql
import seaborn as sns

plt.rcParams['font.sans-serif'] = ['WenQuanYi Zen Hei']
plt.rcParams['axes.unicode_minus'] = False

MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'hive'),
    'password': os.getenv('MYSQL_PASSWORD', 'hive'),
    'database': os.getenv('MYSQL_DATABASE', 'metastore'),
    'charset': os.getenv('MYSQL_CHARSET', 'utf8mb4')
}

HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', '9090'))
TABLE_NAME = 'user_features'
COLUMN_FAMILY = 'f'

OUTPUT_FIGURE_DIR = os.getenv('OUTPUT_FIGURE_DIR', 'outputs/figures')
os.makedirs(OUTPUT_FIGURE_DIR, exist_ok=True)


def output_figure_path(filename):
    return os.path.join(OUTPUT_FIGURE_DIR, filename)


def finalize_plot(filename, show=False):
    plt.tight_layout()
    plt.savefig(output_figure_path(filename))
    if show:
        plt.show()
    plt.close()
    print(f"Saved: {filename}")


def fetch_data_from_db(db_type, columns):
    df = pd.DataFrame()
    if db_type == 'mysql':
        conn = None
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            query = f"SELECT {', '.join(columns)} FROM user_features"
            df = pd.read_sql(query, conn)
        except Exception as e:
            print(f"从MySQL获取数据失败: {str(e)}")
        finally:
            if conn:
                conn.close()
    elif db_type == 'hbase':
        connection = None
        try:
            connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
            table = connection.table(TABLE_NAME)
            rows = []
            for key, data in table.scan():
                row = {'user_id': key.decode()}
                for col in columns:
                    hcol = f'{COLUMN_FAMILY}:{col}'.encode('utf-8')
                    row[col] = data[hcol].decode() if hcol in data else None
                rows.append(row)
            df = pd.DataFrame(rows)
            for col in columns:
                if col != 'user_id' and col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        except Exception as e:
            print(f"从HBase获取数据失败: {str(e)}")
        finally:
            if connection:
                connection.close()
    else:
        print('不支持的数据库类型！')
    return df


def active_bins(max_active_days):
    if max_active_days <= 1:
        return [0, max_active_days + 1], ['0d']
    if max_active_days <= 7:
        return [0, 1, max_active_days + 1], ['0d', '1-7d']
    if max_active_days <= 30:
        return [0, 1, 7, max_active_days + 1], ['0d', '1-7d', '8-30d']
    if max_active_days <= 90:
        return [0, 1, 7, 30, max_active_days + 1], ['0d', '1-7d', '8-30d', '31-90d']
    if max_active_days <= 365:
        return [0, 1, 7, 30, 90, max_active_days + 1], ['0d', '1-7d', '8-30d', '31-90d', '91-365d']
    return [0, 1, 7, 30, 90, 365, max_active_days + 1], ['0d', '1-7d', '8-30d', '31-90d', '91-365d', '>365d']


def plot_active_days_distribution(db_type, show=False):
    df = fetch_data_from_db(db_type, ['active_days'])
    if df.empty:
        return
    df['active_days'] = pd.to_numeric(df['active_days'], errors='coerce')
    df = df[df['active_days'].notna()]
    if df.empty:
        return
    plt.figure(figsize=(10, 6))
    sns.histplot(df['active_days'], bins=range(0, int(df['active_days'].max()) + 2), kde=True)
    plt.title('User Active Days Distribution')
    plt.xlabel('Active Days')
    plt.ylabel('User Count')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    finalize_plot(f'active_days_distribution_{db_type}.png', show)


def plot_buy_ratio_distribution(db_type, show=False):
    df = fetch_data_from_db(db_type, ['buy_ratio'])
    if df.empty:
        return
    df['buy_ratio'] = pd.to_numeric(df['buy_ratio'], errors='coerce')
    df = df[df['buy_ratio'].notna()]
    if df.empty:
        return
    plt.figure(figsize=(10, 6))
    sns.histplot(df['buy_ratio'], bins=20, kde=True)
    plt.title('User Buy Ratio Distribution')
    plt.xlabel('Buy Ratio')
    plt.ylabel('User Count')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    finalize_plot(f'buy_ratio_distribution_{db_type}.png', show)


def plot_active_level_pie(db_type, show=False):
    df = fetch_data_from_db(db_type, ['active_days'])
    if df.empty:
        return
    df['active_days'] = pd.to_numeric(df['active_days'], errors='coerce')
    df = df[df['active_days'].notna()]
    if df.empty:
        return
    bins, labels = active_bins(df['active_days'].max())
    df['active_level'] = pd.cut(df['active_days'], bins=bins, labels=labels, right=False)
    level_counts = df['active_level'].value_counts().sort_index()
    plt.figure(figsize=(8, 8))
    plt.pie(level_counts, labels=level_counts.index, autopct='%1.1f%%', startangle=140)
    plt.title('User Active Level Distribution')
    finalize_plot(f'active_level_pie_{db_type}.png', show)


def plot_top_active_users_bar(db_type, top_n=10, show=False):
    df = fetch_data_from_db(db_type, ['user_id', 'active_days'])
    if df.empty:
        return
    df['active_days'] = pd.to_numeric(df['active_days'], errors='coerce')
    df = df[df['active_days'].notna()].sort_values('active_days', ascending=False).head(top_n)
    if df.empty:
        return
    plt.figure(figsize=(12, 6))
    sns.barplot(x='user_id', y='active_days', data=df)
    plt.title(f'Top {top_n} Users by Active Days')
    plt.xlabel('User ID')
    plt.ylabel('Active Days')
    plt.xticks(rotation=45)
    finalize_plot(f'top_active_users_{db_type}.png', show)


def plot_active_days_vs_buy_ratio_boxplot(db_type, show=False):
    df = fetch_data_from_db(db_type, ['active_days', 'buy_ratio'])
    if df.empty:
        return
    df['active_days'] = pd.to_numeric(df['active_days'], errors='coerce')
    df['buy_ratio'] = pd.to_numeric(df['buy_ratio'], errors='coerce')
    df = df[df['active_days'].notna() & df['buy_ratio'].notna()]
    if df.empty:
        return
    bins, labels = active_bins(df['active_days'].max())
    df['active_days_group'] = pd.cut(df['active_days'], bins=bins, labels=labels, right=False)
    plt.figure(figsize=(12, 7))
    sns.boxplot(x='active_days_group', y='buy_ratio', data=df, order=labels, palette='viridis')
    plt.title('Buy Ratio by Active Days Group')
    plt.xlabel('Active Days Group')
    plt.ylabel('Buy Ratio')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    finalize_plot(f'active_days_vs_buy_ratio_boxplot_{db_type}.png', show)


def plot_active_days_vs_category_scatter(db_type, show=False):
    df = fetch_data_from_db(db_type, ['active_days', 'item_category_count'])
    if df.empty:
        return
    df['active_days'] = pd.to_numeric(df['active_days'], errors='coerce')
    df['item_category_count'] = pd.to_numeric(df['item_category_count'], errors='coerce')
    df = df[df['active_days'].notna() & df['item_category_count'].notna()]
    if df.empty:
        return
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='active_days', y='item_category_count', data=df, alpha=0.5)
    plt.title('Active Days vs Item Category Count')
    plt.xlabel('Active Days')
    plt.ylabel('Item Category Count')
    finalize_plot(f'active_days_vs_category_scatter_{db_type}.png', show)


def plot_violin_active_days_vs_buy_ratio(db_type, show=False):
    df = fetch_data_from_db(db_type, ['active_days', 'buy_ratio'])
    if df.empty:
        return
    df['active_days'] = pd.to_numeric(df['active_days'], errors='coerce')
    df['buy_ratio'] = pd.to_numeric(df['buy_ratio'], errors='coerce')
    df = df[df['active_days'].notna() & df['buy_ratio'].notna()]
    if df.empty:
        return
    bins, labels = active_bins(df['active_days'].max())
    df['active_days_group'] = pd.cut(df['active_days'], bins=bins, labels=labels, right=False)
    plt.figure(figsize=(12, 7))
    sns.violinplot(x='active_days_group', y='buy_ratio', data=df, order=labels, palette='Set2')
    plt.title('Violin Plot: Buy Ratio by Active Days Group')
    plt.xlabel('Active Days Group')
    plt.ylabel('Buy Ratio')
    finalize_plot(f'violin_active_days_vs_buy_ratio_{db_type}.png', show)


def plot_heatmap_active_days_vs_category(db_type, show=False):
    df = fetch_data_from_db(db_type, ['active_days', 'item_category_count'])
    if df.empty:
        return
    df['active_days'] = pd.to_numeric(df['active_days'], errors='coerce')
    df['item_category_count'] = pd.to_numeric(df['item_category_count'], errors='coerce')
    df = df[df['active_days'].notna() & df['item_category_count'].notna()]
    if df.empty:
        return
    plt.figure(figsize=(10, 8))
    heatmap_data = pd.crosstab(df['active_days'], df['item_category_count'])
    sns.heatmap(heatmap_data, cmap='YlGnBu')
    plt.title('Heatmap: Active Days vs Item Category Count')
    plt.xlabel('Item Category Count')
    plt.ylabel('Active Days')
    finalize_plot(f'heatmap_active_days_vs_category_{db_type}.png', show)


def plot_ecdf_buy_ratio(db_type, show=False):
    df = fetch_data_from_db(db_type, ['buy_ratio'])
    if df.empty:
        return
    df['buy_ratio'] = pd.to_numeric(df['buy_ratio'], errors='coerce')
    df = df[df['buy_ratio'].notna()]
    if df.empty:
        return
    plt.figure(figsize=(10, 6))
    sns.ecdfplot(df['buy_ratio'])
    plt.title('ECDF: Buy Ratio')
    plt.xlabel('Buy Ratio')
    plt.ylabel('ECDF')
    finalize_plot(f'ecdf_buy_ratio_{db_type}.png', show)


def plot_pairplot_features(db_type, show=False):
    cols = ['active_days', 'item_category_count', 'buy_ratio', 'cart2buy_rate']
    df = fetch_data_from_db(db_type, cols)
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna()
    if df.empty:
        return
    g = sns.pairplot(df[cols], diag_kind='kde')
    g.figure.suptitle('Pairplot of Key Features', y=1.02)
    g.figure.tight_layout()
    g.figure.savefig(output_figure_path(f'pairplot_features_{db_type}.png'))
    if show:
        plt.show()
    plt.close(g.figure)
    print(f"Saved: pairplot_features_{db_type}.png")


def main():
    parser = argparse.ArgumentParser(description='User Behavior Data Visualization Tool (MySQL/HBase)')
    parser.add_argument('--db', choices=['mysql', 'hbase'], required=True, help='Select data source')
    parser.add_argument('--show', action='store_true', help='是否展示图表窗口（默认仅保存）')
    parser.add_argument('--topn', type=int, default=10, help='活跃用户柱状图 TopN')
    args = parser.parse_args()

    plot_active_days_distribution(args.db, args.show)
    plot_buy_ratio_distribution(args.db, args.show)
    plot_active_level_pie(args.db, args.show)
    plot_top_active_users_bar(args.db, top_n=args.topn, show=args.show)
    plot_active_days_vs_buy_ratio_boxplot(args.db, args.show)
    plot_active_days_vs_category_scatter(args.db, args.show)
    plot_violin_active_days_vs_buy_ratio(args.db, args.show)
    plot_heatmap_active_days_vs_category(args.db, args.show)
    plot_ecdf_buy_ratio(args.db, args.show)
    plot_pairplot_features(args.db, args.show)
    print('All visualizations generated.')


if __name__ == '__main__':
    main()
