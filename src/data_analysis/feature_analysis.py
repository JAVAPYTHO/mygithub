import argparse
import os

from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt


def ensure_output_dirs(data_dir, report_dir, figure_dir):
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(report_dir, exist_ok=True)
    os.makedirs(figure_dir, exist_ok=True)


def run_analysis(hdfs_input, spark_master, sample_limit, data_dir, report_dir, figure_dir, show=False):
    ensure_output_dirs(data_dir, report_dir, figure_dir)

    spark = SparkSession.builder.master(spark_master).appName("FeatureAnalysis").getOrCreate()
    try:
        df = spark.read.parquet(hdfs_input)

        print("=== 前 10 条数据 ===")
        df.show(10)

        print("=== 字段分布 ===")
        df.describe().show()

        print("=== 字段类型 ===")
        df.printSchema()

        sample_csv_path = os.path.join(data_dir, "sample_features.csv")
        df.limit(sample_limit).toPandas().to_csv(sample_csv_path, index=False)

        df_sample = pd.read_csv(sample_csv_path)
        print("=== 样本数据前 5 条 ===")
        print(df_sample.head())
        print("=== 样本数据统计描述 ===")
        print(df_sample.describe())

        plt.figure(figsize=(10, 6))
        df_sample['buy_ratio'].hist(bins=50)
        plt.title("User Buy Ratio Distribution")
        plt.xlabel("Buy Ratio")
        plt.ylabel("User Count")
        buy_ratio_fig_path = os.path.join(figure_dir, "buy_ratio_distribution.png")
        plt.savefig(buy_ratio_fig_path)
        if show:
            plt.show()
        plt.close()

        report_path = os.path.join(report_dir, "feature_analysis_results.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=== 特征分析结果 ===\n")
            f.write("1. 前 10 条数据：\n")
            f.write(str(df.limit(10).toPandas()) + "\n\n")
            f.write("2. 字段分布：\n")
            f.write(str(df.describe().toPandas()) + "\n\n")
            f.write("3. 字段类型：\n")
            f.write(str(df.schema) + "\n\n")
            f.write("4. 样本数据前 5 条：\n")
            f.write(str(df_sample.head()) + "\n\n")
            f.write("5. 样本数据统计描述：\n")
            f.write(str(df_sample.describe()) + "\n\n")
            f.write(f"6. 可视化结果已保存为 {buy_ratio_fig_path}\n")

        print(f"样本数据已输出: {sample_csv_path}")
        print(f"分析报告已输出: {report_path}")
        print(f"图表已输出: {buy_ratio_fig_path}")
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="特征分析脚本")
    parser.add_argument(
        "--input",
        default=os.getenv("HDFS_FEATURE_INPUT", "hdfs://namenode:9000/user/behavior/features/user_behavior_features.parquet"),
    )
    parser.add_argument("--spark-master", default=os.getenv("SPARK_MASTER", "local[2]"))
    parser.add_argument("--sample-limit", type=int, default=1000)
    parser.add_argument("--output-data-dir", default=os.getenv("OUTPUT_DATA_DIR", "outputs/data_samples"))
    parser.add_argument("--output-report-dir", default=os.getenv("OUTPUT_REPORT_DIR", "outputs/reports"))
    parser.add_argument("--output-figure-dir", default=os.getenv("OUTPUT_FIGURE_DIR", "outputs/figures"))
    parser.add_argument("--show", action="store_true", help="是否展示图表窗口（默认仅保存）")
    args = parser.parse_args()

    run_analysis(
        hdfs_input=args.input,
        spark_master=args.spark_master,
        sample_limit=args.sample_limit,
        data_dir=args.output_data_dir,
        report_dir=args.output_report_dir,
        figure_dir=args.output_figure_dir,
        show=args.show,
    )


if __name__ == "__main__":
    main()
