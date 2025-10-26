from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, min as spark_min, max as spark_max,
    count as spark_count, concat_ws
)
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import sys
import matplotlib
matplotlib.use("Agg")

# ==========================================
# Problem 2: Cluster Usage Analysis (Cluster Version)
# ==========================================

# ---------- 参数解析 ----------
if len(sys.argv) < 2:
    print("Usage: uv run python problem2_cluster.py spark://<MASTER_PRIVATE_IP>:7077 [--skip-spark]")
    sys.exit(1)

spark_master = sys.argv[1]
skip_spark = "--skip-spark" in sys.argv

# ---------- 输出路径 ----------
output_base = "/home/ubuntu/data/output/problem2"
os.makedirs(output_base, exist_ok=True)

# ---------- 如果跳过 Spark，直接加载 CSV 绘图 ----------
if skip_spark:
    print("Skipping Spark processing, regenerating visualizations...")
    apps_pd = pd.read_csv(f"{output_base}/problem2_timeline.csv")
    summary_pd = pd.read_csv(f"{output_base}/problem2_cluster_summary.csv")
else:
    # ---------- 创建 Spark Session ----------
    spark = (
        SparkSession.builder
        .appName("Problem2_ClusterUsage_Cluster")
        .master(spark_master)
        .getOrCreate()
    )

    bucket = os.environ.get("SPARK_LOGS_BUCKET")
    if not bucket:
        raise ValueError("SPARK_LOGS_BUCKET environment variable not set!")

    # ---------- 读取数据 ----------
    input_path = f"{bucket}/data/application_*/*.log"
    print(f"Loading logs from: {input_path}")
    logs_df = spark.read.text(input_path)

    # ---------- 正则提取 ----------
    pattern = r"application_(\d+)_(\d+)"
    time_pattern = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"


    parsed_df = logs_df.select(
        regexp_extract("value", pattern, 1).alias("cluster_id"),
        regexp_extract("value", pattern, 2).alias("app_number"),
        regexp_extract("value", time_pattern, 1).alias("timestamp"),
        col("value").alias("log_line")
    ).filter(col("cluster_id") != "")

    # ---------- 每个 application 的起止时间 ----------
    apps_df = (
        parsed_df.groupBy("cluster_id", "app_number")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
        .withColumn(
            "application_id",
            concat_ws("_", col("cluster_id").cast("string"), col("app_number").cast("string"))
        )
    )

    # ---------- 每个 cluster 的汇总 ----------
    cluster_summary = (
        apps_df.groupBy("cluster_id")
        .agg(
            spark_count("application_id").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app")
        )
    )

    # ---------- 总体统计 ----------
    total_clusters = cluster_summary.count()
    total_apps = apps_df.count()
    avg_per_cluster = total_apps / total_clusters if total_clusters > 0 else 0

    summary_text = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_per_cluster:.2f}",
    ]

    # ---------- Top clusters ----------
    top_clusters = (
        cluster_summary
        .orderBy(col("num_applications").desc())
        .limit(5)
        .collect()
    )
    summary_text.append("\nMost heavily used clusters:")
    for row in top_clusters:
        summary_text.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

    # ---------- 输出到文件 ----------
    print("\n".join(summary_text))
    apps_df.coalesce(1).write.csv(f"{output_base}/problem2_timeline", header=True, mode="overwrite")
    cluster_summary.coalesce(1).write.csv(f"{output_base}/problem2_cluster_summary", header=True, mode="overwrite")

    # ✅ 等待 Spark 写入完成并确保文件存在
    import time, glob, shutil

    def wait_for_file(path_pattern, retries=10, delay=2):
        """等待文件生成"""
        for _ in range(retries):
            matches = glob.glob(path_pattern)
            if matches:
                print(f"✅ Found file: {matches[0]}")
                return matches[0]
            print(f"⏳ Waiting for {path_pattern} to appear...")
            time.sleep(delay)
        raise FileNotFoundError(f"❌ File not found after {retries * delay} seconds: {path_pattern}")

    print("⏳ Ensuring Spark has finished committing output files...")

    timeline_dir = f"{output_base}/problem2_timeline"
    summary_dir = f"{output_base}/problem2_cluster_summary"

    timeline_part = wait_for_file(f"{timeline_dir}/part-*.csv")
    summary_part = wait_for_file(f"{summary_dir}/part-*.csv")

    # ✅ 复制成固定名字的 CSV（保证存在）
    final_timeline_csv = f"{output_base}/problem2_timeline.csv"
    final_summary_csv = f"{output_base}/problem2_cluster_summary.csv"

    shutil.copy(timeline_part, final_timeline_csv)
    shutil.copy(summary_part, final_summary_csv)

    print(f"✅ CSV files saved:")
    print(f"   - {final_timeline_csv}")
    print(f"   - {final_summary_csv}")

    # ---------- 转 Pandas 方便画图 ----------
    apps_pd = apps_df.toPandas()
    summary_pd = cluster_summary.toPandas()

    spark.stop()

# ==========================================
# 📊 Visualization Section
# ==========================================
print("\nGenerating visualizations...")

# ---- 1️⃣ Bar chart ----
plt.figure(figsize=(8, 5))
sns.barplot(data=summary_pd, x="cluster_id", y="num_applications", palette="viridis")
plt.title("Number of Applications per Cluster")
plt.xlabel("Cluster ID")
plt.ylabel("Applications")
for i, v in enumerate(summary_pd["num_applications"]):
    plt.text(i, v + 0.5, str(v), ha='center', fontsize=9)
plt.tight_layout()
plt.savefig(f"{output_base}/problem2_bar_chart.png")
plt.close()

# ---- 2️⃣ Density plot ----
if not apps_pd.empty:
    apps_pd["start_time"] = pd.to_datetime(apps_pd["start_time"], errors='coerce')
    apps_pd["end_time"] = pd.to_datetime(apps_pd["end_time"], errors='coerce')
    apps_pd["duration_sec"] = (apps_pd["end_time"] - apps_pd["start_time"]).dt.total_seconds()

    top_cluster = apps_pd["cluster_id"].value_counts().idxmax()
    top_apps = apps_pd[apps_pd["cluster_id"] == top_cluster]

    plt.figure(figsize=(8, 5))
    sns.histplot(top_apps["duration_sec"].dropna(), kde=True, bins=30, log_scale=True)
    plt.title(f"Job Duration Distribution (Cluster {top_cluster})\n(n={len(top_apps)})")
    plt.xlabel("Duration (seconds, log scale)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(f"{output_base}/problem2_density_plot.png")
    plt.close()

print("✅ Problem 2 (Cluster) completed successfully!")
print(f"Outputs saved in: {output_base}/")
