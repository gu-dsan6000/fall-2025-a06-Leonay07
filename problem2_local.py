# from pyspark.sql import SparkSession
# from pyspark.sql.functions import regexp_extract, col, min as spark_min, max as spark_max, count as spark_count
# import os
# from pyspark.sql.functions import concat_ws


# # -------------------------------
# # Problem 2 (Local test version)
# # -------------------------------

# spark = (
#     SparkSession.builder
#     .appName("Problem2_ClusterUsage_Local")
#     .getOrCreate()
# )

# input_path = "data/sample/*"
# print(f"Loading sample logs from: {input_path}")

# logs_df = spark.read.text(input_path)

# # 提取字段
# pattern = r"application_(\d+)_(\d+)"
# time_pattern = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"

# parsed_df = logs_df.select(
#     regexp_extract("value", pattern, 1).alias("cluster_id"),
#     regexp_extract("value", pattern, 2).alias("app_number"),
#     regexp_extract("value", time_pattern, 1).alias("timestamp"),
#     col("value").alias("log_line")
# ).filter(col("cluster_id") != "")

# # 找到 start 和 end 时间
# apps_df = (
#     parsed_df.groupBy("cluster_id", "app_number")
#     .agg(
#         spark_min("timestamp").alias("start_time"),
#         spark_max("timestamp").alias("end_time")
#     )
#     .withColumn(
#         "application_id",
#         concat_ws("_", col("cluster_id").cast("string"), col("app_number").cast("string"))
#     )
# )

# # 汇总
# cluster_summary = (
#     apps_df.groupBy("cluster_id")
#     .agg(
#         spark_count("application_id").alias("num_applications"),
#         spark_min("start_time").alias("cluster_first_app"),
#         spark_max("end_time").alias("cluster_last_app")
#     )
# )

# # 总统计
# total_clusters = cluster_summary.count()
# total_apps = apps_df.count()
# avg_per_cluster = total_apps / total_clusters if total_clusters > 0 else 0

# summary_text = [
#     f"Total unique clusters: {total_clusters}",
#     f"Total applications: {total_apps}",
#     f"Average applications per cluster: {avg_per_cluster:.2f}",
# ]

# print("\n".join(summary_text))

# # 保存输出
# output_base = "data/output/problem2"
# os.makedirs(output_base, exist_ok=True)

# apps_df.write.csv(f"{output_base}/problem2_timeline.csv", header=True, mode="overwrite")
# cluster_summary.write.csv(f"{output_base}/problem2_cluster_summary.csv", header=True, mode="overwrite")

# with open(f"{output_base}/problem2_stats.txt", "w") as f:
#     f.write("\n".join(summary_text))

# print("\n✅ Problem 2 local test completed successfully.")
# print(f"Outputs saved in: {output_base}/")

# spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, min as spark_min, max as spark_max,
    count as spark_count, concat_ws
)
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# -------------------------------
# Problem 2 (Local Full Version)
# -------------------------------

spark = (
    SparkSession.builder
    .appName("Problem2_ClusterUsage_LocalFull")
    .getOrCreate()
)

# ✅ 读取路径
input_path = "data/sample/**/*.log"
print(f"Loading sample logs from: {input_path}")

logs_df = spark.read.text(input_path)

# 正则匹配字段
pattern = r"application_(\d+)_(\d+)"
time_pattern = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"

parsed_df = logs_df.select(
    regexp_extract("value", pattern, 1).alias("cluster_id"),
    regexp_extract("value", pattern, 2).alias("app_number"),
    regexp_extract("value", time_pattern, 1).alias("timestamp"),
    col("value").alias("log_line")
).filter(col("cluster_id") != "")

# 每个 application 的开始/
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

# 每个 cluster 的汇总
cluster_summary = (
    apps_df.groupBy("cluster_id")
    .agg(
        spark_count("application_id").alias("num_applications"),
        spark_min("start_time").alias("cluster_first_app"),
        spark_max("end_time").alias("cluster_last_app")
    )
)

# 汇总统计信息
total_clusters = cluster_summary.count()
total_apps = apps_df.count()
avg_per_cluster = total_apps / total_clusters if total_clusters > 0 else 0

summary_text = [
    f"Total unique clusters: {total_clusters}",
    f"Total applications: {total_apps}",
    f"Average applications per cluster: {avg_per_cluster:.2f}",
]
# --------------------------
# Find most heavily used clusters
# --------------------------
top_clusters = (
    cluster_summary
    .orderBy(col("num_applications").desc())
    .limit(5)
    .collect()
)

summary_text.append("\nMost heavily used clusters:")
for row in top_clusters:
    summary_text.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

print("\n".join(summary_text))

# ✅ 保存 Spark 输出
output_base = "data/output/problem2"
os.makedirs(output_base, exist_ok=True)

apps_path = f"{output_base}/problem2_timeline"
summary_path = f"{output_base}/problem2_cluster_summary"

apps_df.coalesce(1).write.csv(apps_path, header=True, mode="overwrite")
cluster_summary.coalesce(1).write.csv(summary_path, header=True, mode="overwrite")

with open(f"{output_base}/problem2_stats.txt", "w") as f:
    f.write("\n".join(summary_text))

# ==========================
# 📊 Visualization Section
# ==========================
print("\nGenerating visualizations...")

apps_pd = apps_df.toPandas()
summary_pd = cluster_summary.toPandas()

# ---- 1. Bar chart (applications per cluster) ----
plt.figure(figsize=(8, 5))
sns.barplot(
    data=summary_pd,
    x="cluster_id",
    y="num_applications",
    palette="viridis"
)
plt.title("Number of Applications per Cluster")
plt.xlabel("Cluster ID")
plt.ylabel("Applications")
for i, v in enumerate(summary_pd["num_applications"]):
    plt.text(i, v + 0.5, str(v), ha='center', fontsize=9)
plt.tight_layout()
plt.savefig(f"{output_base}/problem2_bar_chart.png")
plt.close()

# ---- 2. Density plot (application durations) ----
if not apps_pd.empty:
    # 转换时间格式并计算持续时长
    apps_pd["start_time"] = pd.to_datetime(apps_pd["start_time"], errors='coerce')
    apps_pd["end_time"] = pd.to_datetime(apps_pd["end_time"], errors='coerce')
    apps_pd["duration_sec"] = (apps_pd["end_time"] - apps_pd["start_time"]).dt.total_seconds()

    # 找出最大 cluster
    top_cluster = (
        apps_pd["cluster_id"]
        .value_counts()
        .idxmax()
    )
    top_apps = apps_pd[apps_pd["cluster_id"] == top_cluster]

    plt.figure(figsize=(8, 5))
    sns.histplot(top_apps["duration_sec"].dropna(), kde=True, bins=30, log_scale=True)
    plt.title(f"Job Duration Distribution (Cluster {top_cluster})\n(n={len(top_apps)})")
    plt.xlabel("Duration (seconds, log scale)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(f"{output_base}/problem2_density_plot.png")
    plt.close()

print("✅ Visualizations generated successfully!")
print(f"Outputs saved in: {output_base}/")

spark.stop()
