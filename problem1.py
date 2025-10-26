from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand
import os, glob, shutil

# ===============================
#  Problem 1: Log Level Distribution (Cluster - Local Output)
# ===============================

spark = (
    SparkSession.builder
    .appName("Problem1_LogLevelDistribution_LocalOutput")
    .getOrCreate()
)

# Read log data
bucket = os.environ.get("SPARK_LOGS_BUCKET")
if not bucket:
    raise ValueError("SPARK_LOGS_BUCKET environment variable not set!")

input_path = f"{bucket}/data/application_*/*.log"
print(f"Loading full dataset from: {input_path}")
logs_df = spark.read.text(input_path)

# Extract log levels
parsed_df = logs_df.select(
    col("value").alias("log_entry"),
    regexp_extract("value", r"(INFO|WARN|ERROR|DEBUG)", 1).alias("log_level")
)

# Count log level distribution
counts_df = (
    parsed_df.filter(col("log_level") != "")
    .groupBy("log_level")
    .count()
    .orderBy(col("count").desc())
)

# Randomly sample 10 log entries
sample_df = (
    parsed_df.filter(col("log_level") != "")
    .orderBy(rand())
    .limit(10)
)

# Calculate summary statistics
total_lines = logs_df.count()
valid_lines = parsed_df.filter(col("log_level") != "").count()
invalid_lines = total_lines - valid_lines
counts = counts_df.collect()

summary_lines = [
    f"Total log lines processed: {total_lines}",
    f"Total lines with log levels: {valid_lines}",
    f"Lines without log levels: {invalid_lines}",
    f"Unique log levels found: {len(counts)}",
    "",
    "Log level distribution:"
]
for row in counts:
    pct = (row['count'] / valid_lines) * 100 if valid_lines > 0 else 0
    summary_lines.append(f"  {row['log_level']:<6}: {row['count']:>10,} ({pct:6.2f}%)")

# Define output directory
output_base = "/home/ubuntu/problem1_output"
os.makedirs(output_base, exist_ok=True)

counts_dir = f"{output_base}/problem1_counts_tmp"
sample_dir = f"{output_base}/problem1_sample_tmp"
summary_path = f"{output_base}/problem1_summary.txt"

# Write CSV outputs
counts_df.coalesce(1).write.csv(counts_dir, header=True, mode="overwrite")
sample_df.coalesce(1).write.csv(sample_dir, header=True, mode="overwrite")

# Write summary text file
with open(summary_path, "w") as f:
    f.write("\n".join(summary_lines))

# Flatten Spark output directories
def flatten_spark_output(dir_path, final_path):
    part_file = glob.glob(f"{dir_path}/part-*.csv")
    if part_file:
        shutil.move(part_file[0], final_path)
        shutil.rmtree(dir_path, ignore_errors=True)
        print(f"Output saved as: {final_path}")
    else:
        print(f"No CSV part file found in {dir_path}")

flatten_spark_output(counts_dir, f"{output_base}/problem1_counts.csv")
flatten_spark_output(sample_dir, f"{output_base}/problem1_sample.csv")

print("\nProblem 1 (Cluster, Local Output) completed successfully!")
print(f"Outputs saved to: {output_base}/")

spark.stop()
