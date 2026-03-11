from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder \
    .appName("github-trend-analysis") \
    .getOrCreate()

# Load raw GitHub archive data
df = spark.read.json("data/raw/*.json")

print("Loaded rows:", df.count())

# Extract useful fields
events = df.select(
    col("type"),
    col("repo.name").alias("repo"),
    col("created_at")
)

# Count events by repository
repo_event_counts = events.groupBy("repo").agg(
    count("*").alias("total_events")
)

# Push events
push_events = events.filter(col("type") == "PushEvent")

repo_push_counts = push_events.groupBy("repo").agg(
    count("*").alias("push_count")
)

# Pull request events
pr_events = events.filter(col("type") == "PullRequestEvent")

repo_pr_counts = pr_events.groupBy("repo").agg(
    count("*").alias("pr_count")
)

print("\nTop repositories by activity:")
repo_event_counts.orderBy(col("total_events").desc()).show(20, False)

print("\nTop repositories by pushes:")
repo_push_counts.orderBy(col("push_count").desc()).show(20, False)

print("\nTop repositories by PRs:")
repo_pr_counts.orderBy(col("pr_count").desc()).show(20, False)

spark.stop()