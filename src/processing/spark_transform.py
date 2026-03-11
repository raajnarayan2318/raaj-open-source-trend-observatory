from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

spark = SparkSession.builder \
    .appName("github-trend-analysis") \
    .getOrCreate()

df = spark.read.json("data/raw/*.json")

events = df.select(
    col("type"),
    col("repo.name").alias("repo"),
    col("created_at")
)

# Total activity
repo_events = events.groupBy("repo").agg(
    count("*").alias("total_events")
)

# Push events
push_counts = events.filter(
    col("type") == "PushEvent"
).groupBy("repo").agg(
    count("*").alias("push_count")
)

# Pull request events
pr_counts = events.filter(
    col("type") == "PullRequestEvent"
).groupBy("repo").agg(
    count("*").alias("pr_count")
)

# Join metrics
metrics = repo_events.join(push_counts, "repo", "left") \
    .join(pr_counts, "repo", "left") \
    .fillna(0)

top_repos = metrics.orderBy(desc("total_events")).limit(20)

print("Top repositories:")
top_repos.show(20, False)

spark.stop()