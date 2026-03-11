from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import plotly.express as px
import os

spark = SparkSession.builder \
    .appName("github-trend-analysis") \
    .getOrCreate()

# Load GitHub archive data
df = spark.read.json("data/raw/*.json")

events = df.select(
    col("type"),
    col("repo.name").alias("repo")
)

# Analytics
repo_event_counts = (
    events.groupBy("repo")
    .agg(count("*").alias("total_events"))
    .orderBy(col("total_events").desc())
    .limit(20)
)

push_counts = (
    events.filter(col("type") == "PushEvent")
    .groupBy("repo")
    .agg(count("*").alias("push_count"))
    .orderBy(col("push_count").desc())
    .limit(20)
)

pr_counts = (
    events.filter(col("type") == "PullRequestEvent")
    .groupBy("repo")
    .agg(count("*").alias("pr_count"))
    .orderBy(col("pr_count").desc())
    .limit(20)
)

# Convert to pandas for visualization
df1 = repo_event_counts.toPandas()
df2 = push_counts.toPandas()
df3 = pr_counts.toPandas()

# Create output directory
os.makedirs("dashboard_output", exist_ok=True)

fig1 = px.bar(df1, x="repo", y="total_events",
title="Top Trending GitHub Repositories")

fig2 = px.bar(df2, x="repo", y="push_count",
title="Repositories with Most Push Activity")

fig3 = px.bar(df3, x="repo", y="pr_count",
title="Repositories with Most Pull Requests")

fig1.write_html("dashboard_output/top_repositories.html")
fig2.write_html("dashboard_output/push_activity.html")
fig3.write_html("dashboard_output/pr_activity.html")

spark.stop()

print("Dashboards generated successfully")