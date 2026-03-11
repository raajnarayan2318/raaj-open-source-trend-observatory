import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder \
    .appName("github-trend-analysis") \
    .getOrCreate()

# Load GitHub Archive JSON
df = spark.read.json("data/raw/*.json")

events = df.select(
    col("type"),
    col("repo.name").alias("repo"),
    col("created_at")
)

repo_event_counts = events.groupBy("repo").agg(count("*").alias("total_events"))
push_counts = events.filter(col("type")=="PushEvent").groupBy("repo").agg(count("*").alias("push_count"))
pr_counts = events.filter(col("type")=="PullRequestEvent").groupBy("repo").agg(count("*").alias("pr_count"))

repo_event_counts = repo_event_counts.orderBy(col("total_events").desc()).limit(100)
push_counts = push_counts.orderBy(col("push_count").desc()).limit(100)
pr_counts = pr_counts.orderBy(col("pr_count").desc()).limit(100)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="github_trends",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Clear old data
cur.execute("DELETE FROM repo_event_counts")
cur.execute("DELETE FROM repo_push_counts")
cur.execute("DELETE FROM repo_pr_counts")

# Insert new analytics data
for row in repo_event_counts.collect():
    cur.execute(
        "INSERT INTO repo_event_counts VALUES (%s,%s)",
        (row["repo"], row["total_events"])
    )

for row in push_counts.collect():
    cur.execute(
        "INSERT INTO repo_push_counts VALUES (%s,%s)",
        (row["repo"], row["push_count"])
    )

for row in pr_counts.collect():
    cur.execute(
        "INSERT INTO repo_pr_counts VALUES (%s,%s)",
        (row["repo"], row["pr_count"])
    )

conn.commit()
cur.close()
conn.close()

spark.stop()

print("Data loaded into PostgreSQL successfully")