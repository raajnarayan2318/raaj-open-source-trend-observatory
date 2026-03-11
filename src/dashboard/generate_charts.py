from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, lower
import plotly.express as px
import json
import os

spark = SparkSession.builder \
    .appName("github-ecosystem-intelligence") \
    .getOrCreate()

df = spark.read.json("data/raw/*.json")

events = df.select(
    col("type"),
    col("repo.name").alias("repo")
)

# ---------- CORE METRICS ----------

total_events = events.count()
active_repos = events.select("repo").distinct().count()

push_events = events.filter(col("type") == "PushEvent")
push_count = push_events.count()

pr_events = events.filter(col("type") == "PullRequestEvent")
pr_count = pr_events.count()

# ---------- REPOSITORY ACTIVITY ----------

repo_activity = events.groupBy("repo").agg(
    count("*").alias("total_events")
)

push_counts = push_events.groupBy("repo").agg(
    count("*").alias("push_count")
)

pr_counts = pr_events.groupBy("repo").agg(
    count("*").alias("pr_count")
)

joined = repo_activity.join(push_counts, "repo", "left") \
    .join(pr_counts, "repo", "left") \
    .fillna(0)

top_repos = joined.orderBy(desc("total_events")).limit(20).toPandas()

# ---------- FASTEST GROWING ----------

fastest = joined.orderBy(desc("push_count")).limit(20).toPandas()

# ---------- AI / ML REPOS ----------

ai_keywords = [
"ai","ml","machine-learning","deep-learning",
"transformer","llm","gpt","neural"
]

ai_repos = joined.filter(
    lower(col("repo")).rlike("|".join(ai_keywords))
).orderBy(desc("total_events")).limit(20).toPandas()

# ---------- DATA ENGINEERING ----------

data_keywords = [
"data","etl","pipeline","spark",
"warehouse","analytics","dbt"
]

data_repos = joined.filter(
    lower(col("repo")).rlike("|".join(data_keywords))
).orderBy(desc("total_events")).limit(20).toPandas()

# ---------- LANGUAGE APPROXIMATION ----------

language_map = {
"python": "Python",
"java": "Java",
"js": "JavaScript",
"react": "JavaScript",
"go": "Go",
"rust": "Rust",
"cpp": "C++"
}

lang_counts = {}

repos = joined.select("repo").limit(2000).toPandas()

for r in repos["repo"]:
    r = r.lower()

    for key,val in language_map.items():
        if key in r:
            lang_counts[val] = lang_counts.get(val,0)+1

lang_df = list(lang_counts.items())

# ---------- SAVE OUTPUT ----------

output_path = "dashboard_output"
os.makedirs(output_path, exist_ok=True)

dashboard_data = {
"metrics":{
"total_events": total_events,
"active_repos": active_repos,
"push_events": push_count,
"pull_requests": pr_count
},

"tables":{
"top_repositories": top_repos.to_dict(orient="records"),
"fastest_growth": fastest.to_dict(orient="records"),
"ai_repositories": ai_repos.to_dict(orient="records"),
"data_repositories": data_repos.to_dict(orient="records")
},

"languages": lang_df
}

with open(f"{output_path}/dashboard_data.json","w") as f:
    json.dump(dashboard_data,f)

# ---------- CHARTS ----------

fig1 = px.bar(
top_repos,
x="repo",
y="total_events",
template="plotly_dark",
title="Top Repositories"
)

fig2 = px.pie(
values=[push_count,pr_count],
names=["Push Events","Pull Requests"],
template="plotly_dark",
title="Event Distribution"
)

fig3 = px.scatter(
top_repos,
x="push_count",
y="pr_count",
size="total_events",
hover_name="repo",
template="plotly_dark",
title="Push vs PR Activity"
)

fig4 = px.bar(
ai_repos,
x="repo",
y="total_events",
template="plotly_dark",
title="Trending AI Repositories"
)

fig5 = px.bar(
data_repos,
x="repo",
y="total_events",
template="plotly_dark",
title="Trending Data Engineering Repositories"
)

if len(lang_df)>0:
    lang_names = [x[0] for x in lang_df]
    lang_values = [x[1] for x in lang_df]

    fig6 = px.pie(
        names=lang_names,
        values=lang_values,
        template="plotly_dark",
        title="Language Distribution"
    )

    fig6.write_html(f"{output_path}/language_distribution.html")

fig1.write_html(f"{output_path}/top_repositories.html")
fig2.write_html(f"{output_path}/event_distribution.html")
fig3.write_html(f"{output_path}/push_vs_pr.html")
fig4.write_html(f"{output_path}/ai_repos.html")
fig5.write_html(f"{output_path}/data_repos.html")

spark.stop()

print("Advanced GitHub ecosystem analytics generated")