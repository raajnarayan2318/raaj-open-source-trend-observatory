from pathlib import Path
import psycopg2
import pandas as pd
import plotly.express as px

OUTPUT_DIR = Path("dashboard_output")
OUTPUT_DIR.mkdir(exist_ok=True)

conn = psycopg2.connect(
    dbname="github_trends",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

# -------- Top Repositories --------

query1 = """
SELECT repo, total_events
FROM repo_event_counts
ORDER BY total_events DESC
LIMIT 20
"""

df1 = pd.read_sql(query1, conn)

fig1 = px.bar(
    df1,
    x="repo",
    y="total_events",
    title="Top Trending GitHub Repositories"
)

fig1.write_html(OUTPUT_DIR / "top_repositories.html")


# -------- Push Activity --------

query2 = """
SELECT repo, push_count
FROM repo_push_counts
ORDER BY push_count DESC
LIMIT 20
"""

df2 = pd.read_sql(query2, conn)

fig2 = px.bar(
    df2,
    x="repo",
    y="push_count",
    title="Repositories with Most Push Activity"
)

fig2.write_html(OUTPUT_DIR / "push_activity.html")


# -------- Pull Request Activity --------

query3 = """
SELECT repo, pr_count
FROM repo_pr_counts
ORDER BY pr_count DESC
LIMIT 20
"""

df3 = pd.read_sql(query3, conn)

fig3 = px.bar(
    df3,
    x="repo",
    y="pr_count",
    title="Repositories with Most Pull Requests"
)

fig3.write_html(OUTPUT_DIR / "pr_activity.html")


conn.close()

print("All dashboards generated successfully")