import subprocess

print("Running ingestion...")
subprocess.run(["python", "src/ingest/fetch_github_archive.py"], check=True)

print("Running Spark transformation...")
subprocess.run(["python", "src/processing/spark_transform.py"], check=True)

print("Generating dashboards...")
subprocess.run(["python", "src/dashboard/generate_charts.py"], check=True)

print("Pipeline completed successfully")