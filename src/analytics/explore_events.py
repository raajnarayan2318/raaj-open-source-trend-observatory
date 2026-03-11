import json
import glob

files = glob.glob("data/raw/*.json")

print("Files found:", files)

file = files[0]
print("Opening:", file)

count = 0

with open(file) as f:
    for line in f:
        event = json.loads(line)
        print("type:", event.get("type"))
        print("repo:", event.get("repo", {}).get("name"))
        print("created_at:", event.get("created_at"))
        print("-" * 50)

        count += 1
        if count == 5:
            break