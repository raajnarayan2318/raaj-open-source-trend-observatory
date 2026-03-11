import requests
import gzip
import shutil
import os
from datetime import datetime

BASE_URL = "https://data.gharchive.org"

OUTPUT_DIR = "data/raw"

os.makedirs(OUTPUT_DIR, exist_ok=True)

file_name = "2024-03-01-15.json.gz"

url = f"{BASE_URL}/{file_name}"

print("Downloading:", url)

response = requests.get(url, stream=True)

if response.status_code == 200:

    gz_path = os.path.join(OUTPUT_DIR, file_name)

    with open(gz_path, "wb") as f:
        f.write(response.content)

    json_path = gz_path.replace(".gz", "")

    with gzip.open(gz_path, "rb") as f_in:
        with open(json_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    print("Saved:", json_path)

else:
    print("File not available yet.")