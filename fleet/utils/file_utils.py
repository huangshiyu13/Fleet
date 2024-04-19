import json
import time
from pathlib import Path


def safe_load_json(file_path: Path, max_retry_times: int = 60):
    retry_time = 0
    while True:
        try:
            retry_time += 1
            data = json.loads(file_path.read_text())
            return data
        except Exception as e:
            if retry_time>3:
                print(f"Failed to load json file {file_path}, retry_time: {retry_time}")
            if retry_time >= max_retry_times:
                return None
            time.sleep(1)
