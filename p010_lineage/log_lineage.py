import json
import os
from datetime import datetime

LINEAGE_FILE = "p010_lineage/lineage_log.json"

def log_pipeline_run(stage, input_files, output_files):
    os.makedirs("p010_lineage", exist_ok=True)

    if not os.path.exists(LINEAGE_FILE):
        with open(LINEAGE_FILE, "w") as f:
            json.dump([], f)

    with open(LINEAGE_FILE, "r") as f:
        data = json.load(f)

    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stage": stage,
        "input_files": input_files,
        "output_files": output_files
    }

    data.append(entry)

    with open(LINEAGE_FILE, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Lineage logged for stage: {stage}")
