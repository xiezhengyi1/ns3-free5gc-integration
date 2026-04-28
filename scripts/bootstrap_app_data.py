#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from pathlib import Path

import yaml


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upsert free5GC application PFD data from uerouting.yaml")
    parser.add_argument("--uerouting-file", required=True)
    parser.add_argument("--mongo-container", default="mongodb")
    parser.add_argument("--database", default="free5gc")
    args = parser.parse_args(argv)

    payload = yaml.safe_load(Path(args.uerouting_file).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("uerouting payload must be a mapping")
    app_data = payload.get("pfdDataForApp")
    if not isinstance(app_data, list) or not app_data:
        raise ValueError("uerouting payload does not define pfdDataForApp")

    statements = [
        f"db.getSiblingDB({json.dumps(args.database)}).getCollection('applicationData.pfds').updateOne("
        f"{{applicationId:{json.dumps(item['applicationId'])}}},"
        f"{{$set:{json.dumps(item, separators=(',', ':'))}}},"
        "{upsert:true});"
        for item in app_data
        if isinstance(item, dict) and item.get("applicationId")
    ]
    if not statements:
        raise ValueError("pfdDataForApp contains no applicationId")

    subprocess.run(
        [
            "docker",
            "exec",
            args.mongo_container,
            "mongo",
            "--quiet",
            "--eval",
            " ".join(statements),
        ],
        check=True,
    )
    print(f"upserted {len(statements)} application PFD records into {shlex.quote(args.mongo_container)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
