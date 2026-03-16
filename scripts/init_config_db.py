#!/usr/bin/env python3
import os
import json
import argparse

from sync import SyncDB


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="./config/sync_records.db")
    p.add_argument("--config", default="./config/quark_config.json")
    p.add_argument("--username", default="admin")
    args = p.parse_args()

    if not os.path.exists(args.config):
        raise SystemExit(f"配置文件不存在: {args.config}")

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    db = SyncDB(args.db)
    uid = db.import_config_dict(cfg)
    exported = db.export_config_dict(username=args.username) or {}
    db.set_app_config("quark_config", json.dumps(exported, ensure_ascii=False))

    accounts = exported.get("accounts") or []
    tasklist = exported.get("tasklist") or []
    sync_tasks = exported.get("sync_tasks") or []

    print("初始化完成")
    print(f"- db: {args.db}")
    print(f"- user_id: {uid}")
    print(f"- accounts: {len(accounts)}")
    print(f"- tasklist: {len(tasklist)}")
    print(f"- sync_tasks: {len(sync_tasks)}")


if __name__ == "__main__":
    main()
