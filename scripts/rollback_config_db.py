#!/usr/bin/env python3
import os
import json
import argparse

from sync import SyncDB


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="./config/sync_records.db")
    p.add_argument("--out", default="./config/quark_config.rollback.json")
    p.add_argument("--username", default="admin")
    p.add_argument("--disable-relational", action="store_true")
    args = p.parse_args()

    db = SyncDB(args.db)
    cfg = db.export_config_dict(username=args.username)
    if not cfg:
        raise SystemExit("未找到可导出的配置（请确认 username）")

    out_dir = os.path.dirname(os.path.realpath(args.out))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    if args.disable_relational:
        uid = db.ensure_user(cfg.get("webui", {}).get("username") or args.username, cfg.get("webui", {}).get("password") or "")
        db.set_system_setting(uid, "config_storage_mode", "kv")
        db.set_app_config("quark_config", json.dumps(cfg, ensure_ascii=False))

    print("回滚导出完成")
    print(f"- db: {args.db}")
    print(f"- out: {args.out}")
    print(f"- disable_relational: {bool(args.disable_relational)}")


if __name__ == "__main__":
    main()
