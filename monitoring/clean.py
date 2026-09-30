"""Reset the lab to a clean state (like a fresh clone).

  python -m monitoring.clean            delete output/ (all run results)
  python -m monitoring.clean --kafka    also delete the topics/groups created by the scripts
  python -m monitoring.clean --hard     also wipe ALL Kafka data (docker compose down -v)
  add --yes to skip the confirmation question
"""
import argparse
import shutil
import subprocess
import sys

from confluent_kafka.admin import AdminClient
from config.settings import KAFKA_COMMON, ROOT

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Topics created by run_scenario, demo_routing and demo_rebalance (the manual 'sensor-data' topic is kept unless --hard).
OWN_PREFIXES = ("bench-", "routing-", "sensor-data-")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kafka", action="store_true", help="delete benchmark/demo topics and their consumer groups")
    parser.add_argument("--hard", action="store_true", help="wipe all Kafka data: docker compose down -v")
    parser.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    args = parser.parse_args()

    out = ROOT / "output"
    plan = [f"delete {out}" + ("" if out.exists() else " (does not exist)")]
    if args.hard:
        plan.append("docker compose down -v  (removes the Kafka container AND all topics/data)")
    elif args.kafka:
        plan.append(f"delete Kafka topics starting with {OWN_PREFIXES} and all consumer groups")
    print("This will:\n  - " + "\n  - ".join(plan))
    if not args.yes and input("Continue? [y/N] ").strip().lower() != "y":
        print("Cancelled.")
        return 1

    if out.exists():
        shutil.rmtree(out)
        print(f"Deleted {out}")

    if args.hard:
        subprocess.run(["docker", "compose", "down", "-v"], cwd=ROOT, check=False)
        print("Kafka data wiped. Start again with: docker compose up -d")
    elif args.kafka:
        admin = AdminClient({**KAFKA_COMMON})
        try:
            topics = [t for t in admin.list_topics(timeout=10).topics if t.startswith(OWN_PREFIXES)]
            for name, fut in admin.delete_topics(topics).items() if topics else []:
                fut.result(30)
                print(f"Deleted topic {name}")
            groups = [g.group_id for g in admin.list_consumer_groups().result(30).valid]
            for gid, fut in admin.delete_consumer_groups(groups).items() if groups else []:
                try:
                    fut.result(30)
                    print(f"Deleted group {gid}")
                except Exception as exc:
                    print(f"Could not delete group {gid}: {exc}")
            print(f"Done: {len(topics)} topic(s), {len(groups)} group(s).")
        except Exception as exc:
            print(f"Kafka cleanup failed (is the broker running?): {exc}")
            return 1
    print("Clean.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
