"""Phase 5: run the mock Firco and the analysts for one business date, or all of them, oldest first."""
import argparse
import sys
from datetime import date

from src.common import oracle
from src.firco import analysts, engine

FIRCO_USER = "firco_app"


def run(day, con):
    done = engine.run_day(con, day)
    decided = analysts.review(con, day)
    print(f"{day}  {done['batch']}  list={done['list']}{' (new)' if done['new_list'] else ''}  "
          f"alerts={done['alerts']} rescreen_alerts={done['rescreen_alerts']}  "
          f"L1 cleared={decided['cleared']} escalated={decided['escalated']}  L2 confirmed={decided['confirmed']}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", type=date.fromisoformat, help="business date, e.g. 2026-10-01")
    group.add_argument("--all", action="store_true", help="every day with a screening extract, oldest first")
    args = parser.parse_args()
    days = [args.date] if args.date else sorted(date.fromisoformat(p.name) for p in engine.OUTBOUND.iterdir())
    with oracle.connect(FIRCO_USER) as con:
        for day in days:
            run(day, con)
    return 0


if __name__ == "__main__":
    sys.exit(main())