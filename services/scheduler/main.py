from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from distributed_crawler.shared.config import load_settings
from distributed_crawler.shared.repository import FrontierRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect scheduler-ready leases and domain budgets.")
    parser.add_argument("--batch-size", type=int, default=None, help="Max URLs to lease.")
    parser.add_argument("--render-mode", default="http", choices=("http", "browser"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings()
    repository = FrontierRepository(settings)
    batch_size = args.batch_size or settings.scheduler_batch_size
    scheduled = repository.schedule_ready_urls(batch_size, settings.lease_seconds, render_mode=args.render_mode)
    if not scheduled.leases and not scheduled.promoted_to_browser and not scheduled.deferred:
        print("[scheduler] no ready URLs available")
        return

    print(
        f"[scheduler] selected={len(scheduled.leases)} deferred={len(scheduled.deferred)} "
        f"promoted_to_browser={len(scheduled.promoted_to_browser)} "
        f"render_mode={args.render_mode}"
    )


if __name__ == "__main__":
    main()
