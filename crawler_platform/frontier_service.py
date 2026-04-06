from __future__ import annotations

import argparse
from pathlib import Path

from crawler_platform.config import load_settings
from crawler_platform.storage import PostgresStorage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed URLs into the crawler frontier.")
    parser.add_argument("--input", required=True, help="Path to a text file containing seed URLs.")
    parser.add_argument(
        "--source-type",
        default="manual_seed",
        help="Source label stored with inserted URLs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Seed file not found: {input_path}")
    seeds = [line.strip() for line in input_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    storage = PostgresStorage(load_settings())
    inserted = storage.seed_urls(seeds, source_type=args.source_type)
    print(f"Seeded {inserted} new URLs into the frontier.")


if __name__ == "__main__":
    main()
