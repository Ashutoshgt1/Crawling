from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from distributed_crawler.shared.config import load_settings
from distributed_crawler.shared.repository import FrontierRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed URLs into the distributed frontier.")
    parser.add_argument("--input", required=True, help="Path to a text file of seed URLs.")
    parser.add_argument("--source-type", default="manual_seed", help="Source type label.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Seed file not found: {input_path}")

    seeds = [line.strip() for line in input_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    repository = FrontierRepository(load_settings())
    inserted = repository.seed_urls(seeds, source_type=args.source_type)
    print(f"[frontier] inserted={inserted} source_type={args.source_type}")


if __name__ == "__main__":
    main()
