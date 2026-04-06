from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main() -> None:
    print(
        "[browser-fetch] placeholder worker. Production intent is Playwright-only for promoted URLs "
        "after scheduler escalation based on domain policy, JS dependence, or challenge rate."
    )


if __name__ == "__main__":
    main()
