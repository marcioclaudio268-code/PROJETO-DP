"""Map a persisted ingestion snapshot with a versioned company config."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mapping import map_snapshot_with_company_config


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read a canonical ingestion snapshot, apply company mapping and persist a mapped artifact."
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        required=True,
        help="Persisted ingestion snapshot JSON.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Versioned company configuration JSON.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional mapped artifact path. When omitted, it is derived from the snapshot path.",
    )
    args = parser.parse_args()

    artifacts = map_snapshot_with_company_config(
        args.snapshot,
        args.config,
        output_path=args.output,
    )
    print(
        " ".join(
            [
                f"movimentos_mapeados={len(artifacts.result.mapped_movements)}",
                f"pendencias_mapping={len(artifacts.result.pendings)}",
                f"saida={artifacts.output_path}",
            ]
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

