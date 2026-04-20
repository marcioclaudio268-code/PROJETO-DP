"""Ingest the V1 payroll template and update its technical tabs."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion import ingest_and_fill_planilha_padrao_v1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read the V1 payroll template and populate the technical tabs."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data") / "templates" / "planilha_padrao_folha_v1.xlsx",
        help="Workbook to ingest.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output path. When omitted, the input workbook is updated in place.",
    )
    args = parser.parse_args()

    result = ingest_and_fill_planilha_padrao_v1(args.input, output_path=args.output)
    print(f"movimentos={len(result.movements)} pendencias={len(result.pendings)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
