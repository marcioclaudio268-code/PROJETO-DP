"""Generate the V1 Excel template for human payroll entry."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion import TEMPLATE_V1_FILENAME, save_planilha_padrao_folha_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate the V1 payroll Excel template.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data") / "templates" / TEMPLATE_V1_FILENAME,
        help="Destination .xlsx path.",
    )
    args = parser.parse_args()

    output_path = save_planilha_padrao_folha_v1(args.output)
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
