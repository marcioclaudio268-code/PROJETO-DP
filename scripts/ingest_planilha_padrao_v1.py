"""Ingest the V1 payroll template and update its technical tabs."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion import ingest_fill_and_persist_planilha_padrao_v1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read the V1 payroll template, populate technical tabs and persist ingestion artifacts."
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
    parser.add_argument(
        "--snapshot-output",
        type=Path,
        default=None,
        help="Optional snapshot JSON path. When omitted, it is derived from the workbook path.",
    )
    parser.add_argument(
        "--manifest-output",
        type=Path,
        default=None,
        help="Optional manifest JSON path. When omitted, it is derived from the workbook path.",
    )
    parser.add_argument(
        "--skip-manifest",
        action="store_true",
        help="Do not write the execution manifest.",
    )
    args = parser.parse_args()

    artifacts = ingest_fill_and_persist_planilha_padrao_v1(
        args.input,
        output_path=args.output,
        snapshot_path=args.snapshot_output,
        manifest_path=args.manifest_output,
        write_manifest_file=not args.skip_manifest,
    )
    print(
        " ".join(
            [
                f"movimentos={len(artifacts.result.movements)}",
                f"pendencias={len(artifacts.result.pendings)}",
                f"workbook={artifacts.workbook_path}",
                f"snapshot={artifacts.snapshot_path}",
                (
                    f"manifest={artifacts.manifest_path}"
                    if artifacts.manifest_path is not None
                    else "manifest=skipped"
                ),
            ]
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
