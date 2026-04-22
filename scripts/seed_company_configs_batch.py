from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.master_data import DEFAULT_SEED_DEFAULT_PROCESS, seed_company_configs_from_missing_issues


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cria configs internas em lote para as pendencias company_config_missing."
    )
    parser.add_argument(
        "--store-root",
        default=None,
        help="Diretorio raiz do cadastro mestre interno. Padrao: data/company_master",
    )
    parser.add_argument(
        "--default-process",
        default=DEFAULT_SEED_DEFAULT_PROCESS,
        help="Codigo de processo padrao usado no seed. Padrao: 11.",
    )
    parser.add_argument(
        "--report-output",
        default=None,
        help="Caminho opcional para gravar o resumo do seed em JSON.",
    )
    args = parser.parse_args()

    result = seed_company_configs_from_missing_issues(
        store_root=args.store_root,
        default_process=args.default_process,
    )

    payload = result.model_dump(mode="json")
    print(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))

    if args.report_output:
        report_path = Path(args.report_output)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
