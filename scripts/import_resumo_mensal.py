from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.master_data import import_resumo_mensal_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa a planilha Resumo Mensal para o cadastro mestre interno.")
    parser.add_argument("--input", required=True, help="Caminho local para a planilha Resumo Mensal.xls ou .xlsx")
    parser.add_argument(
        "--store-root",
        default=None,
        help="Diretorio raiz do cadastro mestre interno. Padrao: data/company_master",
    )
    parser.add_argument(
        "--legacy-root",
        default=None,
        help="Diretorio com as configuracoes legadas do dashboard. Padrao: configs/companies",
    )
    parser.add_argument(
        "--report-output",
        default=None,
        help="Caminho opcional para gravar o resumo da importacao em JSON.",
    )
    args = parser.parse_args()

    result = import_resumo_mensal_file(
        args.input,
        store_root=args.store_root,
        legacy_configs_root=args.legacy_root,
    )

    payload = result.model_dump(mode="json")
    print(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))

    if args.report_output:
        report_path = Path(args.report_output)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
