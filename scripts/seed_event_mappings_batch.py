from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.master_data import DEFAULT_SEED_CONFIG_VERSION_PREFIX, seed_event_mappings_from_catalog


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cria event_mappings padrao em lote para as configs seedadas."
    )
    parser.add_argument(
        "--store-root",
        default=None,
        help="Diretorio raiz do cadastro mestre interno. Padrao: data/company_master",
    )
    parser.add_argument(
        "--config-version-prefix",
        default=DEFAULT_SEED_CONFIG_VERSION_PREFIX,
        help="Prefixo das versoes seedadas que serao atualizadas. Padrao: seed-v1.",
    )
    parser.add_argument(
        "--report-output",
        default=None,
        help="Caminho opcional para gravar o resumo do seed em JSON.",
    )
    args = parser.parse_args()

    result = seed_event_mappings_from_catalog(
        store_root=args.store_root,
        config_version_prefix=args.config_version_prefix,
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
