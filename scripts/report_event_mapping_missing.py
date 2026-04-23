"""Report event_mapping warnings observed in the seeded company cohort."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import replace
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config import CompanyConfig
from config.master_data import CompanyMasterDataStore, DEFAULT_MASTER_DATA_ROOT
from ingestion import load_planilha_padrao_folha_v1, serialize_ingestion_result
from ingestion.template_v1_loader import EVENT_SPECS
from mapping import build_snapshot_summary, map_ingestion_result


REFERENCE_CONFIG_PATHS = (
    ROOT / "data" / "golden" / "v1" / "happy_path" / "company_config.json",
    ROOT / "configs" / "companies" / "72" / "active.json",
)

EVENT_SPEC_INDEX = {spec.column_name: spec for spec in EVENT_SPECS}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay the seeded cohort and summarize mapeamento_evento_ausente warnings."
    )
    parser.add_argument(
        "--store-root",
        type=Path,
        default=DEFAULT_MASTER_DATA_ROOT,
        help="Internal company master root. Default: data/company_master",
    )
    parser.add_argument(
        "--base-workbook",
        type=Path,
        default=ROOT / "data" / "golden" / "v1" / "happy_path" / "input.xlsx",
        help="Workbook used as the replay base. Default: golden happy_path input.xlsx",
    )
    parser.add_argument(
        "--control-company-code",
        default="72",
        help="Control company code used to validate the standard config baseline.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional JSON report output path.",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        default=None,
        help="Optional Markdown report output path.",
    )
    args = parser.parse_args()

    report = build_report(
        store_root=args.store_root,
        base_workbook=args.base_workbook,
        control_company_code=args.control_company_code,
    )
    markdown = render_markdown(report)

    print(markdown)

    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    if args.markdown_output is not None:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(markdown + "\n", encoding="utf-8")

    return 0


def build_report(
    *,
    store_root: str | Path,
    base_workbook: str | Path,
    control_company_code: str,
) -> dict[str, Any]:
    store = CompanyMasterDataStore(store_root)
    registry_entries = list(store.load_registry_entries())
    config_records = {record.id: record for record in store.load_config_records()}

    cohort = []
    for company in registry_entries:
        active_id = company.active_config_id
        if not active_id:
            continue
        config_record = config_records.get(active_id)
        if config_record is None:
            continue
        if not str(config_record.version).startswith("seed-v1-"):
            continue
        cohort.append((company, config_record))

    base_result = load_planilha_padrao_folha_v1(base_workbook)
    control_summary = _build_control_summary(
        base_result=base_result,
        registry_entries=registry_entries,
        config_records=config_records,
        control_company_code=control_company_code,
    )

    observations: list[dict[str, Any]] = []
    company_missing_counts: Counter[str] = Counter()
    competence_missing_counts: Counter[str] = Counter()

    for company, config_record in cohort:
        sample_result = _replay_result_for_company(
            base_result=base_result,
            company_code=company.company_code,
            company_name=company.razao_social or company.nome_fantasia or company.company_code,
            competence=company.last_competence_seen or base_result.parameters.competence,
        )
        company_config = CompanyConfig.model_validate(config_record.config_payload_internal)
        snapshot_summary = build_snapshot_summary(
            serialize_ingestion_result(sample_result, engine_version="0.1.0")
        )
        mapping_result = map_ingestion_result(
            sample_result,
            company_config,
            snapshot_summary=snapshot_summary,
        )

        missing = [
            pending
            for pending in mapping_result.pendings
            if pending.pending_code == "mapeamento_evento_ausente"
        ]
        company_missing_counts[company.company_code] += len(missing)
        competence_missing_counts[company.last_competence_seen or base_result.parameters.competence] += len(missing)

        for pending in missing:
            observations.append(
                {
                    "company_code": company.company_code,
                    "company_name": company.razao_social or company.nome_fantasia or company.company_code,
                    "competence": company.last_competence_seen or base_result.parameters.competence,
                    "event_name": pending.event_name or "<sem_evento>",
                    "source_sheet": pending.source.sheet_name,
                    "source_row": pending.source.row_number,
                    "source_cell": pending.source.cell,
                }
            )

    reference_catalog = _load_reference_catalog()
    report = _build_report(
        observations=observations,
        cohort_size=len(cohort),
        company_missing_counts=company_missing_counts,
        competence_missing_counts=competence_missing_counts,
        control_summary=control_summary,
        reference_catalog=reference_catalog,
    )
    return report


def _replay_result_for_company(
    *,
    base_result,
    company_code: str,
    company_name: str,
    competence: str,
):
    parameters = replace(
        base_result.parameters,
        company_code=company_code,
        company_name=company_name,
        competence=competence,
    )
    movements = tuple(
        replace(
            movement,
            company_code=company_code,
            competence=competence,
        )
        for movement in base_result.movements
    )
    return replace(base_result, parameters=parameters, movements=movements)


def _load_reference_catalog() -> dict[str, str]:
    for path in REFERENCE_CONFIG_PATHS:
        if path.exists():
            config = CompanyConfig.model_validate_json(path.read_text(encoding="utf-8"))
            return {mapping.event_negocio: mapping.rubrica_saida for mapping in config.event_mappings}
    return {}


def _build_control_summary(
    *,
    base_result,
    registry_entries,
    config_records,
    control_company_code: str,
) -> dict[str, Any]:
    control_company = next(
        (company for company in registry_entries if company.company_code == control_company_code),
        None,
    )
    if control_company is None:
        return {
            "company_code": control_company_code,
            "available": False,
            "missing_occurrences": None,
            "message": "Control company not found in registry.",
        }

    control_record = config_records.get(control_company.active_config_id or "")
    if control_record is None:
        return {
            "company_code": control_company_code,
            "available": False,
            "missing_occurrences": None,
            "message": "Control config not found in master data.",
        }

    control_config = CompanyConfig.model_validate(control_record.config_payload_internal)
    snapshot_summary = build_snapshot_summary(
        serialize_ingestion_result(base_result, engine_version="0.1.0")
    )
    mapping_result = map_ingestion_result(
        base_result,
        control_config,
        snapshot_summary=snapshot_summary,
    )
    missing = [
        pending
        for pending in mapping_result.pendings
        if pending.pending_code == "mapeamento_evento_ausente"
    ]
    return {
        "company_code": control_company_code,
        "company_name": control_company.razao_social or control_company.nome_fantasia or control_company.company_code,
        "config_version": control_record.version,
        "missing_occurrences": len(missing),
        "resolved_status": "FOUND" if not missing else "HAS_MISSING",
    }


def _build_report(
    *,
    observations: list[dict[str, Any]],
    cohort_size: int,
    company_missing_counts: Counter[str],
    competence_missing_counts: Counter[str],
    control_summary: dict[str, Any],
    reference_catalog: dict[str, str],
) -> dict[str, Any]:
    event_counts: Counter[str] = Counter(obs["event_name"] for obs in observations)
    by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for obs in observations:
        by_event[obs["event_name"]].append(obs)

    event_summaries = []
    for event_name, items in sorted(by_event.items(), key=lambda item: (-len(item[1]), item[0])):
        companies = sorted(
            {f"{item['company_code']} - {item['company_name']}" for item in items},
            key=lambda value: int(value.split(" - ", 1)[0]),
        )
        competences = Counter(item["competence"] for item in items)
        spec = EVENT_SPEC_INDEX.get(event_name)
        ref_rubrica = reference_catalog.get(event_name)
        catalog_match = ref_rubrica is not None
        group_name = _group_name_for_event(event_name, spec, catalog_match)
        event_summaries.append(
            {
                "event_name": event_name,
                "occurrences": len(items),
                "companies_impacted": len(companies),
                "competence_counts": dict(sorted(competences.items())),
                "sample_companies": companies[:5],
                "sample_cells": sorted({item["source_cell"] for item in items}),
                "template_value_type": spec.value_type.value if spec and spec.value_type else None,
                "template_allows_automatic_movement": spec.allows_automatic_movement if spec else None,
                "catalog_match": catalog_match,
                "reference_rubrica_saida": ref_rubrica,
                "classification": "catalogo_padrao" if catalog_match else "excecao_real",
                "group_name": group_name,
                "probable_resolution": (
                    "seed_event_mappings_em_lote_a_partir_do_catalogo_padrao"
                    if catalog_match
                    else "override_guiado_ou_analise_manual"
                ),
            }
        )

    groups: dict[str, dict[str, Any]] = defaultdict(lambda: {"events": [], "occurrences": 0, "companies_impacted": 0})
    for event_summary in event_summaries:
        group = groups[event_summary["group_name"]]
        group["events"].append(event_summary["event_name"])
        group["occurrences"] += event_summary["occurrences"]
        group["companies_impacted"] += event_summary["companies_impacted"]

    group_summaries = [
        {
            "group_name": group_name,
            "events": tuple(sorted(data["events"])),
            "occurrences": data["occurrences"],
            "companies_impacted": data["companies_impacted"],
        }
        for group_name, data in sorted(groups.items(), key=lambda item: item[0])
    ]

    company_frequency_mode = max(company_missing_counts.values()) if company_missing_counts else 0
    companies_at_mode = sum(1 for count in company_missing_counts.values() if count == company_frequency_mode)
    companies_with_missing = sum(1 for count in company_missing_counts.values() if count > 0)
    total_occurrences = sum(event_counts.values())
    missing_by_competence = dict(sorted(competence_missing_counts.items()))
    control_missing = control_summary.get("missing_occurrences")

    return {
        "scope": {
            "companies_audited": cohort_size,
            "companies_with_missing": companies_with_missing,
            "total_missing_occurrences": total_occurrences,
            "company_frequency_mode": company_frequency_mode,
            "companies_at_mode": companies_at_mode,
            "competence_counts": missing_by_competence,
            "control_summary": control_summary,
        },
        "event_ranking": event_summaries,
        "group_summaries": group_summaries,
        "reference_catalog": reference_catalog,
        "company_frequency_examples": sorted(
            [f"{code}: {count}" for code, count in company_missing_counts.items() if count > 0],
            key=lambda item: int(item.split(":", 1)[0]),
        )[:10],
        "exceptions": [],
        "decision": {
            "next_phase": "seed_catalogo_padrao_event_mappings",
            "recommended_order": (
                "catalogo_padrao em lote -> validacao controlada -> override guiado apenas se surgir divergencia real"
            ),
            "override_needed": False,
        },
    }


def _group_name_for_event(
    event_name: str,
    spec,
    catalog_match: bool,
) -> str:
    if not catalog_match:
        return "excecao_real"
    if spec is None or spec.value_type is None:
        return "catalogo_padrao_indefinido"
    return f"catalogo_padrao_{spec.value_type.value}"


def render_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    scope = report["scope"]
    lines.append("# Relatorio de mapeamento_evento_ausente")
    lines.append("")
    lines.append(f"- Empresas auditadas: {scope['companies_audited']}")
    lines.append(f"- Empresas com warning: {scope['companies_with_missing']}")
    lines.append(f"- Ocorrencias totais: {scope['total_missing_occurrences']}")
    lines.append(f"- Frequencia por empresa: {scope['company_frequency_mode']} por empresa em {scope['companies_at_mode']} empresas")
    lines.append(f"- Control baseline {scope['control_summary'].get('company_code')}: {scope['control_summary'].get('missing_occurrences', 0)} ocorrencias")
    lines.append("")
    lines.append("## Ranking")
    lines.append("")
    lines.append("| Evento | Ocorrencias | Empresas | Competencias | Tipo template | Catalogo | Rubrica ref. | Caminho |")
    lines.append("| --- | ---: | ---: | --- | --- | --- | --- | --- |")
    for event in report["event_ranking"]:
        competence_text = ", ".join(f"{k}: {v}" for k, v in event["competence_counts"].items())
        lines.append(
            "| {event_name} | {occurrences} | {companies_impacted} | {competence_text} | {template_value_type} | {catalog_match} | {reference_rubrica_saida} | {probable_resolution} |".format(
                event_name=event["event_name"],
                occurrences=event["occurrences"],
                companies_impacted=event["companies_impacted"],
                competence_text=competence_text,
                template_value_type=event["template_value_type"] or "-",
                catalog_match="sim" if event["catalog_match"] else "nao",
                reference_rubrica_saida=event["reference_rubrica_saida"] or "-",
                probable_resolution=event["probable_resolution"],
            )
        )
    lines.append("")
    lines.append("## Agrupamentos")
    lines.append("")
    for group in report["group_summaries"]:
        lines.append(
            f"- {group['group_name']}: {', '.join(group['events'])} "
            f"({group['occurrences']} ocorrencias, {group['companies_impacted']} empresas)"
        )
    lines.append("")
    lines.append("## Excecoes")
    lines.append("")
    if report["exceptions"]:
        for item in report["exceptions"]:
            lines.append(f"- {item}")
    else:
        lines.append("- Nenhuma observada.")
    lines.append("")
    lines.append("## Decisao")
    lines.append("")
    lines.append(f"- Proxima fase: {report['decision']['next_phase']}")
    lines.append(f"- Ordem recomendada: {report['decision']['recommended_order']}")
    lines.append(f"- Override necessario agora: {'sim' if report['decision']['override_needed'] else 'nao'}")
    lines.append("")
    lines.append("## Exemplos")
    lines.append("")
    for event in report["event_ranking"]:
        examples = "; ".join(event["sample_companies"])
        cells = ", ".join(event["sample_cells"])
        lines.append(f"- {event['event_name']}: {examples} | celulas: {cells}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
