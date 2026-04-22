"""Internal company-config resolution for the dashboard workflow."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from config import CompanyConfig
from config.master_data import (
    DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT,
    DEFAULT_MASTER_DATA_ROOT,
    load_legacy_company_config_payload,
    legacy_active_config_path,
    legacy_specific_config_candidates,
    resolve_registry_config_payload,
)


DEFAULT_COMPANY_CONFIGS_ROOT = DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT
DEFAULT_COMPANY_MASTER_DATA_ROOT = DEFAULT_MASTER_DATA_ROOT


class ConfigResolutionStatus(StrEnum):
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    AMBIGUOUS = "AMBIGUOUS"
    MISMATCH = "MISMATCH"


@dataclass(frozen=True, slots=True)
class ConfigResolutionResult:
    status: ConfigResolutionStatus
    company_code: str
    competence: str
    config_source: str | None = None
    config_version: str | None = None
    source_path: Path | None = None
    config_payload: dict[str, Any] | None = None
    message: str = ""


class ConfigResolver:
    """Resolver = componente que decide automaticamente qual configuracao usar."""

    def __init__(
        self,
        root: str | Path | None = None,
        *,
        registry_root: str | Path | None = None,
        legacy_root: str | Path | None = None,
    ) -> None:
        # registry = cadastro mestre/base principal.
        self.registry_root = Path(registry_root) if registry_root is not None else DEFAULT_COMPANY_MASTER_DATA_ROOT
        self.legacy_root = (
            Path(legacy_root)
            if legacy_root is not None
            else (Path(root) if root is not None else DEFAULT_LEGACY_COMPANY_CONFIGS_ROOT)
        )

    def resolve(self, *, company_code: str, competence: str) -> ConfigResolutionResult:
        # competence = competencia/mes de referencia da folha.
        master_payload, master_source, master_path, master_message = resolve_registry_config_payload(
            company_code=company_code,
            competence=competence,
            registry_root=self.registry_root,
        )
        if master_source == ConfigResolutionStatus.AMBIGUOUS.value:
            return ConfigResolutionResult(
                status=ConfigResolutionStatus.AMBIGUOUS,
                company_code=company_code,
                competence=competence,
                source_path=master_path,
                message=master_message
                or "Mais de uma configuracao interna candidata foi encontrada para a empresa e a competencia detectadas.",
            )
        if master_source == ConfigResolutionStatus.MISMATCH.value:
            return ConfigResolutionResult(
                status=ConfigResolutionStatus.MISMATCH,
                company_code=company_code,
                competence=competence,
                source_path=master_path,
                message=master_message
                or "A configuracao interna encontrada esta invalida ou nao corresponde ao escopo esperado.",
            )
        if master_payload is not None:
            return ConfigResolutionResult(
                status=ConfigResolutionStatus.FOUND,
                company_code=company_code,
                competence=competence,
                config_source=master_source,
                config_version=str(master_payload["config_version"]),
                source_path=master_path,
                config_payload=master_payload,
                message=master_message or "Configuracao interna do cadastro mestre encontrada.",
            )

        legacy_payload, legacy_path, legacy_source = self._resolve_legacy_config(
            company_code=company_code,
            competence=competence,
        )
        if legacy_source == ConfigResolutionStatus.AMBIGUOUS.value:
            return ConfigResolutionResult(
                status=ConfigResolutionStatus.AMBIGUOUS,
                company_code=company_code,
                competence=competence,
                source_path=legacy_path,
                message=(
                    "Mais de uma configuracao legada candidata foi encontrada para a empresa e a competencia detectadas."
                ),
            )
        if legacy_source == ConfigResolutionStatus.MISMATCH.value:
            return ConfigResolutionResult(
                status=ConfigResolutionStatus.MISMATCH,
                company_code=company_code,
                competence=competence,
                source_path=legacy_path,
                message=(
                    "A configuracao legada encontrada esta invalida, inconsistente ou nao corresponde ao escopo esperado."
                ),
            )
        if legacy_payload is not None and legacy_path is not None:
            return ConfigResolutionResult(
                status=ConfigResolutionStatus.FOUND,
                company_code=company_code,
                competence=competence,
                config_source=legacy_source,
                config_version=str(legacy_payload["config_version"]),
                source_path=legacy_path,
                config_payload=legacy_payload,
                message="Configuracao interna legada aplicada como fallback.",
            )

        if master_message:
            return ConfigResolutionResult(
                status=ConfigResolutionStatus.NOT_FOUND,
                company_code=company_code,
                competence=competence,
                message=master_message,
            )

        return ConfigResolutionResult(
            status=ConfigResolutionStatus.NOT_FOUND,
            company_code=company_code,
            competence=competence,
            message=(
                "Nenhuma configuracao interna foi encontrada para a empresa detectada. "
                "O time interno precisa cadastrar a configuracao antes desta importacao."
            ),
        )

    def write_resolved_config(
        self,
        result: ConfigResolutionResult,
        *,
        target_path: str | Path,
    ) -> Path:
        if result.status != ConfigResolutionStatus.FOUND or result.config_payload is None:
            raise ValueError("Somente configuracoes resolvidas com status FOUND podem ser materializadas.")

        config = CompanyConfig.model_validate(result.config_payload)
        target = Path(target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(config.model_dump(mode="json"), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return target

    def _resolve_legacy_config(
        self,
        *,
        company_code: str,
        competence: str,
    ) -> tuple[dict[str, Any] | None, Path | None, str | None]:
        specific_candidates = tuple(
            path for path in legacy_specific_config_candidates(company_code, competence, legacy_root=self.legacy_root) if path.exists()
        )

        if len(specific_candidates) > 1:
            return None, specific_candidates[0], ConfigResolutionStatus.AMBIGUOUS.value

        if len(specific_candidates) == 1:
            legacy_path = specific_candidates[0]
            try:
                legacy_config, legacy_path = load_legacy_company_config_payload(
                    company_code,
                    competence,
                    legacy_root=self.legacy_root,
                )
            except Exception as exc:
                return None, legacy_path, ConfigResolutionStatus.MISMATCH.value

            if legacy_config is None or legacy_path is None:
                return None, legacy_path, ConfigResolutionStatus.NOT_FOUND.value
            if legacy_config.company_code != company_code or legacy_config.competence != competence:
                return None, legacy_path, ConfigResolutionStatus.MISMATCH.value

            payload = legacy_config.model_dump(mode="json")
            return payload, legacy_path, "legacy_company_competence"

        active_path = legacy_active_config_path(company_code, legacy_root=self.legacy_root)
        if active_path.exists():
            try:
                legacy_config, legacy_path = load_legacy_company_config_payload(
                    company_code,
                    competence,
                    legacy_root=self.legacy_root,
                )
            except Exception:
                legacy_path = active_path
                legacy_config = None

            if legacy_config is None:
                try:
                    raw_json = active_path.read_text(encoding="utf-8")
                    legacy_config = CompanyConfig.model_validate_json(raw_json)
                except Exception:
                    return None, active_path, ConfigResolutionStatus.MISMATCH.value

            if legacy_config.company_code != company_code:
                return None, active_path, ConfigResolutionStatus.MISMATCH.value

            payload = legacy_config.model_dump(mode="json")
            payload["competence"] = competence
            return payload, active_path, "legacy_company_active"

        return None, None, None
