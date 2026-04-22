"""Load versioned company configuration for deterministic mapping."""

from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError

from config import CompanyConfig

from .errors import MappingConfigurationError
from .taxonomy import MappingFatalCode, render_mapping_fatal_message


def load_company_config(path: str | Path) -> CompanyConfig:
    config_path = Path(path)

    try:
        raw_json = config_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise MappingConfigurationError(
            MappingFatalCode.INVALID_COMPANY_CONFIG,
            render_mapping_fatal_message(
                MappingFatalCode.INVALID_COMPANY_CONFIG,
                details=f"arquivo nao encontrado em {config_path}",
            ),
            source=str(config_path),
        ) from exc

    try:
        return CompanyConfig.model_validate_json(raw_json)
    except ValidationError as exc:
        raise MappingConfigurationError(
            MappingFatalCode.INVALID_COMPANY_CONFIG,
            render_mapping_fatal_message(
                MappingFatalCode.INVALID_COMPANY_CONFIG,
                details=exc,
            ),
            source=str(config_path),
        ) from exc

