from __future__ import annotations

import streamlit as st

from dashboard import (
    ColumnMappingProfileError,
    DashboardActionType,
    apply_dashboard_action,
    apply_workbook_cell_correction,
    analyze_report_import,
    apply_report_employee_suggestions,
    apply_report_rubric_suggestions,
    build_dashboard_paths,
    create_dashboard_run_from_uploads,
    get_company_admin_entry,
    list_active_employees,
    list_active_rubrics,
    list_company_admin_entries,
    load_column_mapping_profile,
    load_company_employee_registry,
    load_company_rubric_catalog,
    load_dashboard_run,
    run_dashboard_analysis,
    save_column_mapping_profile_rule,
    save_company_admin_entry,
    save_employee_registry_record,
    save_rubric_catalog_record,
)
from dashboard.txt_audit import build_txt_audit


RUN_ROOT_KEY = "dashboard_v1_run_root"
ERROR_KEY = "dashboard_v1_last_error"
ASSISTED_IMPORT_RESULT_KEY = "dashboard_v1_assisted_import_result"
RUBRIC_EDIT_INDEX_KEY = "dashboard_v1_rubric_edit_index"
COLUMN_MAPPING_PROFILE_CODES = {
    "column_mapping_profile_missing",
    "column_mapping_profile_incomplete",
}
VALUE_KIND_OPTIONS = ("monetario", "horas", "quantidade")
GENERATION_MODE_OPTIONS = ("single_line", "multi_line", "ignore")
RUBRIC_NATURE_OPTIONS = ("unknown", "provento", "desconto", "informativo")
RECORD_STATUS_OPTIONS = ("active", "inactive", "unknown")
TAB_LABELS = (
    "Importar planilha",
    "Importador assistido de relatorios",
    "Cadastro da empresa",
    "Funcionarios",
    "Rubricas",
    "Perfil de colunas",
    "Auditoria TXT",
)


def main() -> None:
    st.set_page_config(
        page_title="Dashboard operacional V1",
        layout="wide",
    )

    st.title("Dashboard operacional do motor TXT V1")
    st.caption(
        "Use esta tela para importar a planilha, revisar pendencias, aplicar correcoes guiadas e baixar o TXT apenas quando ele estiver liberado."
    )

    result = _load_current_result()
    tabs = st.tabs(TAB_LABELS)
    with tabs[0]:
        _render_import_tab(result)
    with tabs[1]:
        _render_assisted_report_importer_tab()
    with tabs[2]:
        _render_company_registration_tab()
    with tabs[3]:
        _render_employees_tab()
    with tabs[4]:
        _render_rubrics_tab()
    with tabs[5]:
        _render_column_profile_tab()
    with tabs[6]:
        _render_txt_audit_tab(result)


def _render_last_error() -> None:
    last_error = st.session_state.get(ERROR_KEY)
    if last_error:
        st.error(last_error)


def _load_current_result():
    run_root = st.session_state.get(RUN_ROOT_KEY)
    if not run_root:
        return None

    paths = build_dashboard_paths(run_root)
    if not paths.state_path.exists():
        return None

    try:
        return load_dashboard_run(paths)
    except Exception as exc:  # pragma: no cover - fallback visual only
        st.error(f"Nao foi possivel carregar a ultima analise: {exc}")
        return None


def _render_import_tab(result) -> None:
    _render_upload_area()
    _render_last_error()
    if result is None:
        return
    _render_summary(result)
    _render_pendings(result)
    _render_actions_history(result)
    _render_downloads(result)


def _render_upload_area() -> None:
    st.subheader("Importar planilha")
    st.write(
        "Selecione a empresa antes de enviar a planilha. A deteccao automatica continua sendo usada como conferencia."
    )

    selected_company = _render_company_selector(
        "Empresa para esta importacao",
        key="empresa-importacao",
    )
    selected_competence = st.text_input(
        "Competencia informada (opcional)",
        value="",
    )

    uploaded_workbook = st.file_uploader(
        "Planilha da folha (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=False,
    )

    if uploaded_workbook is not None:
        st.write(f"Planilha selecionada: `{uploaded_workbook.name}`")

    if st.button(
        "Iniciar analise",
        type="primary",
        disabled=uploaded_workbook is None or selected_company is None,
    ):
        try:
            if selected_company is None:
                raise ValueError("Selecione a empresa antes de importar a planilha.")
            paths = create_dashboard_run_from_uploads(
                workbook_name=uploaded_workbook.name,
                workbook_bytes=uploaded_workbook.getvalue(),
            )
            run_dashboard_analysis(
                paths,
                selected_company_code=selected_company.company_code,
                selected_company_name=selected_company.company_name,
                selected_competence=selected_competence,
            )
            st.session_state[RUN_ROOT_KEY] = str(paths.run_root)
            st.session_state[ERROR_KEY] = None
            st.rerun()
        except Exception as exc:  # pragma: no cover - visual feedback path
            st.session_state[ERROR_KEY] = f"A analise nao conseguiu ser concluida: {exc}"
            st.rerun()


def _render_assisted_report_importer_tab() -> None:
    st.subheader("Importador assistido de relatorios")
    st.write(
        "Use esta area para extrair sugestoes de relatorios de folha. Nada e aplicado sem revisao e botao explicito."
    )

    selected_company = _render_company_selector(
        "Empresa do relatorio",
        key="empresa-importador-relatorios",
    )
    if selected_company is None:
        st.warning("Selecione a empresa antes de importar um relatorio.")
        return

    uploaded_report = st.file_uploader(
        "Relatorio de folha/resumo (.pdf, .txt, .csv, .xlsx)",
        type=["pdf", "txt", "csv", "xlsx"],
        accept_multiple_files=False,
        key="relatorio-importador-assistido",
    )
    if uploaded_report is not None:
        st.write(f"Relatorio selecionado: `{uploaded_report.name}`")

    if st.button(
        "Analisar relatorio",
        type="primary",
        disabled=uploaded_report is None,
    ):
        try:
            st.session_state[ASSISTED_IMPORT_RESULT_KEY] = analyze_report_import(
                file_name=uploaded_report.name,
                file_bytes=uploaded_report.getvalue(),
                selected_company_code=selected_company.company_code,
                selected_company_name=selected_company.company_name,
            )
        except Exception as exc:  # pragma: no cover - visual feedback path
            st.error(f"Nao foi possivel analisar o relatorio: {exc}")

    analysis = st.session_state.get(ASSISTED_IMPORT_RESULT_KEY)
    if analysis is None:
        return
    if analysis.selected_company_code != selected_company.company_code:
        st.info("A pre-analise carregada pertence a outra empresa selecionada. Analise o relatorio novamente.")
        return

    _render_assisted_import_analysis(analysis)


def _render_assisted_import_analysis(analysis) -> None:
    report = analysis.report
    st.markdown("**Conferencia do relatorio**")
    st.table(
        [
            {
                "Arquivo": report.file_name,
                "Empresa detectada": report.detected_company_code or "-",
                "Nome detectado": report.detected_company_name or "-",
                "Competencia": report.competence or "-",
                "Linhas lidas": report.text_line_count,
            }
        ]
    )
    diagnostics = getattr(report, "diagnostics", None)
    if diagnostics is not None:
        _render_report_extraction_diagnostics(diagnostics)
    else:
        st.warning("Esta pre-analise nao tem diagnostico tecnico. Limpe a analise do relatorio e envie o arquivo novamente.")

    if analysis.employee_reviews:
        st.markdown("**Funcionarios encontrados**")
        st.table(_assisted_employee_rows(analysis.employee_reviews))
    else:
        st.info(
            "Nenhum funcionario com matricula e nome explicitos foi encontrado. "
            "Abra o Diagnostico tecnico da extracao para verificar se o texto do PDF foi extraido corretamente."
        )

    if analysis.rubric_reviews:
        st.markdown("**Rubricas encontradas**")
        st.table(_assisted_rubric_rows(analysis.rubric_reviews))
    else:
        st.info(
            "Nenhuma rubrica com codigo e descricao explicitos foi encontrada. "
            "Abra o Diagnostico tecnico da extracao para verificar se o texto do PDF foi extraido corretamente."
        )

    if report.rubric_totals:
        st.markdown("**Totais por rubrica encontrados**")
        st.table(
            [
                {
                    "Rubrica": item.rubric_code,
                    "Descricao": item.description or "-",
                    "Total": item.total_value,
                    "Origem": item.source_reference,
                }
                for item in report.rubric_totals
            ]
        )

    if report.column_profiles:
        st.markdown("**Sugestoes de perfil de colunas**")
        st.table(
            [
                {
                    "Coluna": item.column_name,
                    "Modo": item.generation_mode,
                    "Rubrica sugerida": item.rubrica_target or "-",
                    "Tipo": item.value_kind or "exige revisao",
                    "Origem": item.source_reference,
                    "Observacoes": item.notes or "-",
                }
                for item in report.column_profiles
            ]
        )
        st.info(
            "As regras de perfil de colunas sao apenas sugeridas nesta aba. Revise e salve regras finais na aba Perfil de colunas."
        )

    if analysis.is_blocked:
        st.error(analysis.blocked_reason)
        _render_assisted_import_cancel_buttons()
        return

    _render_apply_employee_suggestions(analysis)
    _render_apply_rubric_suggestions(analysis)
    _render_assisted_import_cancel_buttons()


def _render_report_extraction_diagnostics(diagnostics) -> None:
    with st.expander("Diagnostico tecnico da extracao"):
        if diagnostics.extraction_warning:
            st.warning(diagnostics.extraction_warning)
        st.table(
            [
                {
                    "Extensao": diagnostics.file_extension or "-",
                    "Extrator PDF": diagnostics.pdf_text_extractor or "-",
                    "Texto extraido do PDF": "sim" if diagnostics.pdf_text_extracted else "nao",
                    "Caracteres brutos": diagnostics.raw_text_length,
                    "Linhas brutas": diagnostics.raw_line_count,
                    "Caracteres reconstruidos": diagnostics.reconstructed_text_length,
                    "Linhas reconstruidas": diagnostics.reconstructed_line_count,
                    "Contem EXTRATO MENSAL": "sim" if diagnostics.contains_extrato_mensal else "nao",
                    "Contem Empr.": "sim" if diagnostics.contains_empr else "nao",
                    "Contem codigo empresa selecionada": (
                        "sim" if diagnostics.contains_selected_company_code else "nao"
                    ),
                    "Contem competencia": "sim" if diagnostics.contains_competence_pattern else "nao",
                }
            ]
        )
        st.text_area(
            "Amostra do texto bruto extraido",
            value=diagnostics.raw_sample,
            height=180,
            disabled=True,
        )
        st.text_area(
            "Amostra do texto reconstruido",
            value=diagnostics.reconstructed_sample,
            height=180,
            disabled=True,
        )


def _assisted_employee_rows(employee_reviews) -> list[dict[str, str]]:
    rows = []
    for review in employee_reviews:
        current = review.current_record
        rows.append(
            {
                "Matricula": review.suggestion.domain_registration,
                "Nome no relatorio": review.suggestion.employee_name,
                "Status": review.status,
                "Cadastro atual": current.employee_name if current else "-",
                "Origem": review.suggestion.source_reference,
                "Mensagem": review.message,
            }
        )
    return rows


def _assisted_rubric_rows(rubric_reviews) -> list[dict[str, str]]:
    rows = []
    for review in rubric_reviews:
        current = review.current_record
        rows.append(
            {
                "Rubrica": review.suggestion.rubric_code,
                "Descricao no relatorio": review.suggestion.description,
                "Status": review.status,
                "Catalogo atual": current.description if current else "-",
                "Evento canonico": review.resolved_canonical_event or "exige revisao",
                "Tipo": review.resolved_value_kind or "exige revisao",
                "Origem": review.suggestion.source_reference,
                "Mensagem": review.message,
            }
        )
    return rows


def _render_apply_employee_suggestions(analysis) -> None:
    candidates = [review for review in analysis.employee_reviews if review.can_apply]
    if not candidates:
        return

    with st.form("form-aplicar-funcionarios-importador"):
        selected_registrations = []
        for review in candidates:
            label = (
                "Aplicar funcionario "
                f"{review.suggestion.domain_registration} - {review.suggestion.employee_name}"
            )
            if st.checkbox(label, value=False, key=f"aplicar-func-{review.suggestion.domain_registration}"):
                selected_registrations.append(review.suggestion.domain_registration)
        submitted = st.form_submit_button("Aplicar funcionarios selecionados")
        if submitted:
            if not selected_registrations:
                st.info("Nenhum funcionario foi selecionado para aplicar.")
                return
            result = apply_report_employee_suggestions(
                analysis,
                selected_domain_registrations=selected_registrations,
            )
            _render_apply_result("funcionarios", result)
            if result.applied:
                st.rerun()


def _render_apply_rubric_suggestions(analysis) -> None:
    candidates = [review for review in analysis.rubric_reviews if review.status != "existente"]
    if not candidates:
        return

    with st.form("form-aplicar-rubricas-importador"):
        selected_codes = []
        overrides = {}
        value_kind_options = ("Selecione o tipo", *VALUE_KIND_OPTIONS)
        for review in candidates:
            code = review.suggestion.rubric_code
            label = f"Aplicar rubrica {code} - {review.suggestion.description}"
            should_apply = st.checkbox(label, value=False, key=f"aplicar-rubrica-{code}")

            canonical_event = st.text_input(
                f"Evento canonico para rubrica {code}",
                value=review.resolved_canonical_event or "",
            )
            resolved_kind = review.resolved_value_kind
            kind_index = value_kind_options.index(resolved_kind) if resolved_kind in value_kind_options else 0
            value_kind = st.selectbox(
                f"Tipo do valor para rubrica {code}",
                options=value_kind_options,
                index=kind_index,
            )
            nature = st.selectbox(
                f"Natureza para rubrica {code}",
                options=RUBRIC_NATURE_OPTIONS,
                index=RUBRIC_NATURE_OPTIONS.index(review.resolved_nature),
            )
            if should_apply:
                selected_codes.append(code)
                overrides[code] = {
                    "canonical_event": canonical_event,
                    "value_kind": value_kind if value_kind != "Selecione o tipo" else None,
                    "nature": nature,
                }

        submitted = st.form_submit_button("Aplicar rubricas selecionadas")
        if submitted:
            if not selected_codes:
                st.info("Nenhuma rubrica foi selecionada para aplicar.")
                return
            result = apply_report_rubric_suggestions(
                analysis,
                selected_rubric_codes=selected_codes,
                review_overrides=overrides,
            )
            _render_apply_result("rubricas", result)
            if result.applied:
                st.rerun()


def _render_apply_result(label: str, result) -> None:
    if result.applied:
        st.success(f"{result.applied} {label} aplicados ao cadastro persistente.")
    if result.skipped:
        st.warning(f"{result.skipped} {label} ignorados por falta de selecao aplicavel ou dados obrigatorios.")
    for error in result.errors:
        st.error(error)


def _render_assisted_import_cancel_buttons() -> None:
    if st.button("Limpar analise do relatorio"):
        st.session_state.pop(ASSISTED_IMPORT_RESULT_KEY, None)
        st.rerun()
    if st.button("Ignorar sugestao"):
        st.session_state.pop(ASSISTED_IMPORT_RESULT_KEY, None)
        st.rerun()
    if st.button("Cancelar importacao"):
        st.session_state.pop(ASSISTED_IMPORT_RESULT_KEY, None)
        st.rerun()


def _render_company_registration_tab() -> None:
    st.subheader("Cadastro da empresa")
    entries = _company_entries()
    selected = _render_company_selector(
        "Empresa existente",
        key="empresa-cadastro",
        include_new=True,
    )

    with st.form("form-cadastro-empresa"):
        company_code = st.text_input("Codigo da empresa", value=selected.company_code if selected else "")
        company_name = st.text_input("Nome da empresa", value=selected.company_name if selected else "")
        default_process = st.text_input("Processo padrao", value=selected.default_process if selected and selected.default_process else "")
        competence = st.text_input("Competencia opcional", value=selected.competence if selected and selected.competence else "")
        is_active = st.checkbox("Status ativo", value=True if selected is None else selected.is_active)
        submitted = st.form_submit_button("Salvar empresa")
        if submitted:
            try:
                saved = save_company_admin_entry(
                    company_code=company_code,
                    company_name=company_name,
                    default_process=default_process,
                    competence=competence,
                    is_active=is_active,
                )
                st.success(f"Empresa salva: {saved.selection_label()}")
                st.rerun()
            except Exception as exc:  # pragma: no cover - visual feedback path
                st.error(f"Nao foi possivel salvar a empresa: {exc}")

    if entries:
        st.markdown("**Empresas cadastradas**")
        st.table(
            [
                {
                    "Codigo": entry.company_code,
                    "Nome": entry.company_name,
                    "Processo": entry.default_process or "-",
                    "Competencia": entry.competence or "-",
                    "Status": entry.status,
                    "Config": entry.config_version or "-",
                }
                for entry in entries
            ]
        )


def _render_employees_tab() -> None:
    st.subheader("Funcionarios")
    selected_company = _render_company_selector("Empresa dos funcionarios", key="empresa-funcionarios")
    if selected_company is None:
        return

    registry = load_company_employee_registry(
        selected_company.company_code,
        company_name=selected_company.company_name,
    )
    active_employees = list_active_employees(registry)
    if active_employees:
        st.table(
            [
                {
                    "Codigo/matricula Dominio": employee.domain_registration,
                    "Nome": employee.employee_name,
                    "Aliases": ", ".join(employee.aliases),
                    "Status": employee.status.value,
                    "Observacoes": employee.notes or "-",
                }
                for employee in active_employees
            ]
        )
    else:
        st.info("Nenhum funcionario ativo cadastrado para esta empresa.")

    employee_options = ["Novo funcionario"] + [
        f"{employee.domain_registration} - {employee.employee_name}" for employee in registry.employees
    ]
    selected_label = st.selectbox("Registro de funcionario", options=employee_options)
    selected_employee = None
    if selected_label != "Novo funcionario":
        selected_index = employee_options.index(selected_label) - 1
        selected_employee = registry.employees[selected_index]

    with st.form("form-funcionario"):
        domain_registration = st.text_input(
            "Codigo/matricula Dominio",
            value=selected_employee.domain_registration if selected_employee else "",
        )
        employee_name = st.text_input("Nome", value=selected_employee.employee_name if selected_employee else "")
        aliases = st.text_input(
            "Aliases separados por virgula",
            value=", ".join(selected_employee.aliases) if selected_employee else "",
        )
        status = st.selectbox(
            "Status",
            options=RECORD_STATUS_OPTIONS,
            index=RECORD_STATUS_OPTIONS.index(selected_employee.status.value) if selected_employee else 0,
        )
        notes = st.text_input("Observacoes", value=selected_employee.notes or "" if selected_employee else "")
        submitted = st.form_submit_button("Salvar funcionario")
        if submitted:
            try:
                save_employee_registry_record(
                    company_code=selected_company.company_code,
                    company_name=selected_company.company_name,
                    employee_key=selected_employee.employee_key if selected_employee else None,
                    domain_registration=domain_registration,
                    employee_name=employee_name,
                    aliases=aliases,
                    status=status,
                    notes=notes,
                )
                st.success("Funcionario salvo no cadastro persistente da empresa.")
                st.rerun()
            except Exception as exc:  # pragma: no cover - visual feedback path
                st.error(f"Nao foi possivel salvar o funcionario: {exc}")


def _render_rubrics_tab() -> None:
    st.subheader("Rubricas")
    selected_company = _render_company_selector("Empresa das rubricas", key="empresa-rubricas")
    if selected_company is None:
        return

    catalog = load_company_rubric_catalog(
        selected_company.company_code,
        company_name=selected_company.company_name,
    )
    active_rubrics = list_active_rubrics(catalog)
    if active_rubrics:
        st.table(
            [
                {
                    "Rubrica": rubric.rubric_code,
                    "Descricao": rubric.description,
                    "Evento canonico": rubric.canonical_event,
                    "Tipo": rubric.value_kind.value,
                    "Natureza": rubric.nature.value,
                    "Aliases": ", ".join(rubric.aliases),
                    "Status": rubric.status.value,
                }
                for rubric in active_rubrics
            ]
        )
    else:
        st.info("Nenhuma rubrica ativa cadastrada para esta empresa.")

    if catalog.rubrics:
        st.markdown("**Editar rubrica cadastrada**")
        for index, rubric in enumerate(catalog.rubrics):
            code_column, description_column, kind_column, nature_column, status_column, action_column = st.columns(6)
            code_column.write(rubric.rubric_code)
            description_column.write(rubric.description)
            kind_column.write(rubric.value_kind.value)
            nature_column.write(rubric.nature.value)
            status_column.write(rubric.status.value)
            if action_column.button(
                "Editar",
                key=f"editar-rubrica-{index}-{rubric.rubric_code}",
            ):
                st.session_state[RUBRIC_EDIT_INDEX_KEY] = index
                st.rerun()

    rubric_options = ["Nova rubrica"] + [
        f"{rubric.rubric_code} - {rubric.description}" for rubric in catalog.rubrics
    ]
    edit_index = _session_rubric_edit_index(catalog)
    selected_option_index = edit_index + 1 if edit_index is not None else 0
    selected_label = st.selectbox(
        "Registro de rubrica",
        options=rubric_options,
        index=selected_option_index,
    )
    selected_rubric = None
    if selected_label != "Nova rubrica":
        selected_index = rubric_options.index(selected_label) - 1
        selected_rubric = catalog.rubrics[selected_index]
        st.session_state[RUBRIC_EDIT_INDEX_KEY] = selected_index
    else:
        st.session_state.pop(RUBRIC_EDIT_INDEX_KEY, None)

    if selected_rubric is not None:
        st.info(f"Editando rubrica {selected_rubric.rubric_code} - {selected_rubric.description}.")

    with st.form("form-rubrica"):
        rubric_code = st.text_input("Codigo da rubrica", value=selected_rubric.rubric_code if selected_rubric else "")
        description = st.text_input("Descricao", value=selected_rubric.description if selected_rubric else "")
        canonical_event = st.text_input("Evento canonico", value=selected_rubric.canonical_event if selected_rubric else "")
        value_kind = st.selectbox(
            "Tipo do valor",
            options=VALUE_KIND_OPTIONS,
            index=_option_index(VALUE_KIND_OPTIONS, selected_rubric.value_kind.value if selected_rubric else None),
        )
        nature = st.selectbox(
            "Natureza",
            options=RUBRIC_NATURE_OPTIONS,
            index=_option_index(RUBRIC_NATURE_OPTIONS, selected_rubric.nature.value if selected_rubric else None),
        )
        aliases = st.text_input(
            "Aliases separados por virgula",
            value=", ".join(selected_rubric.aliases) if selected_rubric else "",
        )
        status = st.selectbox(
            "Status",
            options=RECORD_STATUS_OPTIONS,
            index=_option_index(RECORD_STATUS_OPTIONS, selected_rubric.status.value if selected_rubric else None),
        )
        notes = st.text_input("Observacoes", value=selected_rubric.notes or "" if selected_rubric else "")
        save_submitted = st.form_submit_button("Salvar rubrica")
        inactivate_submitted = st.form_submit_button(
            "Inativar rubrica",
            disabled=selected_rubric is None,
        )
        if save_submitted or inactivate_submitted:
            try:
                target_status = "inactive" if inactivate_submitted else status
                if _has_active_rubric_code_duplicate(
                    catalog,
                    rubric_code=rubric_code,
                    selected_rubric=selected_rubric,
                    target_status=target_status,
                ):
                    raise ValueError(
                        "Ja existe uma rubrica ativa com este codigo para esta empresa. "
                        "Inative a rubrica existente ou use outro codigo."
                    )
                save_rubric_catalog_record(
                    company_code=selected_company.company_code,
                    company_name=selected_company.company_name,
                    rubric_code=rubric_code,
                    description=description,
                    canonical_event=canonical_event,
                    value_kind=value_kind,
                    nature=nature,
                    aliases=aliases,
                    status=target_status,
                    notes=notes,
                )
                if inactivate_submitted:
                    st.success("Rubrica inativada no catalogo persistente da empresa.")
                else:
                    st.success("Rubrica salva no catalogo persistente da empresa.")
                st.session_state.pop(RUBRIC_EDIT_INDEX_KEY, None)
                st.rerun()
            except Exception as exc:  # pragma: no cover - visual feedback path
                st.error(f"Nao foi possivel salvar a rubrica: {exc}")


def _session_rubric_edit_index(catalog) -> int | None:
    raw_index = st.session_state.get(RUBRIC_EDIT_INDEX_KEY)
    if raw_index is None:
        return None
    try:
        index = int(raw_index)
    except (TypeError, ValueError):
        st.session_state.pop(RUBRIC_EDIT_INDEX_KEY, None)
        return None
    if index < 0 or index >= len(catalog.rubrics):
        st.session_state.pop(RUBRIC_EDIT_INDEX_KEY, None)
        return None
    return index


def _option_index(options: tuple[str, ...], value: str | None) -> int:
    if value in options:
        return options.index(value)
    return 0


def _has_active_rubric_code_duplicate(
    catalog,
    *,
    rubric_code: str,
    selected_rubric,
    target_status: str,
) -> bool:
    if str(target_status).strip() != "active":
        return False
    normalized_code = _normalize_rubric_code(rubric_code)
    if not normalized_code:
        return False
    for rubric in catalog.rubrics:
        if selected_rubric is not None and rubric is selected_rubric:
            continue
        if getattr(rubric.status, "value", rubric.status) != "active":
            continue
        if _normalize_rubric_code(rubric.rubric_code) == normalized_code:
            return True
    return False


def _normalize_rubric_code(value: object) -> str:
    return "".join(ch for ch in str(value or "").strip() if ch.isalnum()).upper()


def _render_column_profile_tab() -> None:
    st.subheader("Perfil de colunas")
    selected_company = _render_company_selector("Empresa do perfil", key="empresa-perfil-colunas")
    if selected_company is None:
        return

    try:
        profile = load_column_mapping_profile(selected_company.company_code)
        mappings = profile.mappings
    except ColumnMappingProfileError as exc:
        if exc.code != "profile_not_found":
            st.error(f"Nao foi possivel carregar o perfil de colunas: {exc}")
            return
        profile = None
        mappings = []

    if mappings:
        st.table(
            [
                {
                    "Coluna": rule.column_name or rule.column_key or "-",
                    "Modo": rule.generation_mode.value,
                    "Tipo": rule.value_kind.value,
                    "Rubrica unica": rule.rubrica_target or "-",
                    "Rubricas multiplas": ", ".join(rule.rubricas_target),
                    "Ignorar zero": "sim" if rule.ignore_zero else "nao",
                    "Ignorar texto": "sim" if rule.ignore_text else "nao",
                    "Ativa": "sim" if rule.enabled else "nao",
                }
                for rule in mappings
            ]
        )
    else:
        st.info("Nenhuma regra de perfil de colunas cadastrada para esta empresa.")

    rule_options = ["Nova regra"] + [rule.column_name or rule.column_key or "" for rule in mappings]
    selected_label = st.selectbox("Regra de coluna", options=rule_options)
    selected_rule = None
    if selected_label != "Nova regra":
        selected_index = rule_options.index(selected_label) - 1
        selected_rule = mappings[selected_index]

    with st.form("form-perfil-colunas"):
        column_name = st.text_input("Nome da coluna", value=selected_rule.column_name if selected_rule else "")
        value_kind = st.selectbox(
            "Tipo do valor da coluna",
            options=VALUE_KIND_OPTIONS,
            index=VALUE_KIND_OPTIONS.index(selected_rule.value_kind.value) if selected_rule else 0,
        )
        generation_mode = st.selectbox(
            "Modo de geracao",
            options=GENERATION_MODE_OPTIONS,
            index=GENERATION_MODE_OPTIONS.index(selected_rule.generation_mode.value) if selected_rule else 0,
        )
        rubrica_target = ""
        rubricas_target = ""
        if generation_mode == "single_line":
            rubrica_target = st.text_input("Rubrica unica", value=selected_rule.rubrica_target or "" if selected_rule else "")
        elif generation_mode == "multi_line":
            rubricas_target = st.text_input(
                "Rubricas multiplas separadas por virgula",
                value=", ".join(selected_rule.rubricas_target) if selected_rule else "",
            )
        else:
            st.info("Coluna ignorada nao envia rubrica.")
        ignore_zero = st.checkbox("Ignorar valores zerados", value=selected_rule.ignore_zero if selected_rule else True)
        ignore_text = st.checkbox("Ignorar textos sem valor numerico", value=selected_rule.ignore_text if selected_rule else True)
        enabled = False if generation_mode == "ignore" else st.checkbox("Regra habilitada", value=selected_rule.enabled if selected_rule else True)
        notes = st.text_input("Observacoes", value=selected_rule.notes or "" if selected_rule else "")
        submitted = st.form_submit_button("Salvar regra de coluna")
        if submitted:
            try:
                save_column_mapping_profile_rule(
                    company_code=selected_company.company_code,
                    company_name=selected_company.company_name,
                    default_process=selected_company.default_process,
                    column_name=column_name,
                    value_kind=value_kind,
                    generation_mode=generation_mode,
                    rubrica_target=rubrica_target,
                    rubricas_target=rubricas_target,
                    ignore_zero=ignore_zero,
                    ignore_text=ignore_text,
                    enabled=enabled,
                    notes=notes,
                )
                st.success("Regra salva no perfil persistente da empresa.")
                st.rerun()
            except Exception as exc:  # pragma: no cover - visual feedback path
                st.error(f"Nao foi possivel salvar a regra de coluna: {exc}")


def _render_txt_audit_tab(result) -> None:
    st.subheader("Auditoria TXT")
    if result is None:
        st.info("Nenhuma analise carregada para auditoria TXT.")
        return
    _render_txt_audit(result)


def _company_entries():
    try:
        return list_company_admin_entries()
    except Exception as exc:  # pragma: no cover - visual feedback path
        st.error(f"Nao foi possivel listar empresas cadastradas: {exc}")
        return ()


def _render_company_selector(label: str, *, key: str, include_new: bool = False):
    entries = _company_entries()
    labels = []
    if include_new:
        labels.append("Nova empresa")
    labels.append("Selecione uma empresa")
    labels.extend(entry.selection_label() for entry in entries)
    selected_label = st.selectbox(label, options=labels, key=key)
    if selected_label in {"Nova empresa", "Selecione uma empresa"}:
        return None
    selected_code = selected_label.split(" - ", 1)[0]
    return get_company_admin_entry(selected_code)


def _render_summary(result) -> None:
    st.subheader("Resumo da analise")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Empresa", result.summary.company_name)
    col2.metric("Competencia", result.summary.competence)
    col3.metric("Funcionarios", result.summary.employee_count)
    col4.metric("Lancamentos", result.summary.relevant_movement_count)
    col5.metric("Pendencias", result.summary.pending_count)
    col6.metric("Itens ignorados", result.summary.ignored_count)

    st.write(f"Status geral: **{result.summary.status_label}**")
    st.write(f"Empresa detectada: **{result.summary.company_name}** ({result.summary.company_code})")
    st.write(f"Competencia detectada: **{result.summary.competence}**")
    st.write(f"Configuracao interna: **{result.summary.config_status_label}**")
    if result.summary.config_source is not None:
        st.write(f"Origem da configuracao aplicada: `{result.summary.config_source}`")
    if result.summary.config_version is not None:
        st.write(f"Versao da configuracao aplicada: `{result.summary.config_version}`")
    st.write(f"Recomendacao: {result.summary.recommendation}")

    if result.summary.txt_enabled:
        st.success("TXT liberado. O caso esta apto para baixar o arquivo do Dominio.")
    else:
        st.warning("TXT ainda bloqueado. Revise as pendencias e reprocese antes de baixar.")

    if st.button("Reprocessar analise"):
        try:
            run_dashboard_analysis(result.paths)
            st.session_state[ERROR_KEY] = None
            st.rerun()
        except Exception as exc:  # pragma: no cover - visual feedback path
            st.session_state[ERROR_KEY] = f"Falha ao reprocessar a analise: {exc}"
            st.rerun()


def _render_txt_audit(result) -> None:
    if not result.paths.txt_path.exists() or result.summary.serialized_line_count == 0:
        return

    st.subheader("Auditoria visual do TXT")
    try:
        audit = build_txt_audit(result)
    except Exception as exc:  # pragma: no cover - defensive visual path
        st.warning(f"Nao foi possivel montar a auditoria visual do TXT: {exc}")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Linhas do TXT", audit.summary.total_lines)
    col2.metric("Empresa", result.summary.company_code)
    col3.metric(
        "Processo",
        ", ".join(audit.summary.process_codes) if audit.summary.process_codes else "-",
    )
    col4.metric("Competencia", audit.summary.competence)

    if audit.summary.rubric_totals:
        st.markdown("**Rubricas lancadas**")
        st.table(
            [
                {
                    "Rubrica": item.rubric,
                    "Rubrica TXT": item.rubric_raw,
                    "Linhas": item.line_count,
                    "Tipo": item.value_type,
                    "Total": item.display_total,
                }
                for item in audit.summary.rubric_totals
            ]
        )

    if audit.employee_rows:
        st.markdown("**Auditoria por funcionario**")
        st.table(
            [
                {
                    "Linha TXT": row.line_number,
                    "Matricula": row.domain_registration,
                    "Nome": row.employee_name or "-",
                    "Rubrica": row.rubric,
                    "Descricao": row.description,
                    "Valor/quantidade": row.launched_value,
                    "Status": row.check_status,
                }
                for row in audit.employee_rows
            ]
        )

    if audit.divergences:
        st.warning("Divergencias encontradas na auditoria visual do TXT.")
        st.table(
            [
                {
                    "Status": item.code,
                    "Linha TXT": item.line_number or "-",
                    "Matricula": item.domain_registration or "-",
                    "Rubrica": item.rubric or "-",
                    "Movimento": item.canonical_movement_id or "-",
                    "Mensagem": item.message,
                }
                for item in audit.divergences
            ]
        )
    else:
        st.success("Auditoria visual sem divergencias entre TXT e artefato mapeado.")


def _render_pendings(result) -> None:
    st.subheader("Pendencias para revisar")
    if not result.pendings:
        st.success("Nenhuma pendencia operacional aberta neste momento.")
        return

    st.table([_pending_table_row(item) for item in result.pendings])

    pending_map = {item.selection_label(): item for item in result.pendings}
    selected_label = st.selectbox(
        "Corrigir item selecionado",
        options=["Selecione um item"] + list(pending_map.keys()),
    )
    if selected_label == "Selecione um item":
        return

    pending = pending_map[selected_label]
    st.markdown(f"**Problema:** {pending.problem}")
    st.markdown(f"**O que fazer:** {pending.recommended_action}")
    st.markdown(f"**Valor atual:** {pending.found_value or 'em branco'}")
    st.markdown(f"**Codigo:** `{pending.code}`")
    st.markdown(f"**Etapa:** `{pending.stage}`")
    st.markdown(f"**Funcionario:** {pending.employee_name or pending.employee_key or '-'}")
    st.markdown(f"**Evento:** {pending.event_name or '-'}")
    st.markdown(f"**Coluna:** {pending.source_column_name or '-'}")

    if pending.can_edit_workbook:
        with st.form(f"corrigir-workbook-{pending.uid}"):
            corrected_value = st.text_input(
                "Valor corrigido",
                value="" if pending.found_value in {None, "em branco"} else pending.found_value,
            )
            submitted = st.form_submit_button("Salvar correcao")
            if submitted:
                try:
                    apply_workbook_cell_correction(
                        result.paths,
                        sheet_name=pending.source_sheet or "",
                        cell=pending.source_cell or "",
                        new_value=corrected_value,
                        pending_uid=pending.uid,
                        description=f"Correcao guiada de {pending.field_label}.",
                    )
                    run_dashboard_analysis(result.paths)
                    st.session_state[ERROR_KEY] = None
                    st.rerun()
                except Exception as exc:  # pragma: no cover - visual feedback path
                    st.session_state[ERROR_KEY] = f"Falha ao salvar a correcao: {exc}"
                    st.rerun()

    if pending.can_edit_employee_mapping:
        with st.form(f"corrigir-matricula-{pending.uid}"):
            domain_registration = st.text_input(
                "Matricula Dominio corrigida",
                value="" if pending.found_value in {None, "sem cadastro", "em branco"} else pending.found_value,
            )
            persist_to_employee_registry = st.checkbox(
                "Salvar no cadastro da empresa para proximas importacoes",
                value=False,
            )
            submitted = st.form_submit_button("Salvar matricula")
            if submitted:
                try:
                    if not domain_registration.strip():
                        raise ValueError("Informe a matricula/codigo Dominio.")
                    apply_dashboard_action(
                        result.paths,
                        action_type=DashboardActionType.EMPLOYEE_MAPPING_UPDATE,
                        pending_uid=pending.uid,
                        payload={
                            "domain_registration": domain_registration,
                            "persist_to_employee_registry": persist_to_employee_registry,
                        },
                    )
                    run_dashboard_analysis(result.paths)
                    st.session_state[ERROR_KEY] = None
                    st.rerun()
                except Exception as exc:  # pragma: no cover - visual feedback path
                    st.session_state[ERROR_KEY] = f"Falha ao salvar a matricula: {exc}"
                    st.rerun()

    if pending.can_edit_event_mapping:
        with st.form(f"corrigir-rubrica-{pending.uid}"):
            output_rubric = st.text_input(
                "Rubrica de saida corrigida",
                value="" if pending.found_value in {None, "sem rubrica", "em branco"} else pending.found_value,
            )
            persist_to_rubric_catalog = st.checkbox(
                "Salvar no catalogo de rubricas da empresa para proximas importacoes",
                value=False,
            )
            description = st.text_input("Descricao da rubrica", value="")
            value_kind = st.selectbox("Tipo do valor", options=VALUE_KIND_OPTIONS)
            canonical_event = st.text_input("Evento canonico", value=pending.event_name or "")
            nature = st.selectbox("Natureza", options=RUBRIC_NATURE_OPTIONS)
            submitted = st.form_submit_button("Salvar rubrica")
            if submitted:
                try:
                    if not output_rubric.strip():
                        raise ValueError("Informe a rubrica de saida.")
                    payload = {
                        "output_rubric": output_rubric,
                        "persist_to_rubric_catalog": persist_to_rubric_catalog,
                    }
                    if persist_to_rubric_catalog:
                        if not description.strip():
                            raise ValueError("Informe a descricao da rubrica para salvar no catalogo.")
                        if not value_kind:
                            raise ValueError("Informe o tipo do valor para salvar no catalogo.")
                        payload.update(
                            {
                                "description": description,
                                "value_kind": value_kind,
                                "canonical_event": canonical_event,
                                "nature": nature,
                            }
                        )
                    apply_dashboard_action(
                        result.paths,
                        action_type=DashboardActionType.EVENT_MAPPING_UPDATE,
                        pending_uid=pending.uid,
                        payload=payload,
                    )
                    run_dashboard_analysis(result.paths)
                    st.session_state[ERROR_KEY] = None
                    st.rerun()
                except Exception as exc:  # pragma: no cover - visual feedback path
                    st.session_state[ERROR_KEY] = f"Falha ao salvar a rubrica: {exc}"
                    st.rerun()

    if _can_edit_column_profile(pending):
        with st.form(f"corrigir-perfil-coluna-{pending.uid}"):
            column_name = st.text_input("Coluna", value=pending.source_column_name or "")
            generation_mode = st.selectbox("Modo de geracao", options=GENERATION_MODE_OPTIONS)
            value_kind = st.selectbox("Tipo do valor da coluna", options=VALUE_KIND_OPTIONS)
            rubrica_target = ""
            rubricas_target = ""
            if generation_mode == "single_line":
                rubrica_target = st.text_input("Rubrica unica", value="")
            elif generation_mode == "multi_line":
                rubricas_target = st.text_input("Rubricas multiplas separadas por virgula", value="")
            else:
                st.info("Coluna ignorada nao envia rubrica e nao cria lancamento.")
            ignore_zero = st.checkbox("Ignorar valores zerados", value=True)
            ignore_text = st.checkbox("Ignorar textos sem valor numerico", value=True)
            submitted = st.form_submit_button("Salvar regra da coluna")
            if submitted:
                try:
                    payload = _build_column_profile_payload(
                        column_name=column_name,
                        generation_mode=generation_mode,
                        value_kind=value_kind,
                        rubrica_target=rubrica_target,
                        rubricas_target=rubricas_target,
                        ignore_zero=ignore_zero,
                        ignore_text=ignore_text,
                    )
                    apply_dashboard_action(
                        result.paths,
                        action_type=DashboardActionType.COLUMN_MAPPING_PROFILE_UPDATE,
                        pending_uid=pending.uid,
                        payload=payload,
                    )
                    run_dashboard_analysis(result.paths)
                    st.session_state[ERROR_KEY] = None
                    st.rerun()
                except Exception as exc:  # pragma: no cover - visual feedback path
                    st.session_state[ERROR_KEY] = f"Falha ao salvar a regra da coluna: {exc}"
                    st.rerun()

    if pending.can_ignore:
        st.info(
            f"Opcao disponivel: **{pending.ignore_label}**. O dado original fica preservado na origem do caso e o ignorar vale apenas para esta execucao local."
        )
        if st.button("Ignorar nesta importacao", key=f"ignorar-{pending.uid}"):
            try:
                apply_dashboard_action(
                    result.paths,
                    action_type=DashboardActionType.IGNORE_PENDING,
                    pending_uid=pending.uid,
                )
                run_dashboard_analysis(result.paths)
                st.session_state[ERROR_KEY] = None
                st.rerun()
            except Exception as exc:  # pragma: no cover - visual feedback path
                st.session_state[ERROR_KEY] = f"Falha ao ignorar o item: {exc}"
                st.rerun()

    if not any(
        [
            pending.can_edit_workbook,
            pending.can_edit_employee_mapping,
            pending.can_edit_event_mapping,
            _can_edit_column_profile(pending),
            pending.can_ignore,
        ]
    ):
        st.info(
            "Este item nao tem correcao guiada neste MVP. Ajuste a origem correspondente e rode a analise novamente."
        )


def _can_edit_column_profile(pending) -> bool:
    return pending.stage == "perfil_colunas" and pending.code in COLUMN_MAPPING_PROFILE_CODES


def _pending_table_row(pending) -> dict[str, str]:
    return {
        "Severidade": pending.severity,
        "Etapa": pending.stage,
        "Codigo": pending.code,
        "Funcionario": pending.employee_name or pending.employee_key or "-",
        "Evento": pending.event_name or "-",
        "Coluna": pending.source_column_name or "-",
        "Problema": pending.problem,
        "Acao recomendada": pending.recommended_action,
    }


def _build_column_profile_payload(
    *,
    column_name: str,
    generation_mode: str,
    value_kind: str,
    rubrica_target: str,
    rubricas_target: str,
    ignore_zero: bool,
    ignore_text: bool,
) -> dict:
    column_name = column_name.strip()
    if not column_name:
        raise ValueError("Informe a coluna do perfil.")
    payload = {
        "column_name": column_name,
        "value_kind": value_kind,
        "generation_mode": generation_mode,
        "ignore_zero": ignore_zero,
        "ignore_text": ignore_text,
    }
    if generation_mode == "single_line":
        if not rubrica_target.strip():
            raise ValueError("Informe a rubrica unica para a regra single_line.")
        payload["rubrica_target"] = rubrica_target.strip()
    elif generation_mode == "multi_line":
        targets = [item.strip() for item in rubricas_target.split(",") if item.strip()]
        if len(targets) < 2:
            raise ValueError("Informe pelo menos duas rubricas para a regra multi_line.")
        payload["rubricas_target"] = targets
    elif generation_mode == "ignore":
        pass
    else:
        raise ValueError(f"Modo de geracao nao suportado: {generation_mode}.")
    return payload


def _render_actions_history(result) -> None:
    st.subheader("Historico desta importacao")
    if not result.state.actions:
        st.write("Nenhuma correcao ou ignorar foi registrado nesta execucao.")
        return

    rows = []
    for action in reversed(result.state.actions):
        rows.append(
            {
                "Quando": action.applied_at.strftime("%d/%m/%Y %H:%M:%S UTC"),
                "Acao": action.action_type.value,
                "Descricao": action.description,
            }
        )
    st.table(rows)


def _render_downloads(result) -> None:
    st.subheader("Baixar artefatos")
    txt_bytes = result.paths.txt_path.read_bytes() if result.paths.txt_path.exists() else b""
    validation_bytes = (
        result.paths.validation_path.read_bytes() if result.paths.validation_path.exists() else b""
    )

    st.download_button(
        "Baixar TXT do Dominio",
        data=txt_bytes,
        file_name=result.paths.txt_path.name,
        mime="text/plain",
        disabled=not result.summary.txt_enabled,
    )
    st.download_button(
        "Baixar resumo da validacao",
        data=validation_bytes,
        file_name=result.paths.validation_path.name,
        mime="application/json",
        disabled=not result.paths.validation_path.exists(),
    )


if __name__ == "__main__":
    main()
