from __future__ import annotations

import streamlit as st

from dashboard import (
    DashboardActionType,
    apply_dashboard_action,
    apply_workbook_cell_correction,
    build_dashboard_paths,
    create_dashboard_run_from_uploads,
    load_dashboard_run,
    run_dashboard_analysis,
)


RUN_ROOT_KEY = "dashboard_v1_run_root"
ERROR_KEY = "dashboard_v1_last_error"
COLUMN_MAPPING_PROFILE_CODES = {
    "column_mapping_profile_missing",
    "column_mapping_profile_incomplete",
}
VALUE_KIND_OPTIONS = ("monetario", "horas", "quantidade")
GENERATION_MODE_OPTIONS = ("single_line", "multi_line", "ignore")
RUBRIC_NATURE_OPTIONS = ("unknown", "provento", "desconto", "informativo")


def main() -> None:
    st.set_page_config(
        page_title="Dashboard operacional V1",
        layout="wide",
    )

    st.title("Dashboard operacional do motor TXT V1")
    st.caption(
        "Use esta tela para importar a planilha, revisar pendencias, aplicar correcoes guiadas e baixar o TXT apenas quando ele estiver liberado."
    )

    _render_upload_area()
    _render_last_error()

    run_root = st.session_state.get(RUN_ROOT_KEY)
    if not run_root:
        return

    paths = build_dashboard_paths(run_root)
    if not paths.state_path.exists():
        return

    try:
        result = load_dashboard_run(paths)
    except Exception as exc:  # pragma: no cover - fallback visual only
        st.error(f"Nao foi possivel carregar a ultima analise: {exc}")
        return

    _render_summary(result)
    _render_pendings(result)
    _render_actions_history(result)
    _render_downloads(result)


def _render_last_error() -> None:
    last_error = st.session_state.get(ERROR_KEY)
    if last_error:
        st.error(last_error)


def _render_upload_area() -> None:
    st.subheader("Importar planilha")
    st.write(
        "Envie apenas a planilha preenchida. O sistema detecta empresa e competencia e tenta resolver internamente a configuracao da empresa."
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
        disabled=uploaded_workbook is None,
    ):
        try:
            paths = create_dashboard_run_from_uploads(
                workbook_name=uploaded_workbook.name,
                workbook_bytes=uploaded_workbook.getvalue(),
            )
            run_dashboard_analysis(paths)
            st.session_state[RUN_ROOT_KEY] = str(paths.run_root)
            st.session_state[ERROR_KEY] = None
            st.rerun()
        except Exception as exc:  # pragma: no cover - visual feedback path
            st.session_state[ERROR_KEY] = f"A analise nao conseguiu ser concluida: {exc}"
            st.rerun()


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
