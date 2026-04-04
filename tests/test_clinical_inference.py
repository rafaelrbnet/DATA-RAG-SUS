from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data.clinical_inference import (
    _build_narrative,
    _enriched_file_has_valid_row_id,
    _infer_deslocamento_territorial,
    _infer_interpretacao_clinica,
    _infer_tipo_atendimento,
)


def test_enriched_file_has_valid_row_id_detects_placeholder_values(tmp_path: Path) -> None:
    path = tmp_path / "enriched.parquet"
    pd.DataFrame({"row_id": ["row_missing_0"], "clinical_tipo_atendimento": ["teste"]}).to_parquet(path, index=False)

    assert _enriched_file_has_valid_row_id(path) is False


def test_enriched_file_has_valid_row_id_accepts_real_values(tmp_path: Path) -> None:
    path = tmp_path / "enriched.parquet"
    pd.DataFrame({"row_id": ["169ab639070d3256441f0759f1fb89805276fecd"]}).to_parquet(path, index=False)

    assert _enriched_file_has_valid_row_id(path) is True


def test_infer_interpretacao_clinica_maps_expected_groups() -> None:
    series = pd.Series(["I10", "J45", "M16", "S72", "C50", "E11", "Z00"], dtype="string")

    result = _infer_interpretacao_clinica(series)

    assert result.tolist() == [
        "doencas do aparelho circulatorio",
        "doencas do aparelho respiratorio",
        "doencas osteomusculares e do tecido conjuntivo",
        "lesoes e causas traumaticas",
        "neoplasias",
        "doencas endocrinas, nutricionais e metabolicas",
        "condicoes clinicas diversas",
    ]


def test_infer_tipo_atendimento_maps_sistemas() -> None:
    series = pd.Series(["SIA", "SIH", "OUTRO"], dtype="string")

    result = _infer_tipo_atendimento(series)

    assert result.tolist() == [
        "producao ambulatorial",
        "episodio de internacao",
        "atendimento nao especificado",
    ]


def test_infer_deslocamento_territorial_prefers_explicit_flags_and_fallbacks() -> None:
    result = _infer_deslocamento_territorial(
        pd.Series(["1", None, None, None], dtype="string"),
        pd.Series([None, "1", None, None], dtype="string"),
        pd.Series(["230440", "230440", "230440", "230440"], dtype="string"),
        pd.Series(["230450", "250750", "230450", "230440"], dtype="string"),
    )

    assert result.tolist() == [
        "deslocamento intermunicipal",
        "deslocamento interestadual",
        "deslocamento intermunicipal",
        "sem evidencia de deslocamento territorial",
    ]


def test_build_narrative_generates_expected_metadata_and_text_for_sia() -> None:
    df = pd.DataFrame(
        {
            "row_id": ["abc123"],
            "sistema": ["SIA"],
            "competencia_ano_mes": [202204],
            "cnes_estabelecimento": ["1234567"],
            "cod_munic_estabelecimento": ["230440"],
            "uf_origem": ["CE"],
            "cid_principal": ["M160"],
            "icd_group": ["M16"],
            "nome_proced": ["Artroplastia de quadril"],
            "cod_procedimento": ["0408050160"],
            "custo_total": [1234.56],
            "pa_mndif": ["1"],
            "pa_ufdif": [None],
            "cod_munic_residencia": ["230440"],
            "pa_docorig": ["BPA"],
            "pa_autoriz": ["AUT123"],
        }
    )

    result = _build_narrative(df)

    assert result.loc[0, "row_id"] == "abc123"
    assert result.loc[0, "clinical_template_id"] == "AHEN_V1"
    assert result.loc[0, "clinical_inference_version"] == "ahen_v1.0.0"
    assert result.loc[0, "clinical_inference_rule_id"] == "RULE_AHEN_SIA_V11"
    assert result.loc[0, "clinical_tipo_atendimento"] == "producao ambulatorial"
    assert result.loc[0, "clinical_deslocamento_territorial"] == "deslocamento intermunicipal"
    assert result.loc[0, "clinical_interpretacao_clinica"] == "doencas osteomusculares e do tecido conjuntivo"
    narrative = result.loc[0, "clinical_event_narrative"]
    assert "Registro de evento assistencial identificado na base SIA" in narrative
    assert "Artroplastia de quadril" in narrative
    assert "R$ 1.234,56" in narrative
    assert "documento de origem BPA e autorizacao AUT123" in narrative


def test_build_narrative_generates_expected_metadata_and_text_for_sih() -> None:
    df = pd.DataFrame(
        {
            "row_id": ["def456"],
            "sistema": ["SIH"],
            "competencia_ano_mes": [202204],
            "cnes_estabelecimento": ["7654321"],
            "cod_munic_estabelecimento": ["250750"],
            "uf_origem": ["PB"],
            "cid_principal": ["I219"],
            "icd_group": ["I21"],
            "nome_proced": ["Tratamento clinico em cardiologia"],
            "cod_procedimento": ["0303060190"],
            "custo_total": [987.0],
            "pa_mndif": [None],
            "pa_ufdif": [None],
            "cod_munic_residencia": ["230440"],
            "n_aih": ["AIH999"],
            "dias_perm": [4],
            "morte": [0],
        }
    )

    result = _build_narrative(df)

    assert result.loc[0, "row_id"] == "def456"
    assert result.loc[0, "clinical_inference_rule_id"] == "RULE_AHEN_SIH_V11"
    assert result.loc[0, "clinical_tipo_atendimento"] == "episodio de internacao"
    assert result.loc[0, "clinical_deslocamento_territorial"] == "deslocamento interestadual"
    assert result.loc[0, "clinical_interpretacao_clinica"] == "doencas do aparelho circulatorio"
    narrative = result.loc[0, "clinical_event_narrative"]
    assert "Registro de evento assistencial identificado na base SIH" in narrative
    assert "AIH AIH999" in narrative
    assert "permanencia informada de 4 dia(s)" in narrative
    assert "R$ 987,00" in narrative
