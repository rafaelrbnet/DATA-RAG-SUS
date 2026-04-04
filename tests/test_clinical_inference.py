from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data.clinical_inference import _enriched_file_has_valid_row_id


def test_enriched_file_has_valid_row_id_detects_placeholder_values(tmp_path: Path) -> None:
    path = tmp_path / "enriched.parquet"
    pd.DataFrame({"row_id": ["row_missing_0"], "clinical_tipo_atendimento": ["teste"]}).to_parquet(path, index=False)

    assert _enriched_file_has_valid_row_id(path) is False


def test_enriched_file_has_valid_row_id_accepts_real_values(tmp_path: Path) -> None:
    path = tmp_path / "enriched.parquet"
    pd.DataFrame({"row_id": ["169ab639070d3256441f0759f1fb89805276fecd"]}).to_parquet(path, index=False)

    assert _enriched_file_has_valid_row_id(path) is True
