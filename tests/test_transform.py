from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

from src.data.transform import _derive_row_id, _processed_file_has_valid_row_id


def test_derive_row_id_uses_internal_source_columns() -> None:
    df = pd.DataFrame(
        {
            "__source_file": [
                "data/raw/ano=2022/uf=CE/sistema=SIA/arq.parquet",
                "data/raw/ano=2022/uf=CE/sistema=SIA/arq.parquet",
            ],
            "__source_row": [0, 1],
        }
    )

    row_id = _derive_row_id(df)

    assert row_id.nunique() == 2
    assert row_id.iloc[0] == hashlib.sha1(
        b"data/raw/ano=2022/uf=CE/sistema=SIA/arq.parquet#0"
    ).hexdigest()
    assert row_id.iloc[1] == hashlib.sha1(
        b"data/raw/ano=2022/uf=CE/sistema=SIA/arq.parquet#1"
    ).hexdigest()


def test_derive_row_id_keeps_legacy_source_column_compatibility() -> None:
    df = pd.DataFrame(
        {
            "source_file": ["raw/a.parquet", "raw/a.parquet"],
            "source_row": [10, 11],
        }
    )

    row_id = _derive_row_id(df)

    assert row_id.nunique() == 2


def test_processed_file_has_valid_row_id_detects_legacy_parquet_without_column(tmp_path: Path) -> None:
    path = tmp_path / "processed.parquet"
    pd.DataFrame({"sistema": ["SIA"]}).to_parquet(path, index=False)

    assert _processed_file_has_valid_row_id(path) is False


def test_processed_file_has_valid_row_id_accepts_parquet_with_column(tmp_path: Path) -> None:
    path = tmp_path / "processed.parquet"
    pd.DataFrame({"row_id": ["abc123"], "sistema": ["SIA"]}).to_parquet(path, index=False)

    assert _processed_file_has_valid_row_id(path) is True
