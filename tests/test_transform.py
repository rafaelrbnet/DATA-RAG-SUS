from __future__ import annotations

import hashlib

import pandas as pd

from src.data.transform import _derive_row_id


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
