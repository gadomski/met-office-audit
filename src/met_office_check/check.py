import datetime
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

from .model import Model


class Check(BaseModel):
    model: Model
    reference_datetime: datetime.datetime
    collection: str
    item: str
    has_item: bool = False
    missing: list[str]


def write_paths_parquet(checks: list[Check], path: Path) -> None:
    rows = []
    for check in checks:
        for s3_path in check.missing:
            rows.append(
                {
                    "model": str(check.model),
                    "reference_datetime": check.reference_datetime,
                    "path": s3_path,
                    "collection": check.collection,
                    "item": check.item,
                }
            )
    df = pd.DataFrame(
        rows, columns=["model", "reference_datetime", "path", "collection", "item"]
    )
    path.parent.mkdir(exist_ok=True)
    df.to_parquet(path, index=False)


def write_checks_parquet(checks: list[Check], path: Path) -> None:
    rows = []
    for check in checks:
        rows.append(
            {
                "collection": check.collection,
                "item": check.item,
                "reference_datetime": check.reference_datetime,
                "has_item": check.has_item,
                "num_missing": len(check.missing),
            }
        )
    df = pd.DataFrame(
        rows,
        columns=["collection", "item", "reference_datetime", "has_item", "num_missing"],
    )
    path.parent.mkdir(exist_ok=True)
    df.to_parquet(path, index=False)
