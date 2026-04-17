import datetime
from pathlib import Path

from pystac import Item
from rustac import DuckdbClient

from .model import Model


def get_items(
    directory: Path, model: Model, reference_datetime: datetime.datetime
) -> list[Item]:
    client = DuckdbClient()
    return [
        Item.from_dict(item)
        for item in client.search(
            str(directory.resolve()),
            filter={
                "op": "and",
                "args": [
                    {
                        "op": "=",
                        "args": [{"property": "met_office_deterministic:model"}, model],
                    },
                    {
                        "op": "=",
                        "args": [
                            {"property": "forecast:reference_datetime"},
                            reference_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        ],
                    },
                ],
            },
        )
    ]
