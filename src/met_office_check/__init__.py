import asyncio
import datetime
import urllib.parse
from pathlib import Path
from typing import Annotated

import tqdm
from httpx import Client
from obstore.store import AzureStore
from typer import Argument, Exit, Option, Typer

from . import aws
from .check import write_checks_parquet, write_paths_parquet
from .model import Model

app = Typer()


@app.command()
def check_all(
    model: Annotated[
        Model,
        Argument(
            help="Model to check",
        ),
    ],
    directory: Annotated[
        Path,
        Argument(
            help="The data directory. If not provided, defaults to data",
        ),
    ] = Path("data"),
    output: Annotated[
        Path,
        Argument(
            help="The output directory. If not provided, defaults to output",
        ),
    ] = Path("output"),
) -> None:
    """Check all reference datetimes."""
    if not directory.exists():
        print(f"{directory} does not exist")
        raise Exit(1)

    store = aws.Store(model)
    results = asyncio.run(store.check_all(directory))
    write_checks_parquet(results, output / "checks.parquet")
    write_paths_parquet(results, output / "paths.parquet")
    print(f"Output written to {output}")


@app.command()
def check(
    model: Annotated[
        Model,
        Argument(
            help="Model to check",
        ),
    ],
    reference_datetime: Annotated[
        datetime.datetime | None,
        Option(
            "--reference-datetime",
            "-d",
            help="Forecast reference datetime (ISO 8601). If omitted, uses yesterday at midnight.",
            formats=["%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"],
        ),
    ] = None,
    directory: Annotated[
        Path,
        Argument(
            help="The data directory. If not provided, defaults to data",
        ),
    ] = Path("data"),
    output: Annotated[
        Path,
        Argument(
            help="The output directory. If not provided, defaults to output",
        ),
    ] = Path("output"),
) -> None:
    """Check a single reference datetime"""
    if not directory.exists():
        print(f"{directory} does not exist")
        raise Exit(1)
    if reference_datetime is None:
        reference_datetime = datetime.datetime.combine(
            datetime.date.today() - datetime.timedelta(days=1),
            datetime.time(0, tzinfo=datetime.timezone.utc),
        )

    store = aws.Store(model)
    result = asyncio.run(store.check(reference_datetime, directory))
    write_paths_parquet(result, output / "paths.parquet")
    write_checks_parquet(result, output / "checks.parquet")
    print(f"Output written to {output}")


@app.command()
def download_stac_geoparquet(
    model: Annotated[
        Model,
        Argument(
            help="Model to check",
        ),
    ],
    directory: Annotated[
        Path,
        Argument(
            help="The target directory. If not provided, defaults to data",
        ),
    ] = Path("data"),
) -> None:
    """Download all met office stac geoparquet files to the provided directory."""
    client = Client()
    collections = (
        client.get("https://planetarycomputer.microsoft.com/api/stac/v1/collections")
        .raise_for_status()
        .json()
    )
    for collection in collections["collections"]:
        if not collection["id"].startswith(f"met-office-{model}"):
            continue
        asset = collection["assets"]["geoparquet-items"]
        asset_href = urllib.parse.urlparse(asset["href"])
        geoparquet_account_name = asset["table:storage_options"]["account_name"]
        sas_key = (
            client.get(
                f"https://planetarycomputer.microsoft.com/api/sas/v1/token/{geoparquet_account_name}/{asset_href.netloc}"
            )
            .raise_for_status()
            .json()["token"]
        )
        store = AzureStore(
            account_name=geoparquet_account_name,
            container_name=asset_href.netloc,
            sas_key=sas_key,
        )
        for list_result in store.list(asset_href.path):
            for object in tqdm.tqdm(list_result, desc=asset_href.path):
                path = directory / object["path"]
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "wb") as f:
                    f.write(store.get(object["path"]).bytes())
