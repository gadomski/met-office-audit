import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from urllib.parse import urlparse

import httpx
import rustac
from obstore.store import AzureStore, S3Store
from pystac import Item
from stactools.met_office_deterministic.constants import Model, Theme
from stactools.met_office_deterministic.stac import (
    _get_item_assets,
    create_collection,
    create_items,
)

PREFIXES = {
    Model.global_: "global-deterministic-10km",
    Model.uk: "uk-deterministic-2km",
}

PC_STAC_API_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"

logger = logging.getLogger(__name__)


class AuditSource(str, Enum):
    """Source to query when auditing STAC item completeness."""

    api = "api"
    geoparquet = "geoparquet"


@dataclass(frozen=True)
class AuditResult:
    """Result of auditing Planetary Computer items against S3 source data."""

    missing_assets: list[tuple[str, str]]
    completely_missing_items: list[str]
    complete_item_count: int
    incomplete_item_count: int


def compare_item_assets(
    s3_item_assets: dict[str, set[str]],
    pc_item_assets: dict[str, set[str]],
) -> AuditResult:
    """Compare S3 source item assets against Planetary Computer item assets.

    Args:
        s3_item_assets: Mapping of item ID to asset keys from S3 (the source of truth).
        pc_item_assets: Mapping of item ID to asset keys from Planetary Computer.

    Returns:
        An AuditResult describing any missing items or assets.
    """
    missing_assets = []
    completely_missing_items = []
    complete_item_count = 0
    incomplete_item_count = 0
    for item_id, target_assets in s3_item_assets.items():
        actual_assets = pc_item_assets.get(item_id, set())
        if not actual_assets:
            completely_missing_items.append(item_id)
        diff = target_assets - actual_assets
        if diff:
            for asset in diff:
                missing_assets.append((item_id, asset))
            if actual_assets:
                incomplete_item_count += 1
        else:
            complete_item_count += 1
    return AuditResult(
        missing_assets=missing_assets,
        completely_missing_items=completely_missing_items,
        complete_item_count=complete_item_count,
        incomplete_item_count=incomplete_item_count,
    )


@dataclass
class MetOfficeAudit:
    """System for auditing STAC item completeness for the Met Office collections"""

    model: Model
    theme: Theme
    source: AuditSource = AuditSource.api
    s3_store: S3Store = field(init=False)
    collection_id: str = field(init=False)
    expected_asset_keys: list[str] = field(init=False)

    def __post_init__(self):
        self.s3_store = S3Store(
            bucket="met-office-atmospheric-model-data",
            region="eu-west-2",
            skip_signature=True,
            prefix=PREFIXES[self.model],
        )

        collection = create_collection(
            model=self.model,
            theme=self.theme,
        )
        self.collection_id = collection.id

        item_assets = _get_item_assets(self.model, self.theme)
        self.expected_asset_keys = list(item_assets.keys())

    def download_stac_geoparquet(self) -> Path:
        """Download the stac-geoparquet archive for this collection to /tmp.

        Fetches the geoparquet asset href from the Planetary Computer collection
        metadata, acquires a SAS token, and streams the file to a local path
        derived from the remote filename. Skips the download if the file already
        exists.

        Returns:
            Path to the downloaded (or cached) stac-geoparquet file.
        """
        api_collection = httpx.get(
            f"{PC_STAC_API_URL}/collections/{self.collection_id}"
        ).json()
        stac_geoparquet_asset = api_collection["assets"]["geoparquet-items"]
        asset_href_parsed = urlparse(stac_geoparquet_asset["href"])
        geoparquet_account_name = stac_geoparquet_asset["table:storage_options"][
            "account_name"
        ]

        dst_file = Path("/tmp") / Path(asset_href_parsed.path).name
        if dst_file.exists():
            logger.info("Using cached stac-geoparquet file: %s", dst_file)
            return dst_file

        sas_key = httpx.get(
            f"https://planetarycomputer.microsoft.com/api/sas/v1/token/{geoparquet_account_name}/{asset_href_parsed.netloc}"
        ).json()["token"]

        items_store = AzureStore(
            account_name=geoparquet_account_name,
            container_name=asset_href_parsed.netloc,
            sas_key=sas_key,
        )

        logger.info("Downloading stac-geoparquet to %s", dst_file)
        resp = items_store.get(asset_href_parsed.path)
        with open(dst_file, "wb") as f:
            for chunk in resp:
                f.write(chunk)

        return dst_file

    async def list_reference_datetimes(self) -> list[datetime]:
        prefixes = await self.s3_store.list_with_delimiter_async()
        return [
            datetime.strptime(prefix, "%Y%m%dT%H%MZ")
            for prefix in prefixes["common_prefixes"]
        ]

    async def s3_items_for_forecast_run(
        self, reference_datetime: datetime
    ) -> list[Item]:
        hrefs = []

        forecast_prefix = reference_datetime.strftime("%Y%m%dT%H%MZ")
        bucket = self.s3_store.config["bucket"]
        prefix = self.s3_store.prefix
        logger.info(f"Searching for assets in s3://{bucket}/{prefix}/{forecast_prefix}")

        async for list_result in self.s3_store.list_async(prefix=forecast_prefix):
            for object in list_result:
                if any(
                    asset_key in object["path"]
                    for asset_key in self.expected_asset_keys
                ):
                    hrefs.append(f"s3://{bucket}/{prefix}/{object['path']}")
        if not hrefs:
            raise ValueError(
                f"no assets found in S3 for {reference_datetime.isoformat()}"
            )

        return create_items(hrefs)

    async def audit_pc_items(self, reference_datetime: datetime) -> AuditResult:
        """Audit STAC items against S3 source data for a forecast run.

        The source queried is determined by ``self.source``: ``AuditSource.api``
        queries the Planetary Computer STAC API directly, while
        ``AuditSource.geoparquet`` downloads (or reuses a cached copy of) the
        collection's stac-geoparquet archive and queries that instead.

        Args:
            reference_datetime: The reference datetime of the forecast run to audit.

        Returns:
            An AuditResult with any missing assets or completely missing items.
        """
        s3_items = await self.s3_items_for_forecast_run(reference_datetime)
        s3_item_assets = {item.id: set(item.assets.keys()) for item in s3_items}

        if self.source == AuditSource.geoparquet:
            source_href = str(self.download_stac_geoparquet())
            logger.info("Searching for items in stac-geoparquet archive")
        else:
            source_href = PC_STAC_API_URL
            logger.info("Searching for items in the PC STAC API")

        pc_items = await rustac.search(
            href=source_href,
            collections=self.collection_id,
            ids=list(s3_item_assets.keys()),
            limit=100,
        )
        pc_item_assets = {item["id"]: set(item["assets"].keys()) for item in pc_items}

        return compare_item_assets(s3_item_assets, pc_item_assets)
