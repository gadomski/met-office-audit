"""Met Office STAC item completeness audit"""

from dataclasses import dataclass, field
from datetime import datetime

import rustac
from obstore.store import S3Store
from pystac import Item
from stactools.met_office_deterministic.constants import Model, Theme
from stactools.met_office_deterministic.stac import create_collection, create_items

PREFIXES = {
    Model.global_: "global-deterministic-10km",
    Model.uk: "uk-deterministic-2km",
}

PC_STAC_API_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"


@dataclass
class MetOfficeAudit:
    """Wraps an S3Store for a given Met Office model and theme."""

    model: Model
    theme: Theme
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
        self.expected_asset_keys = list(collection.item_assets.keys())

    async def list_forecast_run_prefixes(self) -> list[str]:
        prefixes = await self.s3_store.list_with_delimiter_async()["common_prefixes"]
        return list(prefixes)

    async def s3_items_for_forecast_run(
        self, reference_datetime: datetime
    ) -> list[Item]:
        hrefs = []

        forecast_prefix = reference_datetime.strftime("%Y%m%dT%H%MZ")
        async for list_result in self.s3_store.list_async(prefix=forecast_prefix):
            for object in list_result:
                if any(
                    asset_key in object["path"]
                    for asset_key in self.expected_asset_keys
                ):
                    hrefs.append(
                        f"s3://{self.s3_store.bucket}/{PREFIXES[self.model]}/{object['path']}"
                    )

        return create_items(hrefs)

    async def audit_pc_items(self, reference_datetime: datetime):
        s3_items = await self.s3_items_for_forecast_run(reference_datetime)
        s3_item_assets = {item.id: set(item.assets.keys()) for item in s3_items}

        pc_items = await rustac.search(
            href=PC_STAC_API_URL,
            collections=self.collection_id,
            ids=list(s3_item_assets.keys()),
            limit=100,
        )
        pc_item_assets = {item["id"]: set(item["assets"].keys()) for item in pc_items}

        expected_assets = 0
        missing_assets = []
        completely_missing_items = []
        for id, target_assets in s3_item_assets.items():
            expected_assets += len(target_assets)
            actual_assets = pc_item_assets.get(id, set())

            if not actual_assets:
                completely_missing_items.append(id)

            diff = target_assets - actual_assets
            for asset in diff:
                missing_assets.append((id, asset))
