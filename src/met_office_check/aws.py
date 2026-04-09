import asyncio
import datetime
from asyncio import Queue, Semaphore, TaskGroup
from collections import defaultdict
from pathlib import Path

import tqdm
from obstore.store import S3Store
from stactools.met_office_deterministic.href import Href

from . import azure
from .check import Check
from .model import Model

BUCKET = "met-office-atmospheric-model-data"


class Store:
    def __init__(self, model: Model) -> None:
        self.store = S3Store(
            bucket=BUCKET,
            region="eu-west-2",
            skip_signature=True,
            prefix=model.s3_prefix,
        )
        self.model = model
        self.semaphore = Semaphore(50)
        self.queue = Queue()

    def get_reference_datetimes(self) -> list[datetime.datetime]:
        reference_datetimes = list()
        for prefix in tqdm.tqdm(
            self.store.list_with_delimiter()["common_prefixes"],
            desc="Reference datetimes",
        ):
            reference_datetimes.append(
                datetime.datetime.strptime(prefix, "%Y%m%dT%H%MZ")
            )
        return reference_datetimes

    async def get_s3_paths(self, reference_datetime: datetime.datetime) -> list[str]:
        forecast_prefix = reference_datetime.strftime("%Y%m%dT%H%MZ")
        prefix = self.store.prefix
        paths = []
        async for list_result in self.store.list_async(prefix=forecast_prefix):
            for object in list_result:
                paths.append(f"s3://{BUCKET}/{prefix}/{object['path']}")
        return paths

    async def check(
        self,
        reference_datetime: datetime.datetime,
        directory: Path,
    ) -> list[Check]:
        async with self.semaphore:
            s3_paths = set(await self.get_s3_paths(reference_datetime))
        await self.queue.put(reference_datetime)

        items = defaultdict(dict)
        for item in azure.get_items(directory, self.model, reference_datetime):
            items[item.collection_id][item.id] = item
        if not items:
            return []

        checks: defaultdict[str, dict[str, Check]] = defaultdict(dict)
        for s3_path in s3_paths:
            href = Href.parse(s3_path)
            item = items.get(href.collection_id, {}).get(href.item_id)
            if href.item_id in checks[href.collection_id]:
                check = checks[href.collection_id][href.item_id]
            else:
                check = Check(
                    model=self.model,
                    reference_datetime=reference_datetime,
                    collection=href.collection_id,
                    item=href.item_id,
                    missing=[],
                )
                checks[href.collection_id][href.item_id] = check

            if item:
                check.has_item = True
                missing = True
                for asset in item.assets.values():
                    if asset.href.rsplit("/", 1)[1] == s3_path.rsplit("/", 1)[1]:
                        missing = False
                        break
                if missing:
                    check.missing.append(s3_path)
            else:
                check.has_item = False
                check.missing.append(s3_path)

        checks_list = []
        for item_checks in checks.values():
            checks_list.extend(item_checks.values())
        return checks_list

    async def check_all(self, directory: Path) -> list[Check]:
        reference_datetimes = self.get_reference_datetimes()
        progress = asyncio.create_task(self.progress(len(reference_datetimes)))
        async with TaskGroup() as task_group:
            tasks = [
                task_group.create_task(self.check(reference_datetime, directory))
                for reference_datetime in reference_datetimes
            ]
        progress.cancel()

        checks = [task.result() for task in tasks]
        return [check for sublist in checks for check in sublist]

    async def progress(self, total: int) -> None:
        progress = tqdm.tqdm(total=total, desc="Checks")
        try:
            while True:
                _ = await self.queue.get()
                progress.update()
        finally:
            progress.close()
