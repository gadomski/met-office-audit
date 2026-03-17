from __future__ import annotations

import asyncio
from asyncio import Queue, Semaphore, TaskGroup
from dataclasses import dataclass

import httpx
import tqdm
from obstore.store import AzureStore, S3Store
from stactools.met_office_deterministic.constants import Theme


@dataclass(frozen=True)
class MetOfficePath:
    interval: str
    variable: str

    @classmethod
    def parse(cls, path: str) -> MetOfficePath:
        file_name = path.split("/")[-1]
        parts = file_name.split("-")
        return MetOfficePath(
            interval=parts[1], variable="-".join(parts[2:]).split(".")[0]
        )


class MetOfficeStore:
    def __init__(self):
        self.semaphore = Semaphore(100)
        self.queue = Queue()

    async def progress(self, total: int) -> None:
        progress = tqdm.tqdm(total=total)
        try:
            while True:
                _ = await self.queue.get()
                progress.update()
        finally:
            progress.close()


class MetOffceS3Store(MetOfficeStore):
    def __init__(self, prefix: str, theme: Theme):
        super().__init__()
        self.store = S3Store(
            bucket="met-office-atmospheric-model-data",
            region="eu-west-2",
            skip_signature=True,
            prefix=prefix,
        )
        self.theme = theme

    def list_prefixes(self) -> list[str]:
        return list(self.store.list_with_delimiter()["common_prefixes"])

    async def count_prefix(self, prefix: str) -> int:
        count = 0
        async with self.semaphore:
            async for list_result in self.store.list(prefix=prefix):
                for object_meta in list_result:
                    if object_meta["path"].endswith(".updated"):
                        continue
                    path = MetOfficePath.parse(object_meta["path"])
                    try:
                        if Theme.from_parameter(path.variable) == self.theme:
                            count += 1
                    except ValueError:
                        pass
        await self.queue.put(prefix)
        return count


class MetOfficeAzureStore(MetOfficeStore):
    def __init__(self, prefix: str):
        super().__init__()
        sas_key = httpx.get(
            "https://planetarycomputer.microsoft.com/api/sas/v1/token/ukmoeuwest/deterministic"
        ).json()["token"]
        self.store = AzureStore(
            account_name="ukmoeuwest",
            container_name="deterministic",
            sas_key=sas_key,
            prefix=prefix,
        )

    async def count_prefix(self, prefix: str) -> int:
        count = 0
        async with self.semaphore:
            async for list_result in self.store.list(prefix=prefix):
                for object_meta in list_result:
                    if object_meta["path"].endswith(".updated"):
                        continue
                    count += 1
        await self.queue.put(prefix)
        return count


async def main() -> None:
    s3_store = MetOffceS3Store("global-deterministic-10km", Theme.near_surface)
    prefixes = s3_store.list_prefixes()
    progress = asyncio.create_task(s3_store.progress(len(prefixes)))
    async with TaskGroup() as task_group:
        tasks = {
            prefix: task_group.create_task(s3_store.count_prefix(prefix))
            for prefix in prefixes
        }
    aws_counts = {prefix: task.result() for prefix, task in tasks.items()}
    progress.cancel()

    azure_store = MetOfficeAzureStore(prefix="global/near-surface")
    progress = asyncio.create_task(azure_store.progress(len(prefixes)))
    async with TaskGroup() as task_group:
        tasks = {
            prefix: task_group.create_task(azure_store.count_prefix(prefix))
            for prefix in prefixes
        }
    microsoft_counts = {prefix: task.result() for prefix, task in tasks.items()}
    progress.cancel()

    print("prefix,aws,microsoft")
    for prefix in prefixes:
        print(
            ",".join([prefix, str(aws_counts[prefix]), str(microsoft_counts[prefix])])
        )


asyncio.run(main())
