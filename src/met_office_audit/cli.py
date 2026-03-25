"""CLI for the Met Office STAC item completeness audit."""

import asyncio
import logging
import random
from datetime import datetime
from typing import Annotated, Optional

import typer
from stactools.met_office_deterministic.constants import Model, Theme

from met_office_audit.audit import AuditSource, MetOfficeAudit

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def audit(
    model: Annotated[Model, typer.Argument(help="Met Office model to audit")],
    theme: Annotated[Theme, typer.Argument(help="Theme to audit")],
    reference_datetime: Annotated[
        Optional[datetime],
        typer.Option(
            "--reference-datetime",
            "-d",
            help="Forecast reference datetime (ISO 8601). If omitted, one is sampled at random.",
            formats=["%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"],
        ),
    ] = None,
    source: Annotated[
        AuditSource,
        typer.Option("--source", "-s", help="Source to query for STAC items."),
    ] = AuditSource.api,
) -> None:
    """Audit Planetary Computer STAC items against the Met Office S3 source data."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(_audit(model, theme, reference_datetime, source))


async def _audit(
    model: Model,
    theme: Theme,
    reference_datetime: Optional[datetime],
    source: AuditSource,
) -> None:
    """Run the audit asynchronously."""
    auditor = MetOfficeAudit(model=model, theme=theme, source=source)

    if reference_datetime is None:
        available = await auditor.list_reference_datetimes()
        if not available:
            typer.echo("No forecast runs found in S3.", err=True)
            raise typer.Exit(1)
        reference_datetime = random.choice(available)
        logger.info("Sampled reference datetime: %s", reference_datetime.isoformat())

    logger.info(
        "Auditing %s/%s for %s",
        model.value,
        theme.value,
        reference_datetime.isoformat(),
    )

    result = await auditor.audit_pc_items(reference_datetime)

    typer.echo(f"Complete items:           {result.complete_item_count}")
    typer.echo(f"Incomplete items:         {result.incomplete_item_count}")
    typer.echo(f"Completely missing items: {len(result.completely_missing_items)}")
    typer.echo(f"Missing assets:           {len(result.missing_assets)}")

    if result.completely_missing_items or result.missing_assets:
        raise typer.Exit(1)


def main() -> None:
    """Entry point for the CLI."""
    app()
