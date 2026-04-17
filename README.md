# mspc-met-office-check

Check for missing netcdf files in the Microsoft Planetary Computer's (MSPC) Met Office collections.

## Usage

First, download the geoparquets:

```sh
uv run met-office-check download-stac-geoparquet global  # or uk
```

Then, check everything:

```sh
uv run met-office-check check-all global  # or uk
```

This will create a file named `check.parquet` that has one row for every netcdf that's missing from the MSPC STAC items.
