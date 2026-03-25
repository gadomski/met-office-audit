# mspc-met-office-check

Audits the completeness of Met Office STAC items on the Microsoft Planetary Computer
against the source data in the Met Office S3 bucket.

## Usage

```
uv run met-office-audit MODEL THEME [--reference-datetime DATETIME] [--source SOURCE]
```

`MODEL` is one of `global` or `uk`. `THEME` is one of `height`, `pressure`,
`near-surface`, or `whole-atmosphere`.

If `--reference-datetime` is omitted, a forecast run is sampled at random from
those available in S3.

```bash
# Audit a random forecast run against the Planetary Computer STAC API
uv run met-office-audit global near-surface

# Audit a specific forecast run
uv run met-office-audit uk pressure --reference-datetime 2025-01-15T06:00Z

# Audit against the collection's stac-geoparquet archive instead of the API
uv run met-office-audit global near-surface --source geoparquet
```

`--source` is one of `api` (default) or `geoparquet`. The `geoparquet` option
downloads the collection's stac-geoparquet archive from the Planetary Computer
and queries it locally. The file is cached in `/tmp` and reused across
invocations in the same session.

> [!WARNING]
> The `geoparquet` source is not recommended at this time. The stac-geoparquet
> archives on the Planetary Computer for the Met Office Collections are currently 
> incomplete, which will produce misleading audit results.
> Use the default `api` source until this is resolved.

The command exits 0 if all items and assets are present, or 1 if anything is
missing. A summary is always printed to stdout:

```
Complete items:           142
Incomplete items:         0
Completely missing items: 0
Missing assets:           0
```
