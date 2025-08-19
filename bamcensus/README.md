# Bamcensus

Executables that pull together the bamcensus crates. Can be used:
  1. as a command line application (CLI) - see installation below
  2. programmatically from Rust code - 

## Installation

Build these command line applications using Cargo (for example, via [rustup](https://rustup.rs/)):

```
$ cargo build -r
```

## Usage

### `acs_tiger_app`

This binary queries ACS data and joins it with TIGER/Line geometries for specified GEOIDs.

**Arguments:**
- `--geoids` (required): Comma-separated list of GEOIDs.
- `--output-resolution`: Geospatial resolution for output (e.g., tract, county).
- `--year` (required): Year of ACS/TIGER data.
- `--acs-query` (required): Comma-separated ACS columns to retrieve.
- `--acs-type` (required): One or five year estimates.
- `--acs-token`: Optional API token for the ACS API.
- `--output-file`: Output file path (default: auto-generated).

**Example:**
```sh
./target/release/acs_tiger_app --geoids=08031,08059 --year=2020 --acs-query=NAME,B01001_001E --acs-type=five-year --output-resolution=census-tract --output-file=output.csv
```

---

### `lodes_tiger_app`

This binary queries LODES data and joins it with TIGER/Line geometries.

**Subcommands:**
- `wac`: Workplace Area Characteristics (WAC) data.

**WAC Arguments:**
- `--geoids`: Comma-separated list of GEOIDs (optional, defaults to all states).
- `--output-resolution`: Geospatial wildcard (e.g., county).
- `--year` (required): Year of LODES data.
- `--wac-segments` (required): Comma-separated WAC segments.
- `--edition`: LODES edition (optional).
- `--segment`: Workforce segment (optional).
- `--jobtype`: Job type (optional).

**Example:**
```sh
./target/release/lodes_tiger_app wac --geoids=08031 --year=2020 --output-resolution=census-tract
```

