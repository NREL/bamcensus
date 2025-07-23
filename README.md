# BAMSODA - The Behavior and Advanced Mobility Scalable Open Dataset Aggregator
A tool for downloading large scale geospatial datasets written in Rust. 

This software is a high-performance tool developed in Rust for downloading and processing large-scale geospatial datasets, specifically focusing on US Census data. It is designed to address limitations found in existing tools, such as R's [tidycensus](https://walker-data.com/tidycensus/), by providing performant streaming dataset JOIN operations between various US Census datasets (like ACS and LEHD) and their corresponding geometries stored on the TIGER/Lines web server.

The tool automates the process of joining these data sources, returning aggregated data to the user based on a specified census GEOID type. Its primary motivation stems from the need for a high-performance solution to combine spatial datasets with graph traversals within the context of mobility analysis tooling being developed at NREL's Behavior and Advanced Mobility (BAM) group.

Key features and design considerations include:

    - Extensible Algebraic Data Types (ADTs) for representing Census, ACS, LODES, FIPS, and TIGRIS taxonomies.
    - The option to join census datasets with their geometry files, with optional typing provided by the Rust geo-types library.
    - A Rust API for programmatic use, offering lazy in-memory results.
    - A command-line interface (CLI) for batch downloading and file system downloads.

## Usage

See the [bamsoda-app](/bamsoda-app/) crate for command line usage.

## Roadmap

  - [ ] open source release of beta version
  - [ ] Python API
  - [ ] readthedocs documentation
  - [ ] R API
  - [ ] macro-driven compilation of annual data schemas
  - [ ] support for OD + RAC LEHD datasets