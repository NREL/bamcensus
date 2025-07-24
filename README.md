# BAMCensus - The Behavior and Advanced Mobility Census dataset aggregator
A tool for downloading large scale geospatial datasets written in Rust. 

This software is a high-performance tool developed in Rust for downloading and processing large-scale geospatial datasets, specifically focusing on US Census data. It is designed to address scaling limitations found in existing tools, such as R's [tidycensus](https://walker-data.com/tidycensus/), by providing performant streaming dataset JOIN operations between various US Census datasets (like ACS and LEHD) and their corresponding geometries stored on the TIGER/Lines web server.

The tool automates the process of joining these data sources, returning aggregated data to the user based on a specified census GEOID type. Its primary motivation stems from the need for a high-performance solution to combine spatial datasets with graph traversals within the context of mobility analysis tooling being developed at NREL's Behavior and Advanced Mobility (BAM) group.

Key features and design considerations include:

    - Extensible Algebraic Data Types (ADTs) for representing Census, ACS, LEHD, FIPS, and TIGRIS taxonomies.
    - The option to join census datasets with their geometry files, with optional typing provided by the Rust geo-types library.
    - A Rust API for programmatic use, offering lazy in-memory results.
    - A command-line interface (CLI) for batch downloading and file system downloads.

## Usage

See the [bamcensus-app](/bamcensus-app/) crate for command line usage.

## Roadmap

  - [ ] open source release of beta version
  - [ ] Python API
  - [ ] readthedocs documentation
  - [ ] R API
  - [ ] macro-driven compilation of annual data schemas
  - [ ] support for OD + RAC LEHD datasets

## License

Copyright 2025 Alliance for Sustainable Energy, LLC

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
