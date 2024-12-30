# BAMSODA - The Behavior and Advanced Mobility Scalable Open Dataset Aggregator
A tool for downloading large scale geospatial datasets written in Rust. 

### Why

I have been developing mobility analysis tooling for NREL's BAMS group built on [RouteE Compass](github.com/NREL/routee-compass), a Rust-based route planner. I want to build on top of that a high-performance tool for combining spatial datasets with graph traversals, and for that to be performant, it should also run in Rust. Looking at my options for tooling to automate US Census downloads, I see mostly that there is R's [tidycensus](), which is a great tool for analysis but doesn't provide the streaming dataset JOIN operation I want to have between various US Census datasets and their corresponding geometries stored in the TIGER/Lines web server. This library attempts to simplify that process, automating the JOIN between sources such as ACS and LEHD with their shapefile data, returning to the user a vector of that data aggregated to some census GEOID type.

### Features and Design Considerations

- define extensible ADTs for Census, ACS, LODES, FIPS, TIGRIS taxonomies
- option to join census datasets with their geometry files with optional typing via the rust geo-types library
- Rust API: programmatic use with lazy in-memory results 
- CLI: batch downloading tool with file system downloads

