# US Census
A tool for downloading US Census datasets written in Rust. 

As of August 29th, 2024, this project is moving into **beta** phase of development.

### Features and Design Considerations

- define extensible ADTs for Census, ACS, LODES, FIPS, TIGRIS taxonomies
- option to join census datasets with their geometry files with optional typing via the rust geo-types library
- Rust API: programmatic use with lazy in-memory results 
- CLI: batch downloading tool with file system downloads

