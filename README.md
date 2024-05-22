# US Census
A tool for downloading US Census datasets written in Rust. This project is in **alpha** phase of development and not intended for use.

### Features and Design Considerations

- define extensible ADTs for Census, ACS, LODES, FIPS, TIGRIS taxonomies
- option to join census datasets with their geometry files with optional typing via the rust geo-types library
- Rust API: programmatic use with lazy in-memory results 
- CLI: batch downloading tool with file system downloads

