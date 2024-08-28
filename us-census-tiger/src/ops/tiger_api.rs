use crate::model::tiger_uri::TigerUri;
use crate::model::tiger_uri_builder::TigerUriBuilder;
use futures::StreamExt;
use geo_types::Geometry;
use rayon::prelude::*;
use reqwest::Client;
use shapefile::dbase::Record;
use shapefile::{dbase, Shape, ShapeReader};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};
use std::path::Path;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use us_census_core::model::identifier::geoid::Geoid;
use zip::ZipArchive;

async fn download(client: &Client, uri: &str, write_file: File) -> Result<(), String> {
    let mut async_file = tokio::fs::File::from(write_file);

    let mut response = client
        .get(uri)
        .send()
        .await
        .map_err(|e| format!("failure retrieving TIGER zip archive: {}", e))?
        .bytes_stream();

    while let Some(buf) = response.next().await {
        let item = buf.map_err(|e| format!("failed to buffer response: {}", e))?;
        tokio::io::copy(&mut item.as_ref(), &mut async_file)
            .await
            .map_err(|e| format!("failed to write response buffer: {}", e))?;
    }

    async_file.flush().await.map_err(|e| {
        format!(
            "error closing async write connection to temp zip file: {}",
            e
        )
    })?;
    Ok(())
}

fn get_zip_filename(archive: &ZipArchive<File>, suffix: &str) -> Result<String, String> {
    let shp_filename = archive
        .file_names()
        .find(|s| s.ends_with(suffix))
        .ok_or_else(|| format!("no files in archive have '{}' suffix", suffix))?;
    Ok(String::from(shp_filename))
}

fn zip_file_into_string(archive: &mut ZipArchive<File>, filename: &str) -> Result<Vec<u8>, String> {
    let mut contents = Vec::new();
    let mut zipfile = archive.by_name(filename).map_err(|e| {
        format!(
            "expected file {} cannot be retrieved by name from zip archive: {}",
            filename, e
        )
    })?;
    zipfile
        .read_to_end(&mut contents)
        .map_err(|e| format!("failure reading {} from zip archive: {}", filename, e))?;
    // let string =
    //     String::from_utf8(contents).map_err(|e| format!("failure parsing zip as utf-8: {}", e))?;
    Ok(contents)
}

fn create_shapefile_reader<'a>(
    shp_contents: &'a Vec<u8>,
    dbf_contents: &'a Vec<u8>,
) -> Result<shapefile::Reader<Cursor<&'a Vec<u8>>, Cursor<&'a Vec<u8>>>, String> {
    let shp_cursor = Cursor::new(shp_contents);
    let dbf_cursor = Cursor::new(dbf_contents);
    let shape_reader = ShapeReader::new(shp_cursor)
        .map_err(|e| format!("failure building shape reader: {}", e))?;
    let database_reader = dbase::Reader::new(dbf_cursor)
        .map_err(|e| format!("failure building dbf reader: {}", e))?;
    let reader: shapefile::Reader<Cursor<&Vec<u8>>, Cursor<&Vec<u8>>> =
        shapefile::Reader::new(shape_reader, database_reader);
    Ok(reader)
}

/// runs as many downloads of TIGER/Lines files as needed to cover
/// the target geoids.
pub async fn run<'a>(
    client: &Client,
    builder: &TigerUriBuilder,
    geoids: &[&Geoid],
) -> Result<Vec<Result<Vec<(Geoid, Geometry)>, String>>, String> {
    let uris = builder.create_uris(geoids)?;
    let run_results = uris
        .into_iter()
        .map(|tiger_uri| {
            let client = &client;
            async move {
                // todo:
                //   man, this is tough!
                //   somehow nothing was written to the NamedTempFile location via the download fn.

                let named_tmp = tempfile::NamedTempFile::new().map_err(|e| {
                    format!("failure creating temporary zip archive filepath: {}", e)
                })?;
                let read_path = named_tmp.path().to_path_buf().clone();

                let write_file = File::create(&read_path)
                    .map_err(|e| format!("failure creating temporary zip archive file: {}", e))?;
                // let write_file = File::create(named_tmp)
                // .map_err(|e| format!("failure creating temporary zip archive file: {}", e))?;
                download(client, &tiger_uri.uri, write_file).await?;

                println!("debugging here, path: {:?}", read_path.to_str());
                // todo!("blows up here because the file is missing, why is it missing?");

                // write_file.rewind();
                let read_file = File::open(&read_path).map_err(|e| {
                    format!("failure opening temporary zip archive file location: {}", e)
                })?;

                // load the archive from the temp file location
                let mut z = ZipArchive::new(read_file)
                    .map_err(|e| format!("failure reading temporary zip archive: {}", e))?;

                let shp_filename = get_zip_filename(&z, ".shp")?;
                let dbf_filename = get_zip_filename(&z, ".dbf")?;

                let mut shp_contents = zip_file_into_string(&mut z, &shp_filename)?;
                let mut dbf_contents = zip_file_into_string(&mut z, &dbf_filename)?;

                let mut reader = create_shapefile_reader(&mut shp_contents, &mut dbf_contents)?;
                let result = reader
                    .iter_shapes_and_records()
                    .map(|row| {
                        let shape_record = row.map_err(|e| {
                            format!("failure reading shapefile shape/record: {}", e)
                        })?;
                        into_geoid_and_geometry(shape_record, &tiger_uri)
                    })
                    .collect::<Result<Vec<_>, String>>()?;

                Ok(result)
            }
        })
        .collect::<Vec<_>>();
    let result = futures::future::join_all(run_results).await;
    // let result = uris
    //     .par_chunks(parallelism)
    //     .flat_map(|tiger_uris| {
    //         tiger_uris
    //             .into_iter()
    //             .map(|tiger_uri| {
    //                 let mut tmpfile = NamedTempFile::new().map_err(|e| {
    //                     format!("failure creating temporary zip archive file: {}", e)
    //                 })?;
    //                 // let temppath = tmpfile.into_temp_path();

    //                 let mut response = reqwest::blocking::get(&tiger_uri.uri)
    //                     .map_err(|e| format!("failure retrieving TIGER zip archive: {}", e))?;
    //                 response
    //                     .copy_to(&mut tmpfile)
    //                     .map_err(|e| format!("failure writing zip archive to temp file: {}", e))?;

    //                 tmpfile.rewind().map_err(|e| {
    //                     format!("failure working with temp zip archive file: {}", e)
    //                 })?;
    //                 let shape_records = shapefile::read(&tmpfile)
    //                     .map_err(|e| format!("failure reading tmp shapefile archive: {}", e))?;
    //                 let result = shape_records
    //                     .into_iter()
    //                     .map(|row| into_geoid_and_geometry(row, &tiger_uri))
    //                     .collect::<Result<Vec<_>, String>>()?;

    //                 Ok(result)
    //             })
    //             .collect::<Vec<Result<Vec<_>, String>>>()
    //     })
    //     .collect::<Vec<Result<Vec<_>, String>>>();

    Ok(result)
}

fn into_geoid_and_geometry(
    row: (Shape, Record),
    tiger_uri: &TigerUri,
) -> Result<(Geoid, Geometry), String> {
    let (shape, record) = row;
    let geometry: Geometry<f64> = shape
        .try_into()
        .map_err(|e| format!("could not convert shape into geometry. {}", e))?;
    let field_value = record.get(&tiger_uri.geoid_column_name).ok_or_else(|| {
        format!(
            "could not find expected column '{}' in shapefile",
            &tiger_uri.geoid_column_name
        )
    })?;
    let geoid = match field_value {
        dbase::FieldValue::Character(s) => match s {
            Some(geoid_string) => tiger_uri.geoid_type.geoid_from_string(geoid_string),
            None => Err(format!(
                "value at field '{}' is empty, should be a GEOID string",
                &tiger_uri.geoid_column_name
            )),
        },
        _ => Err(format!(
            "value at column '{}' is not valid GEOID, found '{}'",
            &tiger_uri.geoid_column_name, field_value
        )),
    }?;
    Ok((geoid, geometry))
}

// pub async fn run<'a>(
//     client: Client,
//     builder: &TigerUriBuilder,
//     geoids: &[&Geoid],
// ) -> Result<Vec<Result<Vec<(Geoid, Geometry<f64>)>, String>>, String> {
//     let uris = builder.create_uris(geoids)?;
//     let responses = uris.into_iter().map(|tiger_uri| {
//         // let client = &client;
//         // async move {
//         //     let response = reqwest::get(tiger_uri.uri).await?;
//         //     let mut tmpfile = tempfile::tempfile()
//         //         .map_err(|e| format!("failure creating temporary zip archive file: {}", e))?;
//         //     let mut stream = response
//         //         .bytes()
//         //         .await
//         //         .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))?
//         //         .copy(tmpfile);
//         //     // .compat()

//         //     // let mut reader = BufReader::new(stream);
//         //     let zip_archive_opt = zip::read::read_zipfile_from_stream(&mut stream)
//         //         .map_err(|e| format!("error reading zip formatted file {}: {}", uri, e))?;
//         //     let zip_archive =
//         //         zip_archive_opt.ok_or_else(|| String::from("no zip files found in archive"))?;
//         //     let shp_filename = zip_archive
//         //         .file_names()
//         //         .find(|s| s.ends_with(&suffix))
//         //         .ok_or_else(|| format!("no files in archive have '{}' suffix", suffix))?;
//         //     Ok(String::from(shp_filename))
//         // };

//         // println!("  getting resource {}", tiger_uri.uri);
//         // let res = reqwest::get(&tiger_uri.uri)
//         //     .map_err(|e| format!("failure getting GTFS archive: {}", e));

//         // let send_result = client
//         //     .get(&tiger_uri.uri)
//         //     .send()
//         //     .map_err(|e| format!("failure getting GTFS archive: {}", e))?;

//         // Ok(vec![])
//         // let client = &client;
//         // async move {
//         //     let mut tmp_file = tempfile::tempfile().unwrap();
//         //     let res = client
//         //         .get(&tiger_uri.uri)
//         //         .send()
//         //         .await
//         //         .map_err(|e| format!("client send error: {}", e))?;

//         //     // let stream = res
//         //     //     .text()
//         //     //     .await
//         //     //     .map_err(|e| format!("client response error: {}", e))?;
//         //     // println!("stream response is {} characters long", stream.len());
//         //     // println!("first few characters: {}", stream[0..50].to_string());

//         //     // // let shp_filename = get_zip_filename(&tiger_uri.uri, &stream, String::from(".shp"))?;
//         //     // // let dbf_filename = get_zip_filename(&tiger_uri.uri, &stream, String::from(".dbf"))?;
//         //     // let rows = get_rows(&tiger_uri, &stream, builder.geoid_shapefile_colname())?;
//         //     // Ok(rows)
//         //     todo!()
//         // }
//     });
//     // let result = future::join_all(responses).await;
//     // Ok(result)
//     todo!()
// }

// fn get_zip_filename(uri: &String, stream: &String, suffix: String) -> Result<String, String> {
//     let mut reader = BufReader::new(stream.as_bytes());

//     let zip_archive_opt = zip::read::read_zipfile_from_stream(&mut reader)
//         .map_err(|e| format!("error reading zip formatted file {}: {}", uri, e))?;
//     let zip_archive =
//         zip_archive_opt.ok_or_else(|| String::from("no zip files found in archive"))?;
//     let shp_filename = zip_archive
//         .file_names()
//         .find(|s| s.ends_with(&suffix))
//         .ok_or_else(|| format!("no files in archive have '{}' suffix", suffix))?;
//     Ok(String::from(shp_filename))
// }

// pub struct ShapeReaders<'a> {
//     shape_reader: shapefile::ShapeReader<Cursor<&'a [u8]>>,
//     database_reader: dbase::Reader<Cursor<&'a [u8]>>,
//     reader: shapefile::Reader<Cursor<&'a [u8]>, Cursor<&'a [u8]>>,
// }

// fn get_shape_reader<'a>(uri: &String, stream: &String) -> Result<ShapeReaders<'a>, String> {
//     let mut shp_file = String::new();
//     let mut dbf_file = String::new();
//     let mut string_reader = BufReader::new(stream.as_bytes());
//     loop {
//         match zip::read::read_zipfile_from_stream(&mut string_reader) {
//             Ok(Some(mut file)) => {
//                 if file.name().ends_with(".shp") {
//                     file.read_to_string(&mut shp_file)
//                         .map_err(|e| format!("failure reading .shp from zip archive: {}", e))?;
//                     // let shp_cursor = Cursor::new(file.read_to_end(buf));
//                     // let shp_reader = ShapeReader::new(shp_cursor)
//                     //     .map_err(|e| format!("failure building shape reader: {}", e))?;
//                 } else if file.name().ends_with(".dbf") {
//                     file.read_to_string(&mut dbf_file)
//                         .map_err(|e| format!("failure reading .dbf from zip archive: {}", e))?;
//                     // let dbf_cursor = Cursor::new(dbf_bytes.as_slice());
//                     // let dbf_reader = dbase::Reader::new(dbf_cursor)
//                     //     .map_err(|e| format!("failure building shape reader: {}", e))?;
//                 }
//             }
//             Ok(None) => break,
//             Err(e) => return Err(format!("failure reading next file in zip archive: {}", e)),
//         }
//     }
//     if shp_file.is_empty() {
//         return Err(format!(".shp file was empty in archive {}", uri));
//     } else if dbf_file.is_empty() {
//         return Err(format!(".dbf file was empty in archive {}", uri));
//     } else {
//         let shp_cursor = Cursor::new(shp_file.as_bytes());
//         let dbf_cursor = Cursor::new(dbf_file.as_bytes());
//         let shape_reader = ShapeReader::new(shp_cursor)
//             .map_err(|e| format!("failure building shape reader: {}", e))?;
//         let database_reader = dbase::Reader::new(dbf_cursor)
//             .map_err(|e| format!("failure building dbf reader: {}", e))?;
//         let reader = shapefile::Reader::new(shape_reader, database_reader);
//         let result = ShapeReaders {
//             shape_reader,
//             database_reader,
//             reader,
//         };
//         Ok(result)
//     }
// }

// ///
// fn get_rows(
//     tiger_uri: &TigerUri,
//     stream: &String,
//     // shp_filename: &String,
//     // dbf_filename: &String,
//     geoid_column_name: String,
// ) -> Result<Vec<(Geoid, Geometry<f64>)>, String> {
//     // // load shapes file
//     // let c1 = Cursor::new(stream.as_bytes());
//     // let mut z1 = zip::ZipArchive::new(c1)
//     //     .map_err(|e| format!("error reading zip formatted file {}: {}", tiger_uri.uri, e))?;
//     // let shp_file = z1.by_name(shp_filename).map_err(|e| {
//     //     format!(
//     //         "shape file {} not found, internal error: {}",
//     //         shp_filename, e
//     //     )
//     // })?;
//     // let shp_bytes: Vec<u8> = Vec::with_capacity(shp_file.size() as usize);
//     // let shp_cursor = Cursor::new(shp_bytes.as_slice());
//     // let shp_reader = ShapeReader::new(shp_cursor)
//     //     .map_err(|e| format!("failure building shape reader: {}", e))?;

//     // // load records file
//     // let c2 = Cursor::new(stream.as_bytes());
//     // let mut z2 = zip::ZipArchive::new(c2)
//     //     .map_err(|e| format!("error reading zip formatted file {}: {}", tiger_uri.uri, e))?;
//     // let dbf_file = z2.by_name(dbf_filename).map_err(|e| {
//     //     format!(
//     //         "database file {} not found, internal error: {}",
//     //         dbf_filename, e
//     //     )
//     // })?;
//     // let dbf_bytes: Vec<u8> = Vec::with_capacity(dbf_file.size() as usize);
//     // let dbf_cursor = Cursor::new(dbf_bytes.as_slice());
//     // let dbf_reader = dbase::Reader::new(dbf_cursor)
//     //     .map_err(|e| format!("failure building shape reader: {}", e))?;

//     // let mut reader = shapefile::Reader::new(shp_reader, dbf_reader);
//     // let result = reader.read().map_err(|e| format!("read error: {}", e))?;

//     let mut shp_file = String::new();
//     let mut dbf_file = String::new();
//     {
//         let mut c = Cursor::new(stream.as_bytes());
//         c.rewind()
//             .map_err(|e| format!("failure rewinding downloaded archive: {}", e))?;
//         let mut r = zip::read::ZipArchive::new(c)
//             .map_err(|e| format!("failure reading downloaded zip archive: {}", e))?;
//         println!("found the following filenames in this archive:");
//         for name in r.file_names() {
//             println!("  {}", name);
//         }
//         // let mut c = Cursor::new(stream.as_bytes());
//         // c.rewind()
//         //     .map_err(|e| format!("failure rewinding downloaded archive: {}", e))?;
//         let shp_file = r.by_name(&"tl_2020_us_county.shp").map_err(|e| {
//             format!(
//                 "shape file {} not found, internal error: {}",
//                 &"tl_2020_us_county.shp", e
//             )
//         })?;
//         let shp_bytes: Vec<u8> = Vec::with_capacity(shp_file.size() as usize);
//         let shp_cursor = Cursor::new(shp_bytes.as_slice());
//         let shp_reader = ShapeReader::new(shp_cursor)
//             .map_err(|e| format!("failure building shape reader: {}", e))?;
//     }
//     let mut string_reader = BufReader::new(stream.as_bytes());
//     loop {
//         match zip::read::read_zipfile_from_stream(&mut string_reader) {
//             Ok(Some(mut file)) => {
//                 let enclosed = file.enclosed_name().unwrap_or_else(|| PathBuf::new());
//                 let enclosed_str = enclosed.to_string_lossy();
//                 println!(
//                     "  found ZipFile with name '{}', enclosed name '{}' with size {} MiB",
//                     file.name(),
//                     enclosed_str,
//                     file.size() / 1024000
//                 );
//                 if file.name().ends_with(".shp") {
//                     println!("    adding .shp file");
//                     file.read_to_string(&mut shp_file)
//                         .map_err(|e| format!("failure reading .shp from zip archive: {}", e))?;
//                 } else if file.name().ends_with(".dbf") {
//                     println!("    adding .dbf file");
//                     file.read_to_string(&mut dbf_file)
//                         .map_err(|e| format!("failure reading .dbf from zip archive: {}", e))?;
//                 }
//             }
//             Ok(None) => break,
//             Err(e) => return Err(format!("failure reading next file in zip archive: {}", e)),
//         }
//     }
//     if shp_file.is_empty() {
//         return Err(format!(".shp file was empty in archive {}", tiger_uri.uri));
//     } else if dbf_file.is_empty() {
//         return Err(format!(".dbf file was empty in archive {}", tiger_uri.uri));
//     } else {
//         let shp_cursor = Cursor::new(shp_file.as_bytes());
//         let dbf_cursor = Cursor::new(dbf_file.as_bytes());
//         let shape_reader = ShapeReader::new(shp_cursor)
//             .map_err(|e| format!("failure building shape reader: {}", e))?;
//         let database_reader = dbase::Reader::new(dbf_cursor)
//             .map_err(|e| format!("failure building dbf reader: {}", e))?;
//         let mut reader = shapefile::Reader::new(shape_reader, database_reader);

//         let result = reader
//             .iter_shapes_and_records()
//             .map(|row| {
//                 let (shape, record) = row.map_err(|e| format!("read error: {}", e))?;
//                 let geometry: Geometry<f64> = shape
//                     .try_into()
//                     .map_err(|e| format!("could not convert shape into geometry. {}", e))?;
//                 let field_value = record.get(&geoid_column_name).ok_or_else(|| {
//                     format!(
//                         "could not find expected column '{}' in shapefile",
//                         geoid_column_name
//                     )
//                 })?;
//                 let geoid = match field_value {
//                     dbase::FieldValue::Character(s) => match s {
//                         Some(geoid_string) => tiger_uri.geoid_type.geoid_from_string(geoid_string),
//                         None => Err(format!(
//                             "value at field '{}' is empty, should be a GEOID string",
//                             geoid_column_name
//                         )),
//                     },
//                     // dbase::FieldValue::Numeric(_) => todo!(),
//                     _ => Err(format!(
//                         "value at column '{}' is not valid GEOID, found '{}'",
//                         geoid_column_name, field_value
//                     )),
//                 }?;
//                 Ok((geoid, geometry))
//             })
//             .collect::<Result<Vec<_>, String>>()?;
//         Ok(result)
//     }
// }
