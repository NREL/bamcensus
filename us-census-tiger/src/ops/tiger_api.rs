use crate::model::{tiger_uri::TigerUri, tiger_uri_builder::TigerUriBuilder};
use futures::future;
use geo_types::Geometry;
use reqwest::Client;
use shapefile::dbase;
use shapefile::reader::ShapeReader;
use std::io::Cursor;
use us_census_core::model::identifier::geoid::Geoid;
use zip;

pub async fn run<'a>(
    client: &Client,
    builder: &TigerUriBuilder,
    geoids: &[&Geoid],
) -> Result<Vec<Result<Vec<(Geoid, Geometry<f64>)>, String>>, String> {
    let uris = builder.create_uris(geoids)?;
    let responses = uris.into_iter().map(|tiger_uri| {
        let client = &client;
        async move {
            let res = client
                .get(&tiger_uri.uri)
                .send()
                .await
                .map_err(|e| format!("client send error: {}", e))?;
            let stream = res
                .text()
                .await
                .map_err(|e| format!("client response error: {}", e))?;

            let shp_filename = get_zip_filename(&tiger_uri.uri, &stream, String::from(".shp"))?;
            let dbf_filename = get_zip_filename(&tiger_uri.uri, &stream, String::from(".dbf"))?;
            let rows = get_rows(
                &tiger_uri,
                &stream,
                &shp_filename,
                &dbf_filename,
                builder.geoid_shapefile_colname(),
            )?;
            Ok(rows)
        }
    });
    let result = future::join_all(responses).await;
    Ok(result)
}

fn get_zip_filename(uri: &String, stream: &String, suffix: String) -> Result<String, String> {
    let cursor = Cursor::new(stream.as_bytes());
    let zip_archive = zip::ZipArchive::new(cursor)
        .map_err(|e| format!("error reading zip formatted file {}: {}", uri, e))?;
    let shp_filename = zip_archive
        .file_names()
        .find(|s| s.ends_with(&suffix))
        .ok_or_else(|| format!("no files in archive have '{}' suffix", suffix))?;
    Ok(String::from(shp_filename))
}

///
fn get_rows(
    tiger_uri: &TigerUri,
    stream: &String,
    shp_filename: &String,
    dbf_filename: &String,
    geoid_column_name: String,
) -> Result<Vec<(Geoid, Geometry<f64>)>, String> {
    // load shapes file
    let c1 = Cursor::new(stream.as_bytes());
    let mut z1 = zip::ZipArchive::new(c1)
        .map_err(|e| format!("error reading zip formatted file {}: {}", tiger_uri.uri, e))?;
    let shp_file = z1.by_name(shp_filename).map_err(|e| {
        format!(
            "shape file {} not found, internal error: {}",
            shp_filename, e
        )
    })?;
    let shp_bytes: Vec<u8> = Vec::with_capacity(shp_file.size() as usize);
    let shp_cursor = Cursor::new(shp_bytes.as_slice());
    let shp_reader = ShapeReader::new(shp_cursor)
        .map_err(|e| format!("failure building shape reader: {}", e))?;

    // load records file
    let c2 = Cursor::new(stream.as_bytes());
    let mut z2 = zip::ZipArchive::new(c2)
        .map_err(|e| format!("error reading zip formatted file {}: {}", tiger_uri.uri, e))?;
    let dbf_file = z2.by_name(dbf_filename).map_err(|e| {
        format!(
            "database file {} not found, internal error: {}",
            dbf_filename, e
        )
    })?;
    let dbf_bytes: Vec<u8> = Vec::with_capacity(dbf_file.size() as usize);
    let dbf_cursor = Cursor::new(dbf_bytes.as_slice());
    let dbf_reader = dbase::Reader::new(dbf_cursor)
        .map_err(|e| format!("failure building shape reader: {}", e))?;

    let mut reader = shapefile::Reader::new(shp_reader, dbf_reader);
    // let result = reader.read().map_err(|e| format!("read error: {}", e))?;
    let result = reader
        .iter_shapes_and_records()
        .map(|row| {
            let (shape, record) = row.map_err(|e| format!("read error: {}", e))?;
            let geometry: Geometry<f64> = shape
                .try_into()
                .map_err(|e| format!("could not convert shape into geometry. {}", e))?;
            let field_value = record.get(&geoid_column_name).ok_or_else(|| {
                format!(
                    "could not find expected column '{}' in shapefile",
                    geoid_column_name
                )
            })?;
            let geoid = match field_value {
                dbase::FieldValue::Character(s) => match s {
                    Some(geoid_string) => tiger_uri.geoid_type.geoid_from_string(geoid_string),
                    None => Err(format!(
                        "value at field '{}' is empty, should be a GEOID string",
                        geoid_column_name
                    )),
                },
                // dbase::FieldValue::Numeric(_) => todo!(),
                _ => Err(format!(
                    "value at column '{}' is not valid GEOID, found '{}'",
                    geoid_column_name, field_value
                )),
            }?;
            Ok((geoid, geometry))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(result)
}
