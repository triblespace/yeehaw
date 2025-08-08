use std::path::PathBuf;

use schema::*;

use crate::*;

fn create_index(path: &str) {
    let mut schema_builder = Schema::builder();
    let label = schema_builder.add_text_field("label", TEXT | STORED);
    let date = schema_builder.add_date_field("date", INDEXED | STORED);
    let schema = schema_builder.build();
    std::fs::create_dir_all(path).unwrap();
    let index = Index::create_in_dir(path, schema).unwrap();
    let mut index_writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
    index_writer
        .add_document(doc!(label => "dateformat", date => DateTime::from_timestamp_nanos(123456)))
        .unwrap();
    index_writer.commit().unwrap();
}

#[test]
/// Writes an Index for the current INDEX_FORMAT_VERSION to disk.
fn create_format() {
    let version = INDEX_FORMAT_VERSION.to_string();
    let file_path = path_for_version(&version);
    if PathBuf::from(file_path.clone()).exists() {
        return;
    }
    create_index(&file_path);
}

fn path_for_version(version: &str) -> String {
    format!("./tests/compat_tests_data/index_v{version}/")
}

#[test]
fn test_format_6() {
    let path = path_for_version("6");

    let index = Index::open_in_dir(path).expect("Failed to open index");
    // dates are truncated to Microseconds in v6
    assert_date_time_precision(&index, DateTimePrecision::Microseconds);
}

#[test]
fn test_format_7() {
    let path = path_for_version("7");

    let index = Index::open_in_dir(path).expect("Failed to open index");
    // dates are not truncated in v7 in the docstore
    assert_date_time_precision(&index, DateTimePrecision::Nanoseconds);
}

fn assert_date_time_precision(index: &Index, _doc_store_precision: DateTimePrecision) {
    use collector::TopDocs;
    let reader = index.reader().expect("Failed to create reader");
    let searcher = reader.searcher();

    let schema = index.schema();
    let label_field = schema.get_field("label").expect("Field 'label' not found");
    let query_parser = query::QueryParser::for_index(index, vec![label_field]);

    let query = query_parser
        .parse_query("dateformat")
        .expect("Failed to parse query");
    let top_docs = searcher
        .search(&query, &TopDocs::with_limit(1))
        .expect("Search failed");

    assert_eq!(top_docs.len(), 1, "Expected 1 search result");

    let _doc_address = top_docs[0].1;
}
