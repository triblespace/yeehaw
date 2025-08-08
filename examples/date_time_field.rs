// # DateTime field example
//
// This example shows how the DateTime field can be used

use yeehaw::collector::TopDocs;
use yeehaw::query::QueryParser;
use yeehaw::schema::{DateOptions, Schema, INDEXED, STRING};
use yeehaw::{Index, IndexWriter, TantivyDocument};

fn main() -> yeehaw::Result<()> {
    // # Defining the schema
    let mut schema_builder = Schema::builder();
    let opts = DateOptions::from(INDEXED)
        .set_fast()
        .set_precision(yeehaw::schema::DateTimePrecision::Seconds);
    // Add `occurred_at` date field type
    let _occurred_at = schema_builder.add_date_field("occurred_at", opts);
    let event_type = schema_builder.add_text_field("event", STRING);
    let schema = schema_builder.build();

    // # Indexing documents
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;
    // The dates are passed as string in the RFC3339 format
    let doc = TantivyDocument::parse_json(
        &schema,
        r#"{
        "occurred_at": "2022-06-22T12:53:50.53Z",
        "event": "pull-request"
    }"#,
    )?;
    index_writer.add_document(doc)?;
    let doc = TantivyDocument::parse_json(
        &schema,
        r#"{
        "occurred_at": "2022-06-22T13:00:00.22Z",
        "event": "comment"
    }"#,
    )?;
    index_writer.add_document(doc)?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // # Search
    let query_parser = QueryParser::for_index(&index, vec![event_type]);
    {
        // Simple exact search on the date
        let query = query_parser.parse_query("occurred_at:\"2022-06-22T12:53:50.53Z\"")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(5))?;
        assert_eq!(count_docs.len(), 1);
    }
    {
        // Range query on the date field
        let query = query_parser
            .parse_query(r#"occurred_at:[2022-06-22T12:58:00Z TO 2022-06-23T00:00:00Z}"#)?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(4))?;
        assert_eq!(count_docs.len(), 1);
    }
    Ok(())
}
