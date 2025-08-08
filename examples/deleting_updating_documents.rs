// # Deleting and Updating (?) documents
//
// This example explains how to delete and update documents.
// In fact there is actually no such thing as an update in yeehaw.
//
// To update a document, you need to delete a document and then reinsert
// its new version.
//
// ---
// Importing yeehaw...
use yeehaw::collector::TopDocs;
use yeehaw::query::TermQuery;
use yeehaw::schema::*;
use yeehaw::{doc, Index, IndexReader, IndexWriter};

// Helper to check whether a document with the given ISBN exists.
fn exists_doc_with_isbn(reader: &IndexReader, isbn_term: &Term) -> yeehaw::Result<bool> {
    let searcher = reader.searcher();
    let term_query = TermQuery::new(isbn_term.clone(), IndexRecordOption::Basic);
    let top_docs = searcher.search(&term_query, &TopDocs::with_limit(1))?;
    Ok(top_docs.first().is_some())
}

fn main() -> yeehaw::Result<()> {
    // # Defining the schema
    //
    // Check out the *basic_search* example if this makes
    // small sense to you.
    let mut schema_builder = Schema::builder();

    // Yeehaw does not really have a notion of primary id.
    // This may change in the future.
    //
    // Still, we can create a `isbn` field and use it as an id. This
    // field can be `u64` or a `text`, depending on your use case.
    // It just needs to be indexed.
    //
    // If it is `text`, let's make sure to keep it `raw` and let's avoid
    // running any text processing on it.
    // This is done by associating this field to the tokenizer named `raw`.
    // Rather than building our
    // [`TextOptions`](//docs.rs/yeehaw/~0/yeehaw/schema/struct.TextOptions.html) manually, We
    // use the `STRING` shortcut. `STRING` stands for indexed (without term frequency or positions)
    // and untokenized.
    //
    let isbn = schema_builder.add_text_field("isbn", STRING);
    let title = schema_builder.add_text_field("title", TEXT);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Let's add a couple of documents, for the sake of the example.
    let mut old_man_doc = TantivyDocument::default();
    old_man_doc.add_text(title, "The Old Man and the Sea");
    index_writer.add_document(doc!(
        isbn => "978-0099908401",
        title => "The old Man and the see"
    ))?;
    index_writer.add_document(doc!(
        isbn => "978-0140177398",
        title => "Of Mice and Men",
    ))?;
    index_writer.add_document(doc!(
       title => "Frankentein", //< Oops there is a typo here.
       isbn => "978-9176370711",
    ))?;
    index_writer.commit()?;
    let reader = index.reader()?;

    let frankenstein_isbn = Term::from_field_text(isbn, "978-9176370711");

    // Oops our frankenstein doc seems misspelled
    assert!(exists_doc_with_isbn(&reader, &frankenstein_isbn)?);

    // # Update = Delete + Insert
    //
    // Here we will want to update the typo in the `Frankenstein` book.
    //
    // Yeehaw does not handle updates directly, we need to delete
    // and reinsert the document.
    //
    // This can be complicated as it means you need to have access
    // to the entire document.
    //
    // To remove one of the document, we just call `delete_term`
    // on its id.
    //
    // Note that `yeehaw` does nothing to enforce the idea that
    // there is only one document associated with this id.
    //
    // Also you might have noticed that we apply the delete before
    // having committed. This does not matter really...
    index_writer.delete_term(frankenstein_isbn.clone());

    // We now need to reinsert our document without the typo.
    index_writer.add_document(doc!(
       title => "Frankenstein",
       isbn => "978-9176370711",
    ))?;

    // You are guaranteed that your clients will only observe your index in
    // the state it was in after a commit.
    // In this example, your search engine will at no point be missing the *Frankenstein* document.
    // Everything happened as if the document was updated.
    index_writer.commit()?;
    // We reload our searcher to make our change available to clients.
    reader.reload()?;

    // No more typo!
    assert!(exists_doc_with_isbn(&reader, &frankenstein_isbn)?);

    Ok(())
}
