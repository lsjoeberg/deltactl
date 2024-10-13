use deltalake::arrow::datatypes::Schema;
use deltalake::kernel::StructField;
use deltalake::{DeltaOps, DeltaTableError, TableProperty};
use std::sync::Arc;

fn arrow_schema_to_delta(schema: &Schema) -> Vec<StructField> {
    let res: Result<Vec<_>, _> = schema
        .fields
        .iter()
        .map(|f| StructField::try_from(f.as_ref()))
        .collect();
    res.unwrap() // error is infallible
}

pub async fn create_table(
    uri: impl AsRef<str>,
    schema: Arc<Schema>,
) -> Result<(), DeltaTableError> {
    let ops = DeltaOps::try_from_uri(uri).await?;

    // Convert Arrow schema to Delta.
    // TODO: Suitable in-mem representation of schema?
    //  Would be deserialized from e.g. JSON or YAML.
    //  NOTE: The Delta `StructField` type has serde to/from JSON.
    let cols = arrow_schema_to_delta(&schema);

    let table = ops
        .create()
        // TODO: Distinguish between URI, location, table name?
        .with_configuration_property(TableProperty::MinReaderVersion, Some("3"))
        .with_configuration_property(TableProperty::MinWriterVersion, Some("7"))
        .with_columns(cols)
        .await?;

    println!("Created table: {table:#?}");

    Ok(())
}

pub async fn optimize(uri: impl AsRef<str>) -> Result<(), DeltaTableError> {
    let table = deltalake::open_table(uri).await?;
    let ops = DeltaOps(table);
    
    // TODO: Configure optimization properties: `.with_...`
    let (table, metrics) = ops.optimize().await?;
    println!("optimized table: {table:#?}\n{metrics:#?}");

    Ok(())
}


pub async fn vacuum(uri: impl AsRef<str>) -> Result<(), DeltaTableError> {
    let table = deltalake::open_table(uri).await?;
    let ops = DeltaOps(table);

    // TODO: Configure optimization properties: `.with_...`
    let (table, metrics) = ops.vacuum().await?;
    println!("vacuumed table: {table:#?}\n{metrics:#?}");

    Ok(())
}
