use deltalake::{DeltaOps, DeltaTableError};

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
