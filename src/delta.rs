use chrono::Duration;
use deltalake::{operations::optimize::OptimizeType, DeltaOps, DeltaTableError};

pub async fn compact(uri: impl AsRef<str>) -> Result<(), DeltaTableError> {
    let table = deltalake::open_table(uri).await?;
    let ops = DeltaOps(table);

    // TODO: Configure optimization properties: `.with_...`
    let opt_builder = ops.optimize().with_type(OptimizeType::Compact);
    let (table, metrics) = opt_builder.await?;
    println!("optimized table: {table:#?}\n{metrics:#?}");

    Ok(())
}

pub async fn zorder(uri: impl AsRef<str>, columns: Vec<String>) -> Result<(), DeltaTableError> {
    let table = deltalake::open_table(uri).await?;
    let ops = DeltaOps(table);

    // TODO: Configure optimization properties: `.with_...`
    let opt_builder = ops.optimize().with_type(OptimizeType::ZOrder(columns));
    let (table, metrics) = opt_builder.await?;
    println!("optimized table: {table:#?}\n{metrics:#?}");

    Ok(())
}

pub struct VacuumOptions {
    pub enforce_retention: bool,
    pub retention_period: Option<Duration>,
    pub dry_run: bool,
}

pub async fn vacuum(uri: impl AsRef<str>, options: VacuumOptions) -> Result<(), DeltaTableError> {
    let table = deltalake::open_table(uri).await?;
    let ops = DeltaOps(table);

    // TODO: Allow control of commit behaviour.
    let mut builder = ops
        .vacuum()
        .with_enforce_retention_duration(options.enforce_retention)
        .with_dry_run(options.dry_run);
    if let Some(days) = options.retention_period {
        builder = builder.with_retention_period(days);
    }

    let (table, metrics) = builder.await?;
    println!("vacuumed table: {table:#?}\n{metrics:#?}");

    Ok(())
}

pub async fn schema(uri: impl AsRef<str>) -> Result<(), DeltaTableError> {
    let table = deltalake::open_table(uri).await?;

    match table.schema() {
        // TODO: Serialize to JSON instead of printing Rust types.
        Some(schema) => println!("schema: {schema:#?}"),
        None => println!("no metadata found in the log for table: {table:#?}"),
    }

    Ok(())
}
