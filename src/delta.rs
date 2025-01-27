use chrono::Duration;
use deltalake::{operations::optimize::OptimizeType, DeltaOps, DeltaTable, DeltaTableError};
use std::io::Write;

pub async fn compact(table: DeltaTable) -> Result<(), DeltaTableError> {
    let ops = DeltaOps(table);

    // TODO: Configure optimization properties: `.with_...`
    let opt_builder = ops.optimize().with_type(OptimizeType::Compact);
    let (table, metrics) = opt_builder.await?;
    println!(
        "compact operation complete for table: '{}'",
        table.table_uri()
    );
    println!("{}", serde_json::to_string_pretty(&metrics)?);

    Ok(())
}

pub async fn zorder(table: DeltaTable, columns: Vec<String>) -> Result<(), DeltaTableError> {
    let ops = DeltaOps(table);

    // TODO: Configure optimization properties: `.with_...`
    let opt_builder = ops.optimize().with_type(OptimizeType::ZOrder(columns));
    let (table, metrics) = opt_builder.await?;

    println!(
        "zorder operation complete for table: '{}'",
        table.table_uri()
    );
    println!("{}", serde_json::to_string_pretty(&metrics)?);

    Ok(())
}

pub struct VacuumOptions {
    pub enforce_retention: bool,
    pub retention_period: Option<Duration>,
    pub dry_run: bool,
    pub print_files: bool,
}

pub async fn vacuum(table: DeltaTable, options: VacuumOptions) -> Result<(), DeltaTableError> {
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

    println!(
        "vacuum operation complete for table: '{}'",
        table.table_uri()
    );
    println!("dry run: {}", metrics.dry_run);
    println!("files deleted: {}", metrics.files_deleted.len());

    if options.print_files {
        let mut stdout = std::io::stdout().lock();
        for file in metrics.files_deleted {
            writeln!(stdout, "{file}")?;
        }
    }

    Ok(())
}

pub fn schema(table: &DeltaTable) -> Result<(), DeltaTableError> {
    if let Some(schema) = table.schema() {
        println!("{}", serde_json::to_string_pretty(schema)?);
    }

    Ok(())
}

pub fn metadata(table: &DeltaTable) -> Result<(), DeltaTableError> {
    let metadata = table.metadata()?;
    println!("{}", serde_json::to_string_pretty(metadata)?);

    Ok(())
}
