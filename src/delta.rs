use deltalake::{
    operations::optimize::{OptimizeBuilder, OptimizeType},
    DeltaOps, DeltaTable, DeltaTableError,
};
use std::io::Write;

/// Supported options for `optimize` operations: [`compact`] and [`zorder`].
pub struct OptimizeOptions {
    pub target_size: Option<i64>,
    pub max_spill_size: Option<usize>,
    pub max_concurrent_tasks: Option<usize>,
    pub preserve_insertion_order: Option<bool>,
    pub min_commit_interval: Option<std::time::Duration>,
}

impl OptimizeOptions {
    /// Configure an [`OptimizeBuilder`] with non-`None` option values.
    fn configure(self, mut builder: OptimizeBuilder) -> OptimizeBuilder {
        if let Some(size) = self.target_size {
            builder = builder.with_target_size(size);
        }
        if let Some(max_spill_size) = self.max_spill_size {
            builder = builder.with_max_spill_size(max_spill_size);
        }
        if let Some(max_concurrent_tasks) = self.max_concurrent_tasks {
            builder = builder.with_max_concurrent_tasks(max_concurrent_tasks);
        }
        if let Some(preserve_insertion_order) = self.preserve_insertion_order {
            builder = builder.with_preserve_insertion_order(preserve_insertion_order);
        }
        if let Some(min_commit_interval) = self.min_commit_interval {
            builder = builder.with_min_commit_interval(min_commit_interval);
        }

        builder
    }
}

pub async fn compact(table: DeltaTable, options: OptimizeOptions) -> Result<(), DeltaTableError> {
    let ops = DeltaOps(table);

    let builder = ops.optimize().with_type(OptimizeType::Compact);
    let builder = options.configure(builder);

    let (table, metrics) = builder.await?;
    println!(
        "compact operation complete for table: '{}'",
        table.table_uri()
    );
    println!("{}", serde_json::to_string_pretty(&metrics)?);

    Ok(())
}

pub async fn zorder(
    table: DeltaTable,
    columns: Vec<String>,
    options: OptimizeOptions,
) -> Result<(), DeltaTableError> {
    let ops = DeltaOps(table);

    let builder = ops.optimize().with_type(OptimizeType::ZOrder(columns));
    let builder = options.configure(builder);

    let (table, metrics) = builder.await?;

    println!(
        "zorder operation complete for table: '{}'",
        table.table_uri()
    );
    println!("{}", serde_json::to_string_pretty(&metrics)?);

    Ok(())
}

pub struct VacuumOptions {
    pub enforce_retention: bool,
    pub retention_period: Option<chrono::Duration>,
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
    if let Some(retention_period) = options.retention_period {
        builder = builder.with_retention_period(retention_period);
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
