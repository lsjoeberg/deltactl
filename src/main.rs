use clap::{Args, Parser, Subcommand};
use deltactl::delta;
use deltalake::{table::builder::ensure_table_uri, DeltaTableError};
use url::Url;

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    pub cmd: Command,
}

impl Cli {
    /// Get the URI argument passed to a subcommand.
    fn get_uri(&self) -> &Url {
        self.cmd.get_uri()
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Optimize a table with Compaction.
    Compact(CompactArgs),
    /// Optimize a table with Z-Ordering.
    #[clap(name = "zorder")]
    ZOrder(ZOrderArgs),
    /// Vacuum table files marked for removal.
    Vacuum(VacuumArgs),
    /// Print the schema of a table.
    Schema(EmptyArgs),
    /// Print the metadata for a table.
    Metadata(EmptyArgs),
}

impl Command {
    /// Get the required URI argument passed to any of the sub-commands.
    fn get_uri(&self) -> &Url {
        match self {
            Self::Compact(args) => &args.location.url,
            Self::ZOrder(args) => &args.location.url,
            Self::Vacuum(args) => &args.location.url,
            Self::Schema(args) | Self::Metadata(args) => &args.location.url,
        }
    }
}

#[derive(Debug, Args)]
struct CompactArgs {
    #[clap(flatten)]
    location: TableUri,
    #[clap(flatten)]
    options: OptimizeArgs,
}

#[derive(Debug, Args)]
struct ZOrderArgs {
    #[clap(flatten)]
    location: TableUri,
    /// Comma-separated list of columns to order on.
    #[arg(long, short, required = true, num_args = 1.., value_delimiter = ',')]
    columns: Vec<String>,
    #[clap(flatten)]
    options: OptimizeArgs,
}

#[derive(Debug, Args)]
struct OptimizeArgs {
    /// Target file size (bytes).
    #[arg(long)]
    target_size: Option<i64>,
    /// Max spill size (bytes).
    #[arg(long)]
    max_spill_size: Option<usize>,
    /// Max number of concurrent tasks.
    #[arg(long)]
    max_concurrent_tasks: Option<usize>,
    /// Whether to preserve insertion order within files.
    #[arg(long)]
    preserve_insertion_order: bool,
    /// Min commit interval; e.g. 2min
    ///
    /// Commit transaction incrementally, instead of a single commit.
    #[arg(long)]
    min_commit_interval: Option<humantime::Duration>,
    // TODO: Partition filters.
}

impl From<OptimizeArgs> for delta::OptimizeOptions {
    fn from(value: OptimizeArgs) -> Self {
        Self {
            target_size: value.target_size,
            max_spill_size: value.max_spill_size,
            max_concurrent_tasks: value.max_concurrent_tasks,
            preserve_insertion_order: Some(value.preserve_insertion_order), // clap opt is a flag
            min_commit_interval: value.min_commit_interval.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct VacuumArgs {
    #[clap(flatten)]
    location: TableUri,
    /// Override the default retention period for which files are deleted.
    #[arg(long)]
    pub retention_days: Option<u32>,
    /// Don't enforce the retention period.
    #[arg(long)]
    pub no_enforce_retention: bool,
    /// Only determine which files can be deleted.
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long)]
    pub print_files: bool,
}

#[derive(Debug, Args)]
struct EmptyArgs {
    #[clap(flatten)]
    location: TableUri,
}

#[derive(Debug, Clone, Args)]
struct TableUri {
    /// URI pointing to the table location.
    #[arg(value_name = "URI", value_parser = verify_uri)]
    url: Url,
}

fn verify_uri(input: &str) -> Result<Url, DeltaTableError> {
    // TODO: Register object store handlers per feature flags.
    // deltalake::azure::register_handlers(None);
    // deltalake::aws::register_handlers(None);
    let url = ensure_table_uri(input)?;
    Ok(url)
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let uri = cli.get_uri();
    let table = deltalake::open_table(uri).await?;

    match cli.cmd {
        Command::Compact(args) => {
            delta::compact(table, args.options.into()).await?;
        }
        Command::ZOrder(args) => {
            delta::zorder(table, args.columns, args.options.into()).await?;
        }
        Command::Vacuum(args) => {
            let retention_period = args
                .retention_days
                .map(|days| chrono::Duration::days(days.into()));
            let options = delta::VacuumOptions {
                enforce_retention: !args.no_enforce_retention,
                retention_period,
                dry_run: args.dry_run,
                print_files: args.print_files,
            };
            delta::vacuum(table, options).await?;
        }
        Command::Schema(_) => delta::schema(&table)?,
        Command::Metadata(_) => delta::metadata(&table)?,
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli).await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
