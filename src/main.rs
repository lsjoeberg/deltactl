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
    Compact(EmptyArgs),
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
            Self::ZOrder(args) => &args.location.url,
            Self::Vacuum(args) => &args.location.url,
            Self::Compact(args) | Self::Schema(args) | Self::Metadata(args) => &args.location.url,
        }
    }
}

#[derive(Debug, Args)]
struct ZOrderArgs {
    #[clap(flatten)]
    location: TableUri,
    /// Comma-separated list of columns to order on.
    #[arg(long, short, required = true, num_args = 1.., value_delimiter = ',')]
    columns: Vec<String>,
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

async fn run(cli: Cli) -> Result<(), DeltaTableError> {
    let uri = cli.get_uri();
    let table = deltalake::open_table(uri).await?;

    match cli.cmd {
        Command::Compact(_) => delta::compact(table).await?,
        Command::ZOrder(args) => delta::zorder(table, args.columns).await?,
        Command::Vacuum(args) => {
            let retention_period = args
                .retention_days
                .map(|days| chrono::Duration::days(days.into()));
            let options = delta::VacuumOptions {
                enforce_retention: !args.no_enforce_retention,
                retention_period,
                dry_run: args.dry_run,
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
