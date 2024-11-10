use clap::{Args, Parser, Subcommand};
use deltactl::delta;
use deltalake::{table::builder::ensure_table_uri, DeltaTableError};
use url::Url;

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Optimize a table with Compaction.
    Compact {
        #[clap(flatten)]
        location: TableUri,
    },
    /// Optimize a table with Z-Ordering.
    #[clap(name = "zorder")]
    ZOrder {
        #[clap(flatten)]
        location: TableUri,

        /// Comma-separated list of columns to order on.
        #[arg(long, short, required = true, num_args = 1.., value_delimiter = ',')]
        columns: Vec<String>,
    },
    /// Vacuum table files marked for removal.
    Vacuum {
        #[clap(flatten)]
        location: TableUri,
        #[clap(flatten)]
        args: VacuumArgs,
    },
    /// Get the schema of a table.
    Schema {
        #[clap(flatten)]
        location: TableUri,
    },
}

#[derive(Debug, Clone, Args)]
struct TableUri {
    /// URI pointing to the table location.
    #[arg(last = true, value_name = "URI", value_parser = verify_uri)]
    url: Url,
}

#[derive(Debug, Clone, Args)]
pub struct VacuumArgs {
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

fn verify_uri(input: &str) -> Result<Url, DeltaTableError> {
    // TODO: Register object store handlers per feature flags.
    // deltalake::azure::register_handlers(None);
    // deltalake::aws::register_handlers(None);
    let url = ensure_table_uri(input)?;
    Ok(url)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.cmd {
        Command::Compact { location } => delta::compact(location.url).await,
        Command::ZOrder { location, columns } => delta::zorder(location.url, columns).await,
        Command::Vacuum { location, args } => {
            let retention_period = args
                .retention_days
                .map(|days| chrono::Duration::days(days.into()));
            let options = delta::VacuumOptions {
                enforce_retention: !args.no_enforce_retention,
                retention_period,
                dry_run: args.dry_run,
            };
            delta::vacuum(location.url, options).await
        }
        Command::Schema { location } => delta::schema(location.url).await,
    };

    if let Err(err) = result {
        eprintln!("{err}");
    }
}
