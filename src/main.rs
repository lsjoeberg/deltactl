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
    dbg!("{:?}", &cli);

    match cli.cmd {
        Command::Compact { location } => {
            delta::compact(location.url).await.unwrap();
        }
        Command::ZOrder { location, columns } => {
            delta::zorder(location.url, columns).await.unwrap();
        }
        Command::Vacuum { location } => {
            delta::vacuum(location.url).await.unwrap();
        }
        Command::Schema { location } => {
            delta::schema(location.url).await.unwrap();
        }
    }
}
