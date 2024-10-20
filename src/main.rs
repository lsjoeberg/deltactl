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
    /// Optimize an existing table.
    Optimize {
        #[clap(flatten)]
        location: TableUri,
    },
    /// Vacuum an existing table.
    Vacuum {
        #[clap(flatten)]
        location: TableUri,
    },
    /// Get schema of an existing table.
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
        Command::Optimize { location } => {
            delta::optimize(location.url).await.unwrap();
        }
        Command::Vacuum { location } => {
            delta::vacuum(location.url).await.unwrap();
        }
        Command::Schema { location } => {
            delta::schema(location.url).await.unwrap();
        }
    }
}
