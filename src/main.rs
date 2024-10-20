use clap::{Parser, Subcommand};

use deltactl::delta;

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Optimize an existing table.
    Optimize {
        /// URI pointing to the table storage.
        uri: String,
    },
    /// Vacuum an existing table.
    Vacuum {
        /// URI pointing to the table storage.
        uri: String,
    },
    /// Get schema of an existing table.
    Schema {
        /// URI pointing to the table storage.
        uri: String
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("{:?}", &args);

    match args.cmd {
        Command::Optimize { uri } => {
            delta::optimize(uri).await.unwrap();
        }
        Command::Vacuum { uri } => {
            delta::vacuum(uri).await.unwrap();
        }
        Command::Schema { uri } => {
            delta::schema(uri).await.unwrap();
        }
    }
}
