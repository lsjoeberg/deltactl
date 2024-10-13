use clap::{Parser, Subcommand};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use deltactl::delta;

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create a new Delta table.
    Create {
        /// URI pointing to the table storage.
        uri: String,
    },
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
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("{:?}", &args);

    match args.cmd {
        Command::Create { uri } => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ]));
            delta::create_table(uri, schema).await.unwrap();
        }
        Command::Optimize { uri } => {
            delta::optimize(uri).await.unwrap();
        },
        Command::Vacuum { uri } => {
            delta::vacuum(uri).await.unwrap();
        }
    }
}
