#![warn(clippy::all, clippy::pedantic, clippy::nursery)]

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use deltactl::delta;
use deltalake::{table::builder::ensure_table_uri, DeltaTableError};
use std::collections::HashMap;
use url::Url;

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    pub cmd: Command,
    #[arg(long, short = 'o', number_of_values = 1, value_parser = parse_key_val)]
    pub storage_options: Option<Vec<(String, String)>>,
}

impl Cli {
    /// Get the URI argument passed to a subcommand.
    const fn get_uri(&self) -> &Url {
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
    /// Set table properties.
    Configure(ConfigureArgs),
    /// Create a new checkpoint at current table version.
    Checkpoint(EmptyArgs),
    /// Delete expired log files before current table version.
    ///
    /// The table log retention is based on the `logRetentionDuration`
    /// property of the table, 30 days by default.
    Expire(EmptyArgs),
    /// Print the schema of a table.
    Schema(EmptyArgs),
    /// Print the details for a table.
    ///
    /// This command will collect details of the table's current state,
    /// including version, the timestamp of the latest commit, table
    /// metadata, and protocol configuration.
    Details(EmptyArgs),
}

impl Command {
    /// Get the required URI argument passed to any of the sub-commands.
    const fn get_uri(&self) -> &Url {
        match self {
            Self::Compact(args) => &args.location.url,
            Self::ZOrder(args) => &args.location.url,
            Self::Vacuum(args) => &args.location.url,
            Self::Configure(args) => &args.location.url,
            Self::Checkpoint(args)
            | Self::Expire(args)
            | Self::Schema(args)
            | Self::Details(args) => &args.location.url,
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
    pub retention_period: Option<humantime::Duration>,
    /// Don't enforce the retention period.
    #[arg(long)]
    pub no_enforce_retention: bool,
    /// Only determine which files can be deleted.
    #[arg(long)]
    pub dry_run: bool,
    /// Whether to print deleted files.
    #[arg(long)]
    pub print_files: bool,
}

impl TryFrom<VacuumArgs> for delta::VacuumOptions {
    type Error = anyhow::Error;

    fn try_from(value: VacuumArgs) -> Result<Self, Self::Error> {
        // Convert the `std::time::Duration`, used for parsed arg, to `chrono::Duration`,
        // used in the delta API.
        let retention_period = value
            .retention_period
            .map(|d| chrono::Duration::from_std(*d).context("invalid retention period"))
            .transpose()?;

        Ok(Self {
            enforce_retention: !value.no_enforce_retention,
            retention_period,
            dry_run: value.dry_run,
            print_files: value.print_files,
        })
    }
}

#[derive(Debug, Clone, Args)]
pub struct ConfigureArgs {
    #[clap(flatten)]
    location: TableUri,
    /// Delta table key-value property pairs.
    ///
    /// Repeat the option for each pair: -p a=1 -p b=2
    ///
    #[allow(clippy::doc_markdown)]
    /// See properties at https://docs.delta.io/latest/table-properties.html
    #[clap(short, number_of_values = 1, value_parser = parse_key_val)]
    properties: Vec<(String, String)>,
}

fn parse_key_val(s: &str) -> Result<(String, String), anyhow::Error> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!("invalid KEY=value: no `=` found in `{}`", s))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
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
    #[cfg(feature = "azure")]
    deltalake::azure::register_handlers(None);

    #[cfg(feature = "s3")]
    deltalake::aws::register_handlers(None);

    let url = ensure_table_uri(input)?;
    Ok(url)
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let uri = cli.get_uri();

    let table = match &cli.storage_options {
        Some(v) => {
            let options = v.clone().into_iter().collect();
            deltalake::open_table_with_storage_options(uri, options).await?
        }
        None => deltalake::open_table(uri).await?,
    };

    match cli.cmd {
        Command::Compact(args) => {
            delta::compact(table, args.options.into()).await?;
        }
        Command::ZOrder(args) => {
            delta::zorder(table, args.columns, args.options.into()).await?;
        }
        Command::Vacuum(args) => {
            delta::vacuum(table, args.try_into()?).await?;
        }
        Command::Configure(args) => {
            let properties = args.properties.into_iter().collect::<HashMap<_, _>>();
            delta::set_properties(table, properties).await?;
        }
        Command::Checkpoint(_) => delta::create_checkpoint(&table).await?,
        Command::Expire(_) => delta::expire_logs(&table).await?,
        Command::Schema(_) => delta::schema(&table)?,
        Command::Details(_) => delta::details(&table).await?,
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
