use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow};
use clap::{Parser, ValueEnum};
use console::style;
use dialoguer::{Editor, Input, Select, theme::ColorfulTheme};

use crate::{
    application::commands::GenerateBatchedSqlCommand,
    domain::sql_dialect::SqlDialectKind,
};

const DEFAULT_BATCH_SIZE: usize = 50_000;
const DEFAULT_OUTPUT: &str = "id_slice.sql";
const DEFAULT_PRIMARY_KEY: &str = "id";

#[derive(Debug, Parser)]
#[command(
    name = "sql-id-slicer",
    version,
    about = "Split one SQL into multiple primary-key-based batches"
)]
struct CliArgs {
    #[arg(long, short = 's')]
    start_id: Option<i128>,
    #[arg(long, short = 'e')]
    end_id: Option<i128>,
    #[arg(long, short = 'b', default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,
    #[arg(long, short = 'q', help = "Raw SQL text")]
    sql: Option<String>,
    #[arg(long, short = 'f', help = "Read SQL from file path")]
    sql_file: Option<PathBuf>,
    #[arg(long, short = 'o', default_value = DEFAULT_OUTPUT)]
    output: PathBuf,
    #[arg(long, short = 'k', default_value = DEFAULT_PRIMARY_KEY)]
    primary_key: String,
    #[arg(long, short = 'd', value_enum, default_value_t = CliDialect::Generic)]
    dialect: CliDialect,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliDialect {
    Generic,
    Mysql,
    Postgres,
    Sqlite,
    Mssql,
    Snowflake,
    Duckdb,
}

impl From<CliDialect> for SqlDialectKind {
    fn from(value: CliDialect) -> Self {
        match value {
            CliDialect::Generic => SqlDialectKind::Generic,
            CliDialect::Mysql => SqlDialectKind::MySql,
            CliDialect::Postgres => SqlDialectKind::PostgreSql,
            CliDialect::Sqlite => SqlDialectKind::Sqlite,
            CliDialect::Mssql => SqlDialectKind::MsSql,
            CliDialect::Snowflake => SqlDialectKind::Snowflake,
            CliDialect::Duckdb => SqlDialectKind::DuckDb,
        }
    }
}

pub fn collect_generate_command() -> Result<GenerateBatchedSqlCommand> {
    if env::args_os().len() == 1 {
        return collect_interactive_command();
    }
    collect_command_from_args(CliArgs::parse())
}

fn collect_command_from_args(args: CliArgs) -> Result<GenerateBatchedSqlCommand> {
    let start_id = args
        .start_id
        .ok_or_else(|| anyhow!("--start-id is required when using argument mode"))?;
    let end_id = args
        .end_id
        .ok_or_else(|| anyhow!("--end-id is required when using argument mode"))?;

    let raw_sql = read_sql_from_sources(args.sql, args.sql_file)?;
    let primary_key = ensure_non_empty_value(args.primary_key, "Primary key")?;

    Ok(GenerateBatchedSqlCommand {
        start_id,
        end_id,
        batch_size: args.batch_size,
        raw_sql,
        output_path: args.output,
        primary_key,
        dialect_kind: args.dialect.into(),
    })
}

fn collect_interactive_command() -> Result<GenerateBatchedSqlCommand> {
    let theme = ColorfulTheme::default();

    println!();
    println!(
        "{}",
        style(" SQL BATCH SLICER ")
            .black()
            .on_cyan()
            .bold()
            .underlined()
    );
    println!("{}", style("Split SQL safely by primary key range").dim());
    println!();

    let start_id: i128 = Input::with_theme(&theme)
        .with_prompt("Start ID")
        .interact_text()?;

    let end_id: i128 = Input::with_theme(&theme)
        .with_prompt("End ID")
        .validate_with(|value: &i128| {
            if *value < start_id {
                Err("End ID must be greater than or equal to Start ID")
            } else {
                Ok(())
            }
        })
        .interact_text()?;

    let batch_size: usize = Input::with_theme(&theme)
        .with_prompt("Batch size")
        .default(DEFAULT_BATCH_SIZE)
        .validate_with(|value: &usize| {
            if *value == 0 {
                Err("Batch size must be greater than 0")
            } else {
                Ok(())
            }
        })
        .interact_text()?;

    let primary_key: String = Input::with_theme(&theme)
        .with_prompt("Primary key column (supports table.column)")
        .default(DEFAULT_PRIMARY_KEY.to_string())
        .validate_with(|value: &String| {
            if value.trim().is_empty() {
                Err("Primary key must not be empty")
            } else {
                Ok(())
            }
        })
        .interact_text()?;

    let dialect_items = SqlDialectKind::ALL
        .iter()
        .map(|dialect| dialect.as_str())
        .collect::<Vec<_>>();
    let selected_dialect_index = Select::with_theme(&theme)
        .with_prompt("SQL dialect")
        .default(0)
        .items(&dialect_items)
        .interact()?;
    let dialect_kind = SqlDialectKind::ALL[selected_dialect_index];

    let source_options = ["Edit SQL in your editor", "Load SQL from file"];
    let source_index = Select::with_theme(&theme)
        .with_prompt("SQL source")
        .default(0)
        .items(&source_options)
        .interact()?;

    let raw_sql = if source_index == 0 {
        let edited_sql = Editor::new()
            .extension(".sql")
            .edit("")?
            .ok_or_else(|| anyhow!("No SQL input detected from editor"))?;
        ensure_non_empty_value(edited_sql, "Input SQL")?
    } else {
        let sql_file_path: String = Input::with_theme(&theme)
            .with_prompt("SQL file path")
            .interact_text()?;
        read_sql_file(Path::new(sql_file_path.trim()))?
    };

    let output_name: String = Input::with_theme(&theme)
        .with_prompt("Output file")
        .default(DEFAULT_OUTPUT.to_string())
        .interact_text()?;

    Ok(GenerateBatchedSqlCommand {
        start_id,
        end_id,
        batch_size,
        raw_sql,
        output_path: PathBuf::from(output_name.trim()),
        primary_key: primary_key.trim().to_string(),
        dialect_kind,
    })
}

fn read_sql_from_sources(sql: Option<String>, sql_file: Option<PathBuf>) -> Result<String> {
    match (sql, sql_file) {
        (Some(_), Some(_)) => Err(anyhow!("Please provide only one of --sql or --sql-file")),
        (Some(sql_text), None) => ensure_non_empty_value(sql_text, "Input SQL"),
        (None, Some(file_path)) => read_sql_file(&file_path),
        (None, None) => Err(anyhow!(
            "One of --sql or --sql-file is required when using argument mode"
        )),
    }
}

fn read_sql_file(path: &Path) -> Result<String> {
    let content = fs::read_to_string(path)
        .map_err(|error| anyhow!("Unable to read SQL file {}: {error}", path.display()))?;
    ensure_non_empty_value(content, "Input SQL")
}

fn ensure_non_empty_value(value: String, field_name: &str) -> Result<String> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field_name} must not be empty"));
    }
    Ok(value)
}
