mod application;
mod domain;
mod infrastructure;
mod interfaces;

use anyhow::Result;
use console::style;

use crate::application::use_cases::generate_batched_sql::GenerateBatchedSqlUseCase;
use crate::interfaces::cli::collect_generate_command;

fn main() -> Result<()> {
    let command = collect_generate_command()?;
    let use_case = GenerateBatchedSqlUseCase::default();

    println!("{}", style("Generating batched SQL...").cyan());
    let result = use_case.execute(command)?;

    println!(
        "{} {} ({} batches)",
        style("Generated SQL has been saved to").green(),
        style(result.output_path.display()).bold(),
        result.batch_count,
    );
    Ok(())
}
