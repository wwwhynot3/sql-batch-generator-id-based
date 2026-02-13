use std::{
    fs::File,
    io::{BufWriter, Write},
};

use anyhow::{Context, Result};

use crate::{
    application::commands::{GenerateBatchedSqlCommand, GenerateBatchedSqlResult},
    domain::id_batch::IdBatchSlicer,
    infrastructure::sql_batch_template::SqlParserBatchTemplate,
};

#[derive(Debug, Default)]
pub struct GenerateBatchedSqlUseCase;

impl GenerateBatchedSqlUseCase {
    pub fn execute(&self, command: GenerateBatchedSqlCommand) -> Result<GenerateBatchedSqlResult> {
        let id_batch_slicer =
            IdBatchSlicer::new(command.start_id, command.end_id, command.batch_size)?;

        let sql_template = SqlParserBatchTemplate::parse(
            &command.raw_sql,
            command.dialect_kind,
            &command.primary_key,
        )?;

        let output_file = File::create(&command.output_path).with_context(|| {
            format!("Unable to create file: {}", command.output_path.display())
        })?;
        let mut output_writer = BufWriter::new(output_file);

        let mut generated_batch_count = 0usize;
        for id_range in id_batch_slicer.iter_ranges() {
            let mut rendered_sql =
                sql_template.render_for_range(id_range.start_id, id_range.end_id)?;
            if !rendered_sql.trim_end().ends_with(';') {
                rendered_sql.push(';');
            }
            writeln!(output_writer, "{rendered_sql}")?;
            generated_batch_count += 1;
        }
        output_writer.flush()?;

        Ok(GenerateBatchedSqlResult {
            output_path: command.output_path,
            batch_count: generated_batch_count,
        })
    }
}
