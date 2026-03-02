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

        let output_file = File::create(&command.output_path)
            .with_context(|| format!("Unable to create file: {}", command.output_path.display()))?;
        let mut output_writer = BufWriter::new(output_file);
        let ranges = id_batch_slicer.iter_ranges().collect::<Vec<_>>();
        let sleep_statement = command.dialect_kind.sleep_statement(command.sleep_seconds);
        if command.sleep_seconds > 0
            && sleep_statement.is_none()
            && let Some(reason) = command.dialect_kind.sleep_unsupported_reason()
        {
            eprintln!(
                "Warning: sleep_seconds is set to {}, but dialect '{}' does not support SQL sleep; {}.",
                command.sleep_seconds, command.dialect_kind, reason
            );
        }

        let mut generated_batch_count = 0usize;
        for (index, id_range) in ranges.iter().enumerate() {
            let mut rendered_sql =
                sql_template.render_for_range(id_range.start_id, id_range.end_id)?;
            if !rendered_sql.trim_end().ends_with(';') {
                rendered_sql.push(';');
            }
            writeln!(output_writer, "{rendered_sql}")?;

            let has_next_batch = index + 1 < ranges.len();
            if has_next_batch && let Some(statement) = &sleep_statement {
                writeln!(output_writer, "{statement}")?;
            }

            generated_batch_count += 1;
        }
        output_writer.flush()?;

        Ok(GenerateBatchedSqlResult {
            output_path: command.output_path,
            batch_count: generated_batch_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        application::commands::GenerateBatchedSqlCommand, domain::sql_dialect::SqlDialectKind,
    };

    use super::GenerateBatchedSqlUseCase;

    fn build_temp_output_path() -> PathBuf {
        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("sql_id_slicer_use_case_{unique_suffix}.sql"))
    }

    #[test]
    fn inserts_sleep_between_batches_when_enabled() {
        let output_path = build_temp_output_path();
        let command = GenerateBatchedSqlCommand {
            start_id: 1,
            end_id: 3,
            batch_size: 1,
            sleep_seconds: 1,
            raw_sql: "DELETE FROM users".to_string(),
            output_path: output_path.clone(),
            primary_key: "id".to_string(),
            dialect_kind: SqlDialectKind::MySql,
        };

        let result = GenerateBatchedSqlUseCase
            .execute(command)
            .expect("use case should run");

        assert_eq!(result.batch_count, 3);
        let content = fs::read_to_string(&output_path).expect("output should be readable");
        assert!(content.contains("DELETE FROM users WHERE id BETWEEN 1 AND 1;"));
        assert!(content.contains("DELETE FROM users WHERE id BETWEEN 2 AND 2;"));
        assert!(content.contains("DELETE FROM users WHERE id BETWEEN 3 AND 3;"));
        assert_eq!(content.matches("DO SLEEP(1);").count(), 2);

        fs::remove_file(output_path).expect("temp output file should be removed");
    }

    #[test]
    fn does_not_insert_sleep_when_disabled() {
        let output_path = build_temp_output_path();
        let command = GenerateBatchedSqlCommand {
            start_id: 1,
            end_id: 2,
            batch_size: 1,
            sleep_seconds: 0,
            raw_sql: "DELETE FROM users".to_string(),
            output_path: output_path.clone(),
            primary_key: "id".to_string(),
            dialect_kind: SqlDialectKind::MySql,
        };

        GenerateBatchedSqlUseCase
            .execute(command)
            .expect("use case should run");
        let content = fs::read_to_string(&output_path).expect("output should be readable");
        assert!(!content.contains("SELECT SLEEP("));

        fs::remove_file(output_path).expect("temp output file should be removed");
    }

    #[test]
    fn does_not_insert_sleep_for_unsupported_dialect() {
        let output_path = build_temp_output_path();
        let command = GenerateBatchedSqlCommand {
            start_id: 1,
            end_id: 2,
            batch_size: 1,
            sleep_seconds: 1,
            raw_sql: "DELETE FROM users".to_string(),
            output_path: output_path.clone(),
            primary_key: "id".to_string(),
            dialect_kind: SqlDialectKind::Sqlite,
        };

        GenerateBatchedSqlUseCase
            .execute(command)
            .expect("use case should run");
        let content = fs::read_to_string(&output_path).expect("output should be readable");
        assert!(!content.contains("SLEEP("));
        assert!(!content.contains("pg_sleep("));
        assert!(!content.contains("WAITFOR DELAY"));
        assert!(!content.contains("SYSTEM$WAIT"));

        fs::remove_file(output_path).expect("temp output file should be removed");
    }
}
