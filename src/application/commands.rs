use std::path::PathBuf;

use crate::domain::sql_dialect::SqlDialectKind;

#[derive(Debug)]
pub struct GenerateBatchedSqlCommand {
    pub start_id: i128,
    pub end_id: i128,
    pub batch_size: usize,
    pub raw_sql: String,
    pub output_path: PathBuf,
    pub primary_key: String,
    pub dialect_kind: SqlDialectKind,
}

#[derive(Debug)]
pub struct GenerateBatchedSqlResult {
    pub output_path: PathBuf,
    pub batch_count: usize,
}
