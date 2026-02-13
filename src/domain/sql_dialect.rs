use std::str::FromStr;

use anyhow::{Result, anyhow};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialectKind {
    Generic,
    MySql,
    PostgreSql,
    Sqlite,
    MsSql,
    Snowflake,
    DuckDb,
}

impl SqlDialectKind {
    pub const ALL: [SqlDialectKind; 7] = [
        SqlDialectKind::Generic,
        SqlDialectKind::MySql,
        SqlDialectKind::PostgreSql,
        SqlDialectKind::Sqlite,
        SqlDialectKind::MsSql,
        SqlDialectKind::Snowflake,
        SqlDialectKind::DuckDb,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SqlDialectKind::Generic => "generic",
            SqlDialectKind::MySql => "mysql",
            SqlDialectKind::PostgreSql => "postgres",
            SqlDialectKind::Sqlite => "sqlite",
            SqlDialectKind::MsSql => "mssql",
            SqlDialectKind::Snowflake => "snowflake",
            SqlDialectKind::DuckDb => "duckdb",
        }
    }
}

impl std::fmt::Display for SqlDialectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SqlDialectKind {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "generic" => Ok(SqlDialectKind::Generic),
            "mysql" => Ok(SqlDialectKind::MySql),
            "postgres" | "postgresql" => Ok(SqlDialectKind::PostgreSql),
            "sqlite" => Ok(SqlDialectKind::Sqlite),
            "mssql" | "sqlserver" | "sql_server" => Ok(SqlDialectKind::MsSql),
            "snowflake" => Ok(SqlDialectKind::Snowflake),
            "duckdb" | "duck_db" => Ok(SqlDialectKind::DuckDb),
            _ => Err(anyhow!(
                "Unsupported dialect: {value}. Available values: generic,mysql,postgres,sqlite,mssql,snowflake,duckdb"
            )),
        }
    }
}
