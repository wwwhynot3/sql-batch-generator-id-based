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

    pub fn sleep_statement(self, seconds: u64) -> Option<String> {
        if seconds == 0 {
            return None;
        }

        match self {
            SqlDialectKind::MySql => Some(format!("DO SLEEP({seconds});")),
            SqlDialectKind::PostgreSql => Some(format!("SELECT pg_sleep({seconds});")),
            SqlDialectKind::MsSql => {
                let hours = seconds / 3600;
                let minutes = (seconds % 3600) / 60;
                let secs = seconds % 60;
                Some(format!(
                    "WAITFOR DELAY '{hours:02}:{minutes:02}:{secs:02}';"
                ))
            }
            SqlDialectKind::Snowflake => Some(format!("CALL SYSTEM$WAIT({seconds}, 'SECONDS');")),
            SqlDialectKind::Generic | SqlDialectKind::Sqlite | SqlDialectKind::DuckDb => None,
        }
    }

    pub fn sleep_unsupported_reason(self) -> Option<&'static str> {
        match self {
            SqlDialectKind::Generic => {
                Some("generic dialect has no portable sleep syntax; choose a concrete dialect")
            }
            SqlDialectKind::Sqlite => Some("sqlite has no built-in SQL sleep function"),
            SqlDialectKind::DuckDb => Some("duckdb has no built-in SQL sleep function"),
            _ => None,
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

#[cfg(test)]
mod tests {
    use super::SqlDialectKind;

    #[test]
    fn sleep_statement_is_disabled_when_seconds_is_zero() {
        assert_eq!(SqlDialectKind::MySql.sleep_statement(0), None);
    }

    #[test]
    fn mysql_sleep_statement_uses_sleep_function() {
        assert_eq!(
            SqlDialectKind::MySql.sleep_statement(2),
            Some("DO SLEEP(2);".to_string())
        );
    }

    #[test]
    fn mssql_sleep_statement_uses_waitfor_delay() {
        assert_eq!(
            SqlDialectKind::MsSql.sleep_statement(61),
            Some("WAITFOR DELAY '00:01:01';".to_string())
        );
    }

    #[test]
    fn sqlite_sleep_statement_is_unsupported() {
        assert_eq!(SqlDialectKind::Sqlite.sleep_statement(2), None);
        assert!(SqlDialectKind::Sqlite.sleep_unsupported_reason().is_some());
    }
}
