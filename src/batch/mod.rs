use std::{
    fmt::{self, Display},
    io::{self, Write},
};

use sqlparser::{
    ast::{
        BinaryOperator, Delete, Expr, FromTable, Ident, ObjectName, SelectItem, SetExpr, Statement,
        TableFactor, TableWithJoins, Value,
    },
    dialect::{
        Dialect, GenericDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect,
    },
    parser::Parser,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlDialectKind {
    #[default]
    Generic,
    MySql,
    Postgres,
    Sqlite,
    MsSql,
}

impl SqlDialectKind {
    fn with_dialect<T>(self, f: impl FnOnce(&dyn Dialect) -> T) -> T {
        match self {
            SqlDialectKind::Generic => {
                let dialect = GenericDialect {};
                f(&dialect)
            }
            SqlDialectKind::MySql => {
                let dialect = MySqlDialect {};
                f(&dialect)
            }
            SqlDialectKind::Postgres => {
                let dialect = PostgreSqlDialect {};
                f(&dialect)
            }
            SqlDialectKind::Sqlite => {
                let dialect = SQLiteDialect {};
                f(&dialect)
            }
            SqlDialectKind::MsSql => {
                let dialect = MsSqlDialect {};
                f(&dialect)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchError {
    EmptySql,
    EmptyPrimaryKey,
    InvalidPrimaryKeyRange { start: i128, end: i128 },
    InvalidBatchSize(usize),
    ParseFailed(String),
    InvalidPrimaryKeyExpression(String),
    UnsupportedStatement,
    MultipleStatements,
    MissingDeleteTargetTable,
}

impl Display for BatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchError::EmptySql => f.write_str("Input SQL must not be empty"),
            BatchError::EmptyPrimaryKey => f.write_str("Primary key column must not be empty"),
            BatchError::InvalidPrimaryKeyRange { start, end } => {
                write!(
                    f,
                    "Invalid primary key range: start ({start}) > end ({end})"
                )
            }
            BatchError::InvalidBatchSize(value) => {
                write!(f, "Invalid batch size: {value}, it must be > 0")
            }
            BatchError::ParseFailed(message) => write!(f, "SQL parse failed: {message}"),
            BatchError::InvalidPrimaryKeyExpression(message) => {
                write!(
                    f,
                    "Primary key expression must be an identifier like `id` or `t2.id`: {message}"
                )
            }
            BatchError::UnsupportedStatement => {
                f.write_str("Only single UPDATE or DELETE statement is supported")
            }
            BatchError::MultipleStatements => {
                f.write_str("Please input exactly one UPDATE or DELETE statement")
            }
            BatchError::MissingDeleteTargetTable => {
                f.write_str("DELETE statement has no target table")
            }
        }
    }
}

impl std::error::Error for BatchError {}

type BatchResult<T> = Result<T, BatchError>;

/// 一次批处理任务的输入参数。
///
/// `raw_sql` 是用户原始 UPDATE/DELETE 语句，
/// 其余字段定义主键范围与每批大小。
#[derive(Debug, Clone)]
pub struct BatchRequest {
    raw_sql: String,
    primary_key: String,
    dialect: SqlDialectKind,
    start_pk: i128,
    end_pk: i128,
    batch_size: usize,
}

impl BatchRequest {
    pub fn new(
        raw_sql: String,
        primary_key: String,
        dialect: SqlDialectKind,
        start_pk: i128,
        end_pk: i128,
        batch_size: usize,
    ) -> Self {
        Self {
            raw_sql,
            primary_key,
            dialect,
            start_pk,
            end_pk,
            batch_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqlBatchJob {
    config: BatchConfig,
    statement: DmlStatement,
}

impl SqlBatchJob {
    /// 将外部输入转换为可执行的批处理任务。
    ///
    /// 这里会集中完成两件事：
    /// 1) 校验并构建分片配置；
    /// 2) 解析并约束 SQL 为单条 UPDATE/DELETE。
    pub fn from_request(request: BatchRequest) -> BatchResult<Self> {
        let config = BatchConfig::new(
            request.primary_key,
            request.dialect,
            request.start_pk,
            request.end_pk,
            request.batch_size,
        )?;
        let statement = DmlStatement::parse(&request.raw_sql, request.dialect)?;
        Ok(Self { config, statement })
    }

    pub fn render_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        // 逐个 ID 区间生成 SQL，边生成边写入，避免一次性占用过多内存。
        for range in self.config.batch_ranges() {
            let statement = self
                .statement
                .to_batched_statement(self.config.primary_key(), range)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
            writeln!(writer, "{statement};")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum PrimaryKey {
    Unqualified(Ident),
    Qualified(Vec<Ident>),
}

impl PrimaryKey {
    fn new(value: String, dialect: SqlDialectKind) -> BatchResult<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(BatchError::EmptyPrimaryKey);
        }
        Self::parse(trimmed, dialect)
    }

    fn parse(raw: &str, dialect: SqlDialectKind) -> BatchResult<Self> {
        // 通过解析 "SELECT <pk> FROM ..." 来复用 SQL parser 的语法校验能力，
        // 从而支持 `id` / `t1.id`，并拒绝函数、表达式等复杂输入。
        let wrapped_sql = format!("SELECT {raw} FROM __pk_probe");
        let mut statements = dialect
            .with_dialect(|dialect| Parser::parse_sql(dialect, &wrapped_sql))
            .map_err(|e| BatchError::InvalidPrimaryKeyExpression(e.to_string()))?;

        let Some(statement) = statements.pop() else {
            return Err(BatchError::InvalidPrimaryKeyExpression(
                "empty parse result".to_string(),
            ));
        };

        let expression = match statement {
            Statement::Query(query) => match &*query.body {
                SetExpr::Select(select) => match select.projection.first() {
                    Some(SelectItem::UnnamedExpr(expr)) if select.projection.len() == 1 => {
                        expr.clone()
                    }
                    _ => {
                        return Err(BatchError::InvalidPrimaryKeyExpression(
                            "invalid projection".to_string(),
                        ));
                    }
                },
                _ => {
                    return Err(BatchError::InvalidPrimaryKeyExpression(
                        "invalid query body".to_string(),
                    ));
                }
            },
            _ => {
                return Err(BatchError::InvalidPrimaryKeyExpression(
                    "invalid primary key input".to_string(),
                ));
            }
        };

        match expression {
            Expr::Identifier(ident) => Ok(Self::Unqualified(ident)),
            Expr::CompoundIdentifier(parts) => Ok(Self::Qualified(parts)),
            _ => Err(BatchError::InvalidPrimaryKeyExpression(
                "only identifier path is supported".to_string(),
            )),
        }
    }

    fn to_expr(&self, target_table: Option<&str>) -> Expr {
        // 未限定主键（如 `id`）在 JOIN 场景下自动补到目标表别名（如 `u.id`），
        // 已限定主键（如 `t2.id`）保持用户输入不变。
        match self {
            PrimaryKey::Unqualified(ident) => {
                if let Some(table_name) = target_table {
                    Expr::CompoundIdentifier(vec![Ident::new(table_name), ident.clone()])
                } else {
                    Expr::Identifier(ident.clone())
                }
            }
            PrimaryKey::Qualified(parts) => Expr::CompoundIdentifier(parts.clone()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BatchSize(usize);

impl BatchSize {
    fn new(value: usize) -> BatchResult<Self> {
        if value == 0 {
            return Err(BatchError::InvalidBatchSize(value));
        }
        Ok(Self(value))
    }

    fn as_i128(self) -> i128 {
        self.0 as i128
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IdRange {
    start: i128,
    end: i128,
}

impl IdRange {
    fn new(start: i128, end: i128) -> BatchResult<Self> {
        if start > end {
            return Err(BatchError::InvalidPrimaryKeyRange { start, end });
        }
        Ok(Self { start, end })
    }

    fn split_by(self, batch_size: BatchSize) -> impl Iterator<Item = IdRange> {
        let step = batch_size.as_i128();
        // 利用闭区间 + step_by 按批次切分，最后一段自动截断到 `self.end`。
        (self.start..=self.end)
            .step_by(batch_size.0)
            .map(move |start| {
                let end = (start + step - 1).min(self.end);
                IdRange { start, end }
            })
    }
}

#[derive(Debug, Clone)]
struct BatchConfig {
    primary_key: PrimaryKey,
    full_range: IdRange,
    batch_size: BatchSize,
}

impl BatchConfig {
    fn new(
        primary_key: String,
        dialect: SqlDialectKind,
        start_pk: i128,
        end_pk: i128,
        batch_size: usize,
    ) -> BatchResult<Self> {
        Ok(Self {
            primary_key: PrimaryKey::new(primary_key, dialect)?,
            full_range: IdRange::new(start_pk, end_pk)?,
            batch_size: BatchSize::new(batch_size)?,
        })
    }

    fn primary_key(&self) -> &PrimaryKey {
        &self.primary_key
    }

    fn batch_ranges(&self) -> impl Iterator<Item = IdRange> {
        self.full_range.split_by(self.batch_size)
    }
}

#[derive(Debug, Clone)]
struct DmlStatement {
    statement: Statement,
    target_table: Option<String>,
}

impl DmlStatement {
    fn parse(raw_sql: &str, dialect: SqlDialectKind) -> BatchResult<Self> {
        if raw_sql.trim().is_empty() {
            return Err(BatchError::EmptySql);
        }

        let mut statements = dialect
            .with_dialect(|dialect| Parser::parse_sql(dialect, raw_sql))
            .map_err(|e| BatchError::ParseFailed(e.to_string()))?;

        if statements.len() != 1 {
            return Err(BatchError::MultipleStatements);
        }

        // 仅允许单条 UPDATE/DELETE，确保后续 WHERE 注入行为确定且可预测。
        let statement = statements.remove(0);
        match statement {
            Statement::Update { .. } | Statement::Delete(_) => {
                let target_table = Self::resolve_target_table(&statement)?;
                Ok(Self {
                    statement,
                    target_table,
                })
            }
            _ => Err(BatchError::UnsupportedStatement),
        }
    }

    fn to_batched_statement(
        &self,
        primary_key: &PrimaryKey,
        range: IdRange,
    ) -> BatchResult<Statement> {
        // 每次按区间构建 "pk BETWEEN x AND y"，并注入到原语句。
        let batch_condition = self.build_batch_condition(primary_key, range);
        let mut statement = self.statement.clone();
        Self::inject_batch_condition(&mut statement, batch_condition)?;
        Ok(statement)
    }

    fn build_batch_condition(&self, primary_key: &PrimaryKey, range: IdRange) -> Expr {
        Expr::Between {
            expr: Box::new(primary_key.to_expr(self.target_table.as_deref())),
            negated: false,
            low: Box::new(Expr::value(Value::Number(range.start.to_string(), false))),
            high: Box::new(Expr::value(Value::Number(range.end.to_string(), false))),
        }
    }

    fn inject_batch_condition(statement: &mut Statement, batch_condition: Expr) -> BatchResult<()> {
        match statement {
            Statement::Update {
                selection, limit, ..
            } => {
                // UPDATE: 合并 WHERE，移除 LIMIT，避免原 LIMIT 影响每批语义。
                *selection = Some(Self::merge_selection(selection.take(), batch_condition));
                *limit = None;
                Ok(())
            }
            Statement::Delete(delete) => {
                // DELETE: 合并 WHERE，清理 ORDER BY / LIMIT，确保批次执行稳定。
                delete.selection = Some(Self::merge_selection(
                    delete.selection.take(),
                    batch_condition,
                ));
                delete.order_by.clear();
                delete.limit = None;
                Ok(())
            }
            _ => Err(BatchError::UnsupportedStatement),
        }
    }

    fn merge_selection(existing: Option<Expr>, batch_condition: Expr) -> Expr {
        match existing {
            // 新条件放在左侧，并将原有条件加括号，避免优先级改变原语义。
            Some(existing) => Expr::BinaryOp {
                left: Box::new(batch_condition),
                op: BinaryOperator::And,
                right: Box::new(Expr::Nested(Box::new(existing))),
            },
            None => batch_condition,
        }
    }

    fn resolve_target_table(statement: &Statement) -> BatchResult<Option<String>> {
        // 推导目标表别名/表名，用于将未限定主键自动定位到正确表。
        match statement {
            Statement::Update { table, .. } => {
                Ok(Self::target_name_from_table_factor(&table.relation))
            }
            Statement::Delete(Delete { tables, from, .. }) => {
                if let Some(name) = tables.first().and_then(Self::object_table_name) {
                    return Ok(Some(name));
                }
                let Some(first_from) = Self::from_table_items(from).first() else {
                    return Err(BatchError::MissingDeleteTargetTable);
                };
                Ok(Self::target_name_from_table_factor(&first_from.relation))
            }
            _ => Err(BatchError::UnsupportedStatement),
        }
    }

    fn from_table_items(from: &FromTable) -> &[TableWithJoins] {
        match from {
            FromTable::WithFromKeyword(items) => items,
            FromTable::WithoutKeyword(items) => items,
        }
    }

    fn target_name_from_table_factor(table_factor: &TableFactor) -> Option<String> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => alias
                .as_ref()
                .map(|item| item.name.value.clone())
                .or_else(|| Self::object_table_name(name)),
            _ => None,
        }
    }

    fn object_table_name(name: &ObjectName) -> Option<String> {
        name.0
            .iter()
            .rev()
            .find_map(|part| part.as_ident().map(|ident| ident.value.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_multiple_statements() {
        let request = BatchRequest::new(
            "UPDATE users SET active = 0; DELETE FROM users".to_string(),
            "id".to_string(),
            SqlDialectKind::Generic,
            1,
            100,
            50,
        );
        assert!(SqlBatchJob::from_request(request).is_err());
    }

    #[test]
    fn update_with_join_uses_update_alias() {
        let request = BatchRequest::new(
            "UPDATE users u JOIN departments d ON u.dept_id = d.id \
             SET u.active = 0 WHERE d.enabled = 1 LIMIT 100"
                .to_string(),
            "id".to_string(),
            SqlDialectKind::Generic,
            1,
            50,
            50,
        );
        let job = SqlBatchJob::from_request(request).expect("job should be created");
        let mut output = Vec::new();
        job.render_to(&mut output).expect("render should succeed");
        let sql = String::from_utf8(output).expect("output should be utf8");

        assert!(sql.contains("u.id BETWEEN 1 AND 50"));
        assert!(sql.contains("AND (d.enabled = 1)"));
        assert!(!sql.contains("LIMIT"));
    }

    #[test]
    fn delete_with_multi_table_uses_delete_target() {
        let request = BatchRequest::new(
            "DELETE t1 FROM users t1 JOIN profiles t2 ON t1.id = t2.user_id \
             WHERE t2.blocked = 1 LIMIT 20"
                .to_string(),
            "id".to_string(),
            SqlDialectKind::Generic,
            10,
            19,
            10,
        );
        let job = SqlBatchJob::from_request(request).expect("job should be created");
        let mut output = Vec::new();
        job.render_to(&mut output).expect("render should succeed");
        let sql = String::from_utf8(output).expect("output should be utf8");

        assert!(sql.contains("t1.id BETWEEN 10 AND 19"));
        assert!(!sql.contains("LIMIT"));
    }

    #[test]
    fn last_batch_range_is_capped() {
        let request = BatchRequest::new(
            "UPDATE users SET active = 0".to_string(),
            "id".to_string(),
            SqlDialectKind::Generic,
            1,
            105,
            50,
        );
        let job = SqlBatchJob::from_request(request).expect("job should be created");
        let mut output = Vec::new();
        job.render_to(&mut output).expect("render should succeed");
        let sql = String::from_utf8(output).expect("output should be utf8");
        let lines: Vec<&str> = sql.lines().collect();

        assert_eq!(lines.len(), 3);
        assert!(sql.contains("id BETWEEN 1 AND 50"));
        assert!(sql.contains("id BETWEEN 51 AND 100"));
        assert!(sql.contains("id BETWEEN 101 AND 105"));
    }

    #[test]
    fn qualified_primary_key_keeps_input_qualifier() {
        let request = BatchRequest::new(
            "UPDATE users t1 JOIN user_ext t2 ON t1.id = t2.user_id \
             SET t1.active = 0 WHERE t2.status = 1"
                .to_string(),
            "t2.id".to_string(),
            SqlDialectKind::Generic,
            1,
            10,
            10,
        );
        let job = SqlBatchJob::from_request(request).expect("job should be created");
        let mut output = Vec::new();
        job.render_to(&mut output).expect("render should succeed");
        let sql = String::from_utf8(output).expect("output should be utf8");

        assert!(sql.contains("t2.id BETWEEN 1 AND 10"));
        assert!(!sql.contains("t1.t2.id"));
    }
}
