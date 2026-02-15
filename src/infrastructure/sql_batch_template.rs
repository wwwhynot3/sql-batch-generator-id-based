use anyhow::{Context, Result, anyhow};
use sqlparser::{
    ast::{BinaryOperator, Expr, Ident, SetExpr, Statement, TableFactor, Value, ValueWithSpan},
    dialect::{
        DuckDbDialect, GenericDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect,
        SQLiteDialect, SnowflakeDialect,
    },
    parser::Parser,
};

use crate::domain::sql_dialect::SqlDialectKind;

#[derive(Debug, Clone)]
pub struct SqlParserBatchTemplate {
    base_statement: Statement,
    qualified_primary_key_expr: Expr,
}

impl SqlParserBatchTemplate {
    pub fn parse(raw_sql: &str, dialect_kind: SqlDialectKind, primary_key: &str) -> Result<Self> {
        if raw_sql.trim().is_empty() {
            return Err(anyhow!("Input SQL must not be empty"));
        }

        let statement = parse_single_statement(raw_sql, dialect_kind)?;

        let table_alias = extract_main_table_alias(&statement);
        let qualified_primary_key_expr = build_primary_key_expr(primary_key, table_alias)?;

        Ok(Self {
            base_statement: statement,
            qualified_primary_key_expr,
        })
    }

    pub fn render_for_range(&self, start_id: i128, end_id: i128) -> Result<String> {
        let batch_condition_expr = Expr::Between {
            expr: Box::new(self.qualified_primary_key_expr.clone()),
            negated: false,
            low: Box::new(Expr::Value(ValueWithSpan::from(Value::Number(
                start_id.to_string(),
                false,
            )))),
            high: Box::new(Expr::Value(ValueWithSpan::from(Value::Number(
                end_id.to_string(),
                false,
            )))),
        };

        let mut statement_for_batch = self.base_statement.clone();
        inject_batch_condition(&mut statement_for_batch, batch_condition_expr)?;
        Ok(statement_for_batch.to_string())
    }
}

fn parse_single_statement(raw_sql: &str, dialect_kind: SqlDialectKind) -> Result<Statement> {
    let statements = match dialect_kind {
        SqlDialectKind::Generic => Parser::parse_sql(&GenericDialect {}, raw_sql),
        SqlDialectKind::MySql => Parser::parse_sql(&MySqlDialect {}, raw_sql),
        SqlDialectKind::PostgreSql => Parser::parse_sql(&PostgreSqlDialect {}, raw_sql),
        SqlDialectKind::Sqlite => Parser::parse_sql(&SQLiteDialect {}, raw_sql),
        SqlDialectKind::MsSql => Parser::parse_sql(&MsSqlDialect {}, raw_sql),
        SqlDialectKind::Snowflake => Parser::parse_sql(&SnowflakeDialect {}, raw_sql),
        SqlDialectKind::DuckDb => Parser::parse_sql(&DuckDbDialect {}, raw_sql),
    }
    .context("Unable to parse SQL with selected dialect")?;

    let statement_count = statements.len();
    if statement_count != 1 {
        return Err(anyhow!(
            "Input SQL must contain exactly one statement, but got {}",
            statement_count
        ));
    }

    statements
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("Input SQL must contain exactly one statement"))
}

fn build_primary_key_expr(primary_key: &str, table_alias: Option<&str>) -> Result<Expr> {
    let trimmed_primary_key = primary_key.trim();
    if trimmed_primary_key.is_empty() {
        return Err(anyhow!("Primary key must not be empty"));
    }

    if trimmed_primary_key.contains('.') {
        let identifier_parts = trimmed_primary_key
            .split('.')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(Ident::new)
            .collect::<Vec<_>>();

        if identifier_parts.len() < 2 {
            return Err(anyhow!(
                "Qualified primary key must contain both table and column, for example: users.id"
            ));
        }

        return Ok(Expr::CompoundIdentifier(identifier_parts));
    }

    if let Some(alias_name) = table_alias {
        return Ok(Expr::CompoundIdentifier(vec![
            Ident::new(alias_name),
            Ident::new(trimmed_primary_key),
        ]));
    }

    Ok(Expr::Identifier(Ident::new(trimmed_primary_key)))
}

fn extract_main_table_alias(statement: &Statement) -> Option<&str> {
    match statement {
        Statement::Update(update_statement) => {
            extract_alias_from_table_factor(&update_statement.table.relation)
        }
        Statement::Delete(delete_statement) => match &delete_statement.from {
            sqlparser::ast::FromTable::WithFromKeyword(table) => table,
            sqlparser::ast::FromTable::WithoutKeyword(table) => table,
        }
        .first()
        .and_then(|table_with_joins| extract_alias_from_table_factor(&table_with_joins.relation)),
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => select.from.first().and_then(|table_with_joins| {
                extract_alias_from_table_factor(&table_with_joins.relation)
            }),
            _ => None,
        },
        _ => None,
    }
}

fn extract_alias_from_table_factor(table_factor: &TableFactor) -> Option<&str> {
    match table_factor {
        TableFactor::Table { alias, .. } => alias
            .as_ref()
            .map(|table_alias| table_alias.name.value.as_str()),
        _ => None,
    }
}

fn inject_batch_condition(statement: &mut Statement, batch_condition: Expr) -> Result<()> {
    match statement {
        Statement::Update(update_statement) => {
            merge_selection(&mut update_statement.selection, batch_condition);
            Ok(())
        }
        Statement::Delete(delete_statement) => {
            merge_selection(&mut delete_statement.selection, batch_condition);
            Ok(())
        }
        Statement::Query(query) => match query.body.as_mut() {
            SetExpr::Select(select) => {
                merge_selection(&mut select.selection, batch_condition);
                Ok(())
            }
            _ => Err(anyhow!(
                "Only SELECT statements with direct FROM clause are supported"
            )),
        },
        _ => Err(anyhow!(
            "Only UPDATE, DELETE and SELECT statements are currently supported"
        )),
    }
}

fn merge_selection(existing_selection: &mut Option<Expr>, batch_condition: Expr) {
    *existing_selection = Some(match existing_selection.take() {
        Some(previous_condition) => Expr::BinaryOp {
            left: Box::new(batch_condition),
            op: BinaryOperator::And,
            right: Box::new(Expr::Nested(Box::new(previous_condition))),
        },
        None => batch_condition,
    });
}

#[cfg(test)]
mod tests {
    use crate::domain::sql_dialect::SqlDialectKind;

    use super::SqlParserBatchTemplate;

    #[test]
    fn uses_alias_when_primary_key_has_no_table() {
        let template = SqlParserBatchTemplate::parse(
            "UPDATE users u SET active = 0 WHERE status = 'old'",
            SqlDialectKind::Generic,
            "id",
        )
        .expect("template should be parsed");

        let sql = template
            .render_for_range(1, 50)
            .expect("sql should be rendered");

        assert_eq!(
            sql,
            "UPDATE users u SET active = 0 WHERE u.id BETWEEN 1 AND 50 AND (status = 'old')"
        );
    }

    #[test]
    fn keeps_qualified_primary_key_without_alias_rewrite() {
        let template = SqlParserBatchTemplate::parse(
            "DELETE FROM users u WHERE u.status = 'old'",
            SqlDialectKind::Generic,
            "users.id",
        )
        .expect("template should be parsed");

        let sql = template
            .render_for_range(10, 20)
            .expect("sql should be rendered");

        assert_eq!(
            sql,
            "DELETE FROM users u WHERE users.id BETWEEN 10 AND 20 AND (u.status = 'old')"
        );
    }

    #[test]
    fn prefixes_unqualified_primary_key_with_main_alias_in_join_query() {
        let template = SqlParserBatchTemplate::parse(
            "SELECT u.id, o.id FROM users u JOIN orders o ON o.user_id = u.id WHERE o.state = 'paid'",
            SqlDialectKind::Generic,
            "id",
        )
        .expect("template should be parsed");

        let sql = template
            .render_for_range(100, 199)
            .expect("sql should be rendered");

        assert_eq!(
            sql,
            "SELECT u.id, o.id FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id BETWEEN 100 AND 199 AND (o.state = 'paid')"
        );
    }

    #[test]
    fn adds_where_clause_when_statement_has_no_selection() {
        let template =
            SqlParserBatchTemplate::parse("DELETE FROM users", SqlDialectKind::Generic, "id")
                .expect("template should be parsed");

        let sql = template
            .render_for_range(1, 10)
            .expect("sql should be rendered");

        assert_eq!(sql, "DELETE FROM users WHERE id BETWEEN 1 AND 10");
    }
}
