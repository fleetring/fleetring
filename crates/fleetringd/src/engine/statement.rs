//! Statements, transactions, and their results, in a form every engine accepts.

/// A value bound to a statement parameter or read from a result row.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// No value.
    Null,
    /// A signed 64-bit integer.
    Integer(i64),
    /// A 64-bit floating-point number.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// Raw bytes.
    Blob(Vec<u8>),
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

/// One statement in the engine's language, with positional parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    sql: String,
    params: Vec<Value>,
}

impl Statement {
    /// A statement without parameters.
    #[must_use]
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    /// A statement with positional parameters, bound in order.
    #[must_use]
    pub fn with_params(sql: impl Into<String>, params: impl IntoIterator<Item = Value>) -> Self {
        Self {
            sql: sql.into(),
            params: params.into_iter().collect(),
        }
    }

    /// The statement's text.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The statement's parameters, in order.
    #[must_use]
    pub fn params(&self) -> &[Value] {
        &self.params
    }
}

/// Statements that commit together or not at all.
///
/// The engine runs the statements in one write transaction. If any of them fails, the
/// transaction rolls back and nothing is replicated. Statements must not begin, commit, or roll
/// back transactions themselves.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Transaction {
    statements: Vec<Statement>,
}

impl Transaction {
    /// A transaction running `statements` in order.
    #[must_use]
    pub fn new(statements: impl IntoIterator<Item = Statement>) -> Self {
        Self {
            statements: statements.into_iter().collect(),
        }
    }

    /// The statements, in order.
    #[must_use]
    pub fn statements(&self) -> &[Statement] {
        &self.statements
    }
}

impl From<Statement> for Transaction {
    fn from(statement: Statement) -> Self {
        Self::new([statement])
    }
}

/// The result of a committed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Executed {
    /// The rows the transaction's statements inserted, updated, or deleted.
    pub rows_affected: u64,
}

/// The result of a query: column names and rows of values.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Rows {
    /// The result's column names, in order.
    pub columns: Vec<String>,
    /// The result's rows, each with one value per column.
    pub rows: Vec<Vec<Value>>,
}
