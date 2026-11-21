//! What the SQLite adapter knows of sqlite-vec's `vec0` tables beyond what SQLite enforces
//! (ADR-0049).
//!
//! A `vec0` table keeps its data in shadow tables named after it: `<table>_info`, `_chunks`,
//! `_rowids`, `_auxiliary`, `_metadatachunksNN`, `_metadatatextNN`, and `_vector_chunksNN`.
//! sqlite-vec trusts what it reads back from them, so they must hold only what it wrote. SQLite's
//! defensive mode keeps statements from writing a virtual table's shadow tables, but two gaps in
//! sqlite-vec 0.1.9 leave holes the adapter closes itself:
//!
//! - **SQLite doesn't know the vector chunk tables as shadow tables.** It finds a shadow table's
//!   virtual table by the name before the last `_`, and 0.1.9 leaves `vector_chunksNN` out of its
//!   list of shadow names besides. So the authorizer refuses a user statement that creates,
//!   writes, alters, drops, indexes, or puts a trigger on a table so named, or defines a view or
//!   virtual table so named, whenever SQLite prepares it as the user's statement or inside a
//!   trigger: vec0's own statements on them are prepared at the top level while the user's
//!   statement steps, never while it is prepared and never inside a trigger. The change recorder
//!   skips them, and the schema hides them, as for any shadow table.
//! - **0.1.9 has no `xRename`.** `ALTER TABLE … RENAME` of a `vec0` table renames the virtual
//!   table alone and leaves its shadow tables under the old name, as ordinary tables any
//!   statement may write. So after each user statement that changed the schema, [`Snapshot`]
//!   checks that no `vec0` table was renamed, and that no object was given a name that belongs to
//!   a `vec0` table's shadow tables, or a vector chunk table's name, except by vec0 creating a new
//!   table. The authorizer sees only a renamed table's old name, so only the schema left behind
//!   shows a rename.
//!
//! Which tables are `vec0` tables comes from their definitions: the module named after `USING`.

use std::collections::BTreeSet;

use rusqlite::Connection;
use rusqlite::hooks::AuthAction;

use super::storage;
use crate::engine::EngineError;

/// What follows a `vec0` table's name in the name of one of its vector chunk tables, before the
/// two digits of the vector column's index.
const CHUNK_INFIX: &str = "_vector_chunks";

/// The suffixes of a `vec0` table's shadow tables that carry no number.
const PLAIN_SUFFIXES: [&str; 4] = ["_info", "_chunks", "_rowids", "_auxiliary"];

/// The suffixes of a `vec0` table's shadow tables that end in a column's two-digit number.
const NUMBERED_SUFFIXES: [&str; 3] = [CHUNK_INFIX, "_metadatachunks", "_metadatatext"];

/// The shadow tables vec0 creates for every table, whatever its columns.
const ALWAYS_CREATED: [&str; 3] = ["_info", "_chunks", "_rowids"];

/// The name of the virtual table that `name` would be a vector chunk table of: `name` without
/// `_vector_chunks` and two ASCII digits at its end, in any ASCII case, if it ends so.
pub(super) fn chunk_table_owner(name: &str) -> Option<&str> {
    numbered_owner(name, CHUNK_INFIX)
}

/// `name` without `suffix` and two ASCII digits at its end, in any ASCII case, if it ends so and
/// something is left.
fn numbered_owner<'n>(name: &'n str, suffix: &str) -> Option<&'n str> {
    let (rest, digits) = name.split_at_checked(name.len().checked_sub(2)?)?;
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    plain_owner(rest, suffix)
}

/// `name` without `suffix` at its end, in any ASCII case, if it ends so and something is left.
fn plain_owner<'n>(name: &'n str, suffix: &str) -> Option<&'n str> {
    let (owner, end) = name.split_at_checked(name.len().checked_sub(suffix.len())?)?;
    (end.eq_ignore_ascii_case(suffix) && !owner.is_empty()).then_some(owner)
}

/// Every virtual table `name` would be a `vec0` shadow table of, by its suffix: one name can end
/// in more than one suffix only through an owner whose own name ends in one.
fn shadow_owners(name: &str) -> impl Iterator<Item = &str> {
    let plain = PLAIN_SUFFIXES
        .iter()
        .filter_map(move |suffix| plain_owner(name, suffix));
    let numbered = NUMBERED_SUFFIXES
        .iter()
        .filter_map(move |suffix| numbered_owner(name, suffix));
    plain.chain(numbered)
}

/// Whether `name` is a vector chunk table of one of `vec0_tables`, which hold lowercase names.
pub(super) fn is_chunk_table(name: &str, vec0_tables: &BTreeSet<String>) -> bool {
    chunk_table_owner(name).is_some_and(|owner| vec0_tables.contains(&owner.to_ascii_lowercase()))
}

/// Whether `action` touches a table, view, or virtual table named like a vector chunk table in a
/// way only vec0 may: creating, writing, altering, dropping, indexing, or putting a trigger on
/// it. Reading one is allowed: it holds only the user's own vectors.
pub(super) fn changes_chunk_table(action: &AuthAction<'_>) -> bool {
    let name = match *action {
        AuthAction::CreateTable { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::Insert { table_name }
        | AuthAction::Update { table_name, .. }
        | AuthAction::Delete { table_name }
        | AuthAction::AlterTable { table_name, .. }
        | AuthAction::CreateIndex { table_name, .. }
        | AuthAction::DropIndex { table_name, .. }
        | AuthAction::CreateTrigger { table_name, .. }
        | AuthAction::DropTrigger { table_name, .. }
        | AuthAction::CreateVtable { table_name, .. } => table_name,
        AuthAction::CreateView { view_name } => view_name,
        _ => return false,
    };
    chunk_table_owner(name).is_some()
}

/// One object in `main`'s schema.
#[derive(Debug)]
struct Object {
    /// `table`, `index`, `view`, or `trigger`, as `sqlite_schema` names it, or `virtual` for a
    /// virtual table.
    kind: String,
    /// Its name, in lowercase: SQLite's names ignore ASCII case.
    name: String,
    /// The module a virtual table uses, in lowercase.
    module: Option<String>,
}

/// Every object in `main`'s schema.
fn objects(conn: &Connection) -> Result<Vec<Object>, EngineError> {
    let mut statement = conn
        .prepare_cached("SELECT type, name, sql FROM main.sqlite_master")
        .map_err(storage)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(storage)?;
    let mut objects = Vec::new();
    for row in rows {
        let (kind, name, sql) = row.map_err(storage)?;
        let module = match kind.as_str() {
            "table" => sql.as_deref().and_then(module_of),
            _ => None,
        };
        let kind = if module.is_some() {
            "virtual".to_owned()
        } else {
            kind
        };
        objects.push(Object {
            kind,
            name: name.to_ascii_lowercase(),
            module,
        });
    }
    Ok(objects)
}

/// The `vec0` tables among `objects`, by lowercase name.
fn vec0_among(objects: &[Object]) -> BTreeSet<String> {
    objects
        .iter()
        .filter(|object| object.module.as_deref() == Some("vec0"))
        .map(|object| object.name.clone())
        .collect()
}

/// The `vec0` tables in `main`, by lowercase name.
pub(super) fn vec0_tables(conn: &Connection) -> Result<BTreeSet<String>, EngineError> {
    Ok(vec0_among(&objects(conn)?))
}

/// What a user statement may not change about `vec0` tables, read from the schema.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Snapshot {
    /// The `vec0` tables, by lowercase name.
    tables: BTreeSet<String>,
    /// Every object named like a shadow table of one of `tables`, or like a vector chunk table
    /// of anything: its kind and lowercase name.
    shadow_named: BTreeSet<(String, String)>,
}

/// Why a statement's schema change is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Violation {
    /// It renamed the `vec0` table so named.
    Renamed(String),
    /// It gave the object so named a name that belongs to a `vec0` table's shadow tables.
    ShadowName(String),
}

impl Snapshot {
    /// The schema as it is now.
    pub(super) fn read(conn: &Connection) -> Result<Self, EngineError> {
        Ok(Self::of(&objects(conn)?))
    }

    fn of(objects: &[Object]) -> Self {
        let tables = vec0_among(objects);
        let shadow_named = objects
            .iter()
            .filter(|object| {
                chunk_table_owner(&object.name).is_some()
                    || shadow_owners(&object.name).any(|owner| tables.contains(owner))
            })
            .map(|object| (object.kind.clone(), object.name.clone()))
            .collect();
        Self {
            tables,
            shadow_named,
        }
    }

    /// After a user statement changed the schema: what it did that it may not have, judged
    /// against this snapshot from before it, and the schema it left.
    pub(super) fn violation(
        &self,
        conn: &Connection,
    ) -> Result<(Self, Option<Violation>), EngineError> {
        let objects = objects(conn)?;
        let after = Self::of(&objects);
        let names: BTreeSet<&str> = objects.iter().map(|object| object.name.as_str()).collect();
        // A dropped table takes its shadow tables with it; a renamed one leaves them behind.
        let renamed = self.tables.difference(&after.tables).find(|table| {
            ALWAYS_CREATED
                .iter()
                .any(|suffix| names.contains(format!("{table}{suffix}").as_str()))
        });
        if let Some(table) = renamed {
            let violation = Violation::Renamed(table.clone());
            return Ok((after, Some(violation)));
        }
        // A new shadow-named object must be an ordinary table of a vec0 table created now, which
        // vec0 created with it.
        let created: BTreeSet<&str> = after
            .tables
            .difference(&self.tables)
            .map(String::as_str)
            .collect();
        let misnamed = after
            .shadow_named
            .difference(&self.shadow_named)
            .find(|(kind, name)| {
                kind != "table" || !shadow_owners(name).any(|owner| created.contains(owner))
            });
        let violation = misnamed.map(|(_, name)| Violation::ShadowName(name.clone()));
        Ok((after, violation))
    }
}

/// The module a `CREATE VIRTUAL TABLE` statement names, in lowercase, as SQLite keeps the
/// statement in `sqlite_schema`: the name after the first `USING` outside quotes and comments.
/// `None` for any other statement.
fn module_of(sql: &str) -> Option<String> {
    let mut tokens = Tokens(sql);
    for keyword in ["create", "virtual", "table"] {
        if !matches!(tokens.next_token()?, Token::Word(word) if word.eq_ignore_ascii_case(keyword))
        {
            return None;
        }
    }
    loop {
        if matches!(tokens.next_token()?, Token::Word(word) if word.eq_ignore_ascii_case("using")) {
            break;
        }
    }
    match tokens.next_token()? {
        Token::Word(word) => Some(word.to_ascii_lowercase()),
        Token::Quoted(word) => Some(word.to_ascii_lowercase()),
        Token::Other => None,
    }
}

/// A token of SQL, as far as [`module_of`] needs one.
#[derive(Debug, PartialEq, Eq)]
enum Token<'s> {
    /// A keyword or bare identifier.
    Word(&'s str),
    /// A quoted identifier or string, unquoted.
    Quoted(String),
    /// Any other character.
    Other,
}

/// The tokens of the SQL left to read, skipping whitespace and comments.
struct Tokens<'s>(&'s str);

impl<'s> Tokens<'s> {
    fn next_token(&mut self) -> Option<Token<'s>> {
        loop {
            // SQLite's whitespace only: other Unicode spaces are identifier characters to it.
            self.0 = self.0.trim_start_matches([' ', '\t', '\n', '\x0c', '\r']);
            if let Some(after) = self.0.strip_prefix("--") {
                self.0 = after.find('\n').map_or("", |end| &after[end..]);
            } else if let Some(after) = self.0.strip_prefix("/*") {
                self.0 = after.find("*/").map_or("", |end| &after[end + 2..]);
            } else {
                break;
            }
        }
        let first = self.0.chars().next()?;
        let close = match first {
            '"' | '\'' | '`' => Some(first),
            '[' => Some(']'),
            _ => None,
        };
        if let Some(close) = close {
            return Some(self.quoted(close));
        }
        if first.is_ascii_alphanumeric() || first == '_' || first == '$' || !first.is_ascii() {
            let end = self
                .0
                .find(|c: char| {
                    !(c.is_ascii_alphanumeric() || c == '_' || c == '$' || !c.is_ascii())
                })
                .unwrap_or(self.0.len());
            let (word, rest) = self.0.split_at(end);
            self.0 = rest;
            return Some(Token::Word(word));
        }
        self.0 = &self.0[first.len_utf8()..];
        Some(Token::Other)
    }

    /// A quoted token whose opening character the input starts with, ending at `close`, which
    /// doubles to stand for itself (except in `[…]`). An unterminated one runs to the end.
    fn quoted(&mut self, close: char) -> Token<'s> {
        let mut text = String::new();
        let mut chars = self.0.char_indices().skip(1).peekable();
        while let Some((at, c)) = chars.next() {
            if c != close {
                text.push(c);
                continue;
            }
            if close != ']' && chars.next_if(|&(_, next)| next == close).is_some() {
                text.push(c);
                continue;
            }
            self.0 = &self.0[at + c.len_utf8()..];
            return Token::Quoted(text);
        }
        self.0 = "";
        Token::Quoted(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_table_names_are_recognized_in_any_case() {
        assert_eq!(chunk_table_owner("docs_vector_chunks00"), Some("docs"));
        assert_eq!(
            chunk_table_owner("my_docs_VECTOR_CHUNKS15"),
            Some("my_docs")
        );
        for name in [
            "docs_vector_chunks0",
            "docs_vector_chunksxx",
            "_vector_chunks00",
            "docs_chunks",
            "docs_vector_chunks000x",
            "",
            "é_vector_chunks0é",
        ] {
            assert_eq!(chunk_table_owner(name), None, "{name}");
        }
        let vec0 = BTreeSet::from(["docs".to_owned()]);
        assert!(is_chunk_table("Docs_vector_chunks01", &vec0));
        assert!(!is_chunk_table("notes_vector_chunks01", &vec0));
    }

    #[test]
    fn shadow_names_are_found_by_every_suffix() {
        let owners: Vec<&str> = shadow_owners("docs_rowids").collect();
        assert_eq!(owners, ["docs"]);
        let owners: Vec<&str> = shadow_owners("a_info_metadatatext03").collect();
        assert_eq!(owners, ["a_info"]);
        let owners: Vec<&str> = shadow_owners("a_info_chunks").collect();
        assert_eq!(owners, ["a_info"]);
        assert_eq!(shadow_owners("docs").count(), 0);
        assert_eq!(shadow_owners("_info").count(), 0);
    }

    #[test]
    fn the_module_is_read_past_quotes_and_comments() {
        let cases = [
            (
                "CREATE VIRTUAL TABLE docs USING vec0(e float[2])",
                Some("vec0"),
            ),
            (
                "create virtual table Docs using VEC0 (e float[2])",
                Some("vec0"),
            ),
            (
                "CREATE VIRTUAL TABLE main.docs USING fts5(body)",
                Some("fts5"),
            ),
            (
                "CREATE VIRTUAL TABLE \"a USING fts5\" USING vec0(e float[2])",
                Some("vec0"),
            ),
            (
                "CREATE VIRTUAL TABLE [x using fts5] USING vec0(e)",
                Some("vec0"),
            ),
            (
                "CREATE VIRTUAL TABLE `x`` using fts5` USING vec0(e)",
                Some("vec0"),
            ),
            (
                "CREATE VIRTUAL TABLE t /* USING fts5 */ USING vec0(e)",
                Some("vec0"),
            ),
            (
                "CREATE VIRTUAL TABLE t -- USING fts5\n USING vec0(e)",
                Some("vec0"),
            ),
            ("CREATE VIRTUAL TABLE t USING \"vec0\"(e)", Some("vec0")),
            ("CREATE VIRTUAL TABLE t USING dbstat", Some("dbstat")),
            ("CREATE TABLE t (using_ TEXT)", None),
            (
                "CREATE TABLE \"CREATE VIRTUAL TABLE x USING vec0\" (a)",
                None,
            ),
            ("CREATE VIRTUAL TABLE \"unterminated", None),
            // A no-break space is part of an identifier to SQLite, not a space.
            (
                "CREATE VIRTUAL TABLE t\u{a0}using\u{a0}fts5 USING vec0(e)",
                Some("vec0"),
            ),
            (
                "CREATE VIRTUAL TABLE \u{a0}using fts5 USING vec0(e)",
                Some("vec0"),
            ),
            ("", None),
        ];
        for (sql, module) in cases {
            assert_eq!(module_of(sql).as_deref(), module, "{sql}");
        }
    }
}
