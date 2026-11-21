//! What the SQLite adapter knows of sqlite-vec's `vec0` tables beyond what SQLite reports
//! (ADR-0049).
//!
//! A `vec0` table keeps each vector column's values in a shadow table named
//! `<table>_vector_chunks00` to `_vector_chunks15`. SQLite knows its other shadow tables as such,
//! but not these: it finds a shadow table's virtual table by the name before the last `_`, and
//! sqlite-vec 0.1.9 leaves them out of its list of shadow names besides. Defensive mode therefore
//! doesn't keep statements from writing them, `pragma_table_list` calls them ordinary tables, and
//! the change recorder would put triggers on them. So the adapter itself:
//!
//! - refuses a user statement that creates, writes, alters, drops, indexes, or puts a trigger on a
//!   table so named, as SQLite prepares it: vec0's own statements on them are prepared while the
//!   user's statement steps, never while it is being prepared;
//! - records no changes to them, as for any shadow table; and
//! - hides them from the schema with the other shadow tables.

use std::collections::BTreeSet;

use rusqlite::hooks::AuthAction;

/// What follows a `vec0` table's name in the name of one of its vector chunk tables, before the
/// two digits of the vector column's index.
const CHUNK_INFIX: &str = "_vector_chunks";

/// The name of the virtual table that `name` would be a vector chunk table of: `name` without
/// `_vector_chunks` and two ASCII digits at its end, in any ASCII case, if it ends so.
pub(super) fn chunk_table_owner(name: &str) -> Option<&str> {
    let (rest, digits) = name.split_at_checked(name.len().checked_sub(2)?)?;
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let (owner, infix) = rest.split_at_checked(rest.len().checked_sub(CHUNK_INFIX.len())?)?;
    (infix.eq_ignore_ascii_case(CHUNK_INFIX) && !owner.is_empty()).then_some(owner)
}

/// Whether `name` is a vector chunk table of one of `virtual_tables`, which name the database's
/// virtual tables in the case SQLite reports them.
pub(super) fn is_chunk_table(name: &str, virtual_tables: &BTreeSet<String>) -> bool {
    chunk_table_owner(name).is_some_and(|owner| {
        virtual_tables
            .iter()
            .any(|table| table.eq_ignore_ascii_case(owner))
    })
}

/// Whether `action` creates, writes, alters, drops, indexes, or puts a trigger on a table named
/// like a vector chunk table, which a user statement may not do. Reading one is allowed: it holds
/// only the user's own vectors.
pub(super) fn changes_chunk_table(action: &AuthAction<'_>) -> bool {
    let table = match *action {
        AuthAction::CreateTable { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::Insert { table_name }
        | AuthAction::Update { table_name, .. }
        | AuthAction::Delete { table_name }
        | AuthAction::AlterTable { table_name, .. }
        | AuthAction::CreateIndex { table_name, .. }
        | AuthAction::DropIndex { table_name, .. }
        | AuthAction::CreateTrigger { table_name, .. }
        | AuthAction::DropTrigger { table_name, .. } => table_name,
        _ => return false,
    };
    chunk_table_owner(table).is_some()
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
        let virtual_tables = BTreeSet::from(["Docs".to_owned()]);
        assert!(is_chunk_table("docs_vector_chunks01", &virtual_tables));
        assert!(!is_chunk_table("notes_vector_chunks01", &virtual_tables));
    }
}
