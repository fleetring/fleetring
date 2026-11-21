//! Registers sqlite-vec, compiled into the binary, on one SQLite connection (ADR-0049).
//!
//! sqlite-vec adds the `vec0` virtual table, for exact k-nearest-neighbour search over float32,
//! int8, and bit vectors, the `vec_each` table function, and scalar functions such as `vec_f32`
//! and `vec_distance_cosine`. Nothing is loaded at run time: the release's C source is built into
//! the binary by the `sqlite-vec` crate, and [`register`] calls its entry point on the connection
//! it is given. Its NumPy file readers have an entry point of their own, which is never called.
//!
//! This is the one place the workspace calls into C, so it is its own crate, which opts out of
//! the workspace's `forbid(unsafe_code)` explicitly (ADR-0015, point 7). It knows nothing of
//! fleetringd.
//!
//! # Examples
//!
//! ```
//! use rusqlite::Connection;
//!
//! # fn main() -> rusqlite::Result<()> {
//! let conn = Connection::open_in_memory()?;
//! fleetring_sqlite_vec::register(&conn)?;
//! let version: String = conn.query_row("SELECT vec_version()", [], |row| row.get(0))?;
//! assert_eq!(version, fleetring_sqlite_vec::VERSION);
//! # Ok(())
//! # }
//! ```

use std::ffi::{CStr, c_char, c_int};
use std::ptr;

use rusqlite::{Connection, ffi};
// Links the static library the `sqlite-vec` crate builds from the release's `sqlite-vec.c`. That
// crate declares the entry point without its parameters, so this one declares it as the C source
// defines it.
use sqlite_vec as _;

/// The sqlite-vec release compiled in, as its `vec_version()` function reports it.
pub const VERSION: &str = "v0.1.9";

#[expect(
    unsafe_code,
    reason = "the declaration of sqlite-vec's C entry point, called only by `register`"
)]
unsafe extern "C" {
    /// sqlite-vec's entry point: `int sqlite3_vec_init(sqlite3 *db, char **pzErrMsg, const
    /// sqlite3_api_routines *pApi)`. Built with `SQLITE_CORE`, it calls SQLite directly and
    /// ignores `pApi`.
    fn sqlite3_vec_init(
        db: *mut ffi::sqlite3,
        error: *mut *mut c_char,
        api: *const ffi::sqlite3_api_routines,
    ) -> c_int;
}

/// Registers sqlite-vec's `vec0` and `vec_each` modules and its scalar functions on `conn`.
///
/// Registration lasts for the connection's life and touches no database file. Register before
/// the connection reads its schema, so that SQLite knows a `vec0` table's shadow tables as such
/// from the start: defensive mode then keeps statements from writing them directly.
///
/// # Errors
///
/// The SQLite error, with sqlite-vec's message, if a function or module can't be registered.
#[expect(
    unsafe_code,
    reason = "the one call into sqlite-vec's C entry point and the free of its error message"
)]
pub fn register(conn: &Connection) -> rusqlite::Result<()> {
    let mut message: *mut c_char = ptr::null_mut();
    // SAFETY: `conn.handle()` is the open connection `conn` owns, which outlives the call and
    // which nothing else uses meanwhile, because `Connection` is `!Sync` and borrowed here.
    // `message` is a valid place for sqlite-vec to store an error string, and `pApi` may be null
    // in a `SQLITE_CORE` build, which never reads it.
    let code = unsafe { sqlite3_vec_init(conn.handle(), &raw mut message, ptr::null()) };
    let text = if message.is_null() {
        None
    } else {
        // SAFETY: sqlite-vec sets `message` only to a NUL-terminated string from
        // `sqlite3_mprintf`, which stays valid until it is freed below.
        let text = unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned();
        // SAFETY: `message` came from `sqlite3_mprintf`, so `sqlite3_free` frees it; nothing
        // reads it afterwards.
        unsafe { ffi::sqlite3_free(message.cast()) };
        Some(text)
    };
    if code == ffi::SQLITE_OK {
        return Ok(());
    }
    Err(rusqlite::Error::SqliteFailure(ffi::Error::new(code), text))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    fn registered() -> rusqlite::Result<Connection> {
        let conn = Connection::open_in_memory()?;
        register(&conn)?;
        Ok(conn)
    }

    #[test]
    fn registration_adds_the_pinned_release() -> rusqlite::Result<()> {
        let conn = registered()?;
        let version: String = conn.query_row("SELECT vec_version()", [], |row| row.get(0))?;
        assert_eq!(version, VERSION);
        Ok(())
    }

    #[test]
    fn a_connection_without_it_has_no_vec0() -> rusqlite::Result<()> {
        let conn = Connection::open_in_memory()?;
        let created = conn.execute_batch("CREATE VIRTUAL TABLE v USING vec0(e float[2])");
        let error = created.expect_err("vec0 is registered only by `register`");
        assert!(error.to_string().contains("no such module"), "{error}");
        Ok(())
    }

    #[test]
    fn vec0_answers_a_k_nearest_query() -> rusqlite::Result<()> {
        let conn = registered()?;
        conn.execute_batch(
            "CREATE VIRTUAL TABLE v USING vec0(e float[2]);
             INSERT INTO v (rowid, e) VALUES (1, '[0, 0]'), (2, '[1, 1]'), (3, '[5, 5]');",
        )?;
        let mut nearest = conn.prepare(
            "SELECT rowid FROM v WHERE e MATCH '[0.9, 0.8]' AND k = 2 ORDER BY distance",
        )?;
        let rowids = nearest
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<i64>>>()?;
        assert_eq!(rowids, [2, 1]);
        Ok(())
    }

    #[test]
    fn the_scalar_functions_are_registered() -> rusqlite::Result<()> {
        let conn = registered()?;
        let (length, json): (i64, String) = conn.query_row(
            "SELECT vec_length(vec_f32('[1, 2, 3]')), vec_to_json(vec_f32('[1, 2]'))",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(length, 3);
        assert_eq!(json, "[1.000000,2.000000]");
        Ok(())
    }

    #[test]
    fn a_fresh_schema_knows_every_shadow_table_but_the_vector_chunks() -> Result<(), Box<dyn Error>>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("v.db");
        let creator = Connection::open(&path)?;
        register(&creator)?;
        creator.execute_batch(
            "CREATE VIRTUAL TABLE v USING vec0(a float[2], b int8[2], kind TEXT, +note TEXT);
             INSERT INTO v (rowid, a, b, kind, note) VALUES (1, '[1, 2]', vec_int8('[1, 2]'), 'k', 'n');",
        )?;
        drop(creator);

        // A connection that reads the schema afresh, before it uses the table. SQLite finds a
        // shadow table's virtual table by the name before its last `_`, and 0.1.9 also leaves
        // `vector_chunksNN` out of its list of shadow names, so those are ordinary tables here:
        // fleetringd's SQLite adapter treats them as shadow tables itself (ADR-0049). A release
        // that changes this fails here, so that its upgrade revisits the adapter.
        let conn = Connection::open(&path)?;
        register(&conn)?;
        let mut types = conn.prepare(
            "SELECT name, type FROM pragma_table_list WHERE schema = 'main' AND name LIKE 'v\\_%' \
             ESCAPE '\\' ORDER BY name",
        )?;
        let types = types
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<(String, String)>>>()?;
        let types: Vec<(&str, &str)> = types
            .iter()
            .map(|(name, kind)| (name.as_str(), kind.as_str()))
            .collect();
        assert_eq!(
            types,
            [
                ("v_auxiliary", "shadow"),
                ("v_chunks", "shadow"),
                ("v_info", "shadow"),
                ("v_metadatachunks00", "shadow"),
                ("v_metadatatext00", "shadow"),
                ("v_rowids", "shadow"),
                ("v_vector_chunks00", "table"),
                ("v_vector_chunks01", "table"),
            ]
        );
        Ok(())
    }

    #[test]
    fn the_lints_are_the_workspaces_but_for_unsafe_code() -> Result<(), Box<dyn Error>> {
        let read = |path: &str| -> Result<toml::Table, Box<dyn Error>> {
            let text = std::fs::read_to_string(format!("{}/{path}", env!("CARGO_MANIFEST_DIR")))?;
            Ok(toml::from_str(&text)?)
        };
        let workspace = read("../../Cargo.toml")?;
        let mut expected = workspace
            .get("workspace")
            .and_then(|table| table.get("lints"))
            .and_then(toml::Value::as_table)
            .ok_or("the workspace has lints")?
            .clone();
        let rust = expected
            .get_mut("rust")
            .and_then(toml::Value::as_table_mut)
            .ok_or("the workspace has Rust lints")?;
        assert_eq!(
            rust.insert("unsafe_code".to_owned(), "deny".into()),
            Some("forbid".into())
        );
        let clippy = expected
            .get_mut("clippy")
            .and_then(toml::Value::as_table_mut)
            .ok_or("the workspace has clippy lints")?;
        clippy.insert("undocumented_unsafe_blocks".to_owned(), "warn".into());
        let own = read("Cargo.toml")?;
        assert_eq!(own.get("lints"), Some(&toml::Value::Table(expected)));
        Ok(())
    }

    #[test]
    fn the_file_reading_functions_are_not_registered() -> rusqlite::Result<()> {
        let conn = registered()?;
        let called = conn.query_row("SELECT vec_npy_file('x.npy')", [], |row| {
            row.get::<_, Option<String>>(0)
        });
        let error = called.expect_err("vec_npy_file has its own entry point, never called");
        assert!(error.to_string().contains("no such function"), "{error}");
        Ok(())
    }
}
