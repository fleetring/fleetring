//! FR-070 phase 7: the agent skill's SQL runs through the SQLite adapter, its front matter is
//! valid, and every relative link in `llms.txt` names a file in the repository.

use std::error::Error;
use std::path::Path;

use fleetringd::engine::{
    CaptureMode, DatabaseEngine, OpenMode, SqliteEngine, Statement, Transaction, Value,
};

const SKILL: &str = include_str!("../../../docs/agents/SKILL.md");
const LLMS_TXT: &str = include_str!("../../../llms.txt");

/// The fence tag of a block the test runs. A block tagged `sql planned` is skipped.
const SQL_FENCE: &str = "sql";
/// The comment line that lists a statement's sample parameters.
const PARAMS_PREFIX: &str = "-- params:";

/// One statement from the skill, with the sample parameters its `-- params:` line lists.
#[derive(Debug)]
struct SkillStatement {
    sql: String,
    params: Vec<Value>,
}

/// The bodies of the fenced blocks tagged exactly `sql`, in order.
fn sql_blocks(markdown: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<(bool, String)> = None;
    for line in markdown.lines() {
        let trimmed = line.trim_start();
        match current.take() {
            None => {
                if let Some(tag) = trimmed.strip_prefix("```") {
                    current = Some((tag.trim() == SQL_FENCE, String::new()));
                }
            }
            Some((runs, mut body)) => {
                if trimmed.starts_with("```") {
                    if runs {
                        blocks.push(body);
                    }
                } else {
                    body.push_str(line);
                    body.push('\n');
                    current = Some((runs, body));
                }
            }
        }
    }
    blocks
}

/// The first keyword of `sql`, after leading comment lines, in upper case.
fn first_keyword(sql: &str) -> String {
    sql.lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with("--"))
        .and_then(|line| line.split_whitespace().next())
        .unwrap_or_default()
        .to_ascii_uppercase()
}

/// Whether `sql` creates a trigger, whose body holds statements of its own.
fn creates_trigger(sql: &str) -> bool {
    let words: Vec<String> = sql
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("--"))
        .flat_map(str::split_whitespace)
        .take(3)
        .map(str::to_ascii_uppercase)
        .collect();
    words.first().is_some_and(|word| word == "CREATE")
        && words.iter().skip(1).any(|word| word == "TRIGGER")
}

/// Splits a block into statements at a `;` that ends a line. A trigger's statement runs on to
/// the line `END;`, since its body's statements end lines too.
fn statements(block: &str) -> Result<Vec<SkillStatement>, Box<dyn Error>> {
    let mut out = Vec::new();
    let mut sql = String::new();
    let mut params = Vec::new();
    for line in block.lines() {
        if let Some(list) = line.trim().strip_prefix(PARAMS_PREFIX) {
            params = parse_params(list)?;
            continue;
        }
        sql.push_str(line);
        sql.push('\n');
        let trimmed = line.trim_end();
        let ends = if creates_trigger(&sql) {
            trimmed.trim_start().eq_ignore_ascii_case("END;")
        } else {
            trimmed.ends_with(';')
        };
        if ends {
            out.push(SkillStatement {
                sql: std::mem::take(&mut sql),
                params: std::mem::take(&mut params),
            });
        }
    }
    if !sql.trim().is_empty() || !params.is_empty() {
        return Err(format!("a statement doesn't end with `;` at a line end: {sql}").into());
    }
    Ok(out)
}

/// Parses `'text', 12, NULL`: single-quoted text with `''` for a quote, integers, and `NULL`.
fn parse_params(list: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    let mut values = Vec::new();
    let mut chars = list.trim().chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let Some(&first) = chars.peek() else {
            break;
        };
        if first == '\'' {
            chars.next();
            let mut text = String::new();
            loop {
                match chars.next() {
                    Some('\'') if chars.next_if_eq(&'\'').is_some() => text.push('\''),
                    Some('\'') => break,
                    Some(c) => text.push(c),
                    None => return Err(format!("unterminated text in params: {list}").into()),
                }
            }
            values.push(Value::Text(text));
        } else {
            let mut token = String::new();
            while let Some(c) = chars.next_if(|c| *c != ',') {
                token.push(c);
            }
            let token = token.trim();
            if token.eq_ignore_ascii_case("NULL") {
                values.push(Value::Null);
            } else {
                values.push(Value::Integer(token.parse().map_err(|error| {
                    format!("param {token:?} in {list:?} is not an integer: {error}")
                })?));
            }
        }
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        match chars.next() {
            None => break,
            Some(',') => {}
            Some(c) => return Err(format!("expected `,` in params, found {c:?}: {list}").into()),
        }
    }
    Ok(values)
}

/// Whether the database records row changes, as it does when a change stream covers it, such
/// as every database on a dev node.
#[derive(Debug, Clone, Copy)]
enum Recording {
    Off,
    On,
}

/// Runs every `sql` block of the skill, in order, on a fresh database: `query` for a `SELECT`
/// or `WITH`, `execute` for the rest. Fails naming the block and statement that failed.
fn run_the_skill(recording: Recording) -> Result<(), Box<dyn Error>> {
    let blocks = sql_blocks(SKILL);
    assert!(!blocks.is_empty(), "the skill has no sql blocks");

    let dir = tempfile::tempdir()?;
    let engine = SqliteEngine::new(4096)?;
    let mut db = engine.open(dir.path(), OpenMode::Create)?;
    if let Recording::On = recording {
        db.logical_changes()
            .ok_or("the SQLite adapter records row changes")?
            .start_recording(CaptureMode::Full)?;
    }
    let mut ran = 0_usize;
    for (index, block) in blocks.iter().enumerate() {
        for statement in statements(block)? {
            let prepared = Statement::with_params(statement.sql.clone(), statement.params);
            let outcome = match first_keyword(&statement.sql).as_str() {
                "SELECT" | "WITH" => db.query(&prepared).map(drop),
                _ => db.execute(&Transaction::from(prepared)).map(drop),
            };
            if let Err(error) = outcome {
                return Err(
                    format!("sql block {} failed: {error}\n{}", index + 1, statement.sql).into(),
                );
            }
            ran += 1;
        }
    }
    assert!(ran >= blocks.len(), "ran {ran} statements");
    db.close()?;
    Ok(())
}

#[test]
fn every_sql_block_in_the_skill_runs_through_the_sqlite_adapter() -> Result<(), Box<dyn Error>> {
    run_the_skill(Recording::Off)
}

#[test]
fn every_sql_block_in_the_skill_runs_with_row_changes_recorded() -> Result<(), Box<dyn Error>> {
    run_the_skill(Recording::On)
}

#[test]
fn the_skills_patterns_behave_as_described() -> Result<(), Box<dyn Error>> {
    // The text promises a compare-and-set claim, a refused transition, and a ranked search;
    // run the blocks, then check each promise.
    let dir = tempfile::tempdir()?;
    let engine = SqliteEngine::new(4096)?;
    let mut db = engine.open(dir.path(), OpenMode::Create)?;
    for block in sql_blocks(SKILL) {
        for statement in statements(&block)? {
            if first_keyword(&statement.sql) != "SELECT" {
                let prepared = Statement::with_params(statement.sql, statement.params);
                db.execute(&Transaction::from(prepared))?;
            }
        }
    }

    let claim = Statement::with_params(
        "UPDATE tasks SET state = 'doing', attempts = attempts + 1, updated_at = ? \
         WHERE id = ? AND state = 'todo'",
        [Value::Integer(1), Value::from("task-7f3a")],
    );
    assert_eq!(db.execute(&Transaction::from(claim))?.rows_affected, 0);

    let reopen = Statement::new("UPDATE tasks SET state = 'todo' WHERE id = 'task-7f3a'");
    assert!(db.execute(&Transaction::from(reopen)).is_err());

    let search = Statement::with_params(
        "SELECT n.id FROM notes_fts JOIN notes n ON n.id = notes_fts.rowid \
         WHERE notes_fts MATCH ? ORDER BY bm25(notes_fts)",
        [Value::from("expiry")],
    );
    assert_eq!(db.query(&search)?.rows, vec![vec![Value::Integer(1)]]);
    db.close()?;
    Ok(())
}

#[test]
fn the_skill_has_valid_front_matter() {
    let mut lines = SKILL.lines();
    assert_eq!(
        lines.next(),
        Some("---"),
        "the skill starts with front matter"
    );
    let front: Vec<&str> = lines.take_while(|line| *line != "---").collect();
    let field = |key: &str| {
        front
            .iter()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix(':'))
            .map(str::trim)
    };
    assert_eq!(field("name"), Some("fleetring"));
    let description = field("description").unwrap_or_default();
    assert!(!description.is_empty(), "the skill has no description");
    assert!(
        description.len() <= 1024,
        "the description is {} bytes",
        description.len()
    );
}

#[test]
fn the_skills_statement_splitter_reads_params_and_triggers() -> Result<(), Box<dyn Error>> {
    let block = "-- params: 'it''s, ok', 7, NULL\nSELECT ?, ?, ?;\n\
                 CREATE TRIGGER t AFTER INSERT ON a BEGIN\n  SELECT 1;\nEND;\n";
    let parsed = statements(block)?;
    assert_eq!(parsed.len(), 2);
    assert_eq!(
        parsed[0].params,
        vec![Value::from("it's, ok"), Value::Integer(7), Value::Null]
    );
    assert!(parsed[1].sql.trim_end().ends_with("END;"));
    assert!(statements("SELECT 1").is_err());
    Ok(())
}

#[test]
fn llms_txt_links_resolve() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut checked = 0_usize;
    let mut rest = LLMS_TXT;
    while let Some(at) = rest.find("](") {
        rest = &rest[at + 2..];
        let Some(end) = rest.find(')') else {
            break;
        };
        let target = &rest[..end];
        if target.contains("://") {
            continue;
        }
        let path = target.split('#').next().unwrap_or_default();
        assert!(
            root.join(path).exists(),
            "llms.txt links to {target}, which isn't in the repository"
        );
        checked += 1;
    }
    assert!(checked > 0, "llms.txt has no relative links");
}
