//! FR-070 phase 7: the agent skill's SQL runs through the SQLite adapter, its tool calls match
//! the tools' input schemas, its front matter is valid, and every relative link in `llms.txt`
//! names a file in the repository.

use std::error::Error;
use std::path::Path;

use fleetringd::engine::{
    CaptureMode, DatabaseEngine, Interrupt, OpenMode, SqliteEngine, Statement, Transaction, Value,
};
use serde_json::Value as Json;

const SKILL: &str = include_str!("../../../docs/agents/SKILL.md");
const QUICKSTART: &str = include_str!("../../../docs/agents/quickstart.md");
const LLMS_TXT: &str = include_str!("../../../llms.txt");
/// `tools/list` as the server answers it, which `mcp::server`'s tests hold equal.
const TOOLS: &str = include_str!("fixtures/mcp/tools.json");

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

/// The fence tag of a block of JSON: tool calls, arguments, or a result.
const JSON_FENCE: &str = "json";

/// The bodies of the fenced blocks tagged exactly `sql`, in order.
fn sql_blocks(markdown: &str) -> Vec<String> {
    blocks_tagged(markdown, SQL_FENCE)
}

/// The bodies of the fenced blocks tagged exactly `fence`, in order.
fn blocks_tagged(markdown: &str, fence: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<(bool, String)> = None;
    for line in markdown.lines() {
        let trimmed = line.trim_start();
        match current.take() {
            None => {
                if let Some(tag) = trimmed.strip_prefix("```") {
                    current = Some((tag.trim() == fence, String::new()));
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
                "SELECT" | "WITH" => db.query(&prepared, &Interrupt::new()).map(drop),
                _ => db
                    .execute(&Transaction::from(prepared), &Interrupt::new())
                    .map(drop),
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
                db.execute(&Transaction::from(prepared), &Interrupt::new())?;
            }
        }
    }

    let claim = Statement::with_params(
        "UPDATE tasks SET state = 'doing', attempts = attempts + 1, updated_at = ? \
         WHERE id = ? AND state = 'todo'",
        [Value::Integer(1), Value::from("task-7f3a")],
    );
    assert_eq!(
        db.execute(&Transaction::from(claim), &Interrupt::new())?
            .rows_affected,
        0
    );

    let reopen = Statement::new("UPDATE tasks SET state = 'todo' WHERE id = 'task-7f3a'");
    assert!(
        db.execute(&Transaction::from(reopen), &Interrupt::new())
            .is_err()
    );

    let search = Statement::with_params(
        "SELECT n.id FROM notes_fts JOIN notes n ON n.id = notes_fts.rowid \
         WHERE notes_fts MATCH ? ORDER BY bm25(notes_fts)",
        [Value::from("expiry")],
    );
    assert_eq!(
        db.query(&search, &Interrupt::new())?.rows,
        vec![vec![Value::Integer(1)]]
    );
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

/// The JSON values of the `json` blocks of `markdown`: a block that parses whole is one value, a
/// block that is an object's members, such as `"description": {…}`, is that object, and any
/// other holds one value per line, as a list of tool calls does.
fn json_values(markdown: &str) -> Result<Vec<Json>, Box<dyn Error>> {
    let mut values = Vec::new();
    for block in blocks_tagged(markdown, JSON_FENCE) {
        let members = format!("{{{block}}}");
        if let Ok(value) =
            serde_json::from_str::<Json>(&block).or_else(|_| serde_json::from_str(&members))
        {
            values.push(value);
            continue;
        }
        for line in block.lines().filter(|line| !line.trim().is_empty()) {
            values
                .push(serde_json::from_str(line).map_err(|error| {
                    format!("a json block's line isn't JSON ({error}): {line}")
                })?);
        }
    }
    Ok(values)
}

/// The tool called `name`, as `tools/list` describes it.
fn tool(name: &str) -> Result<Json, Box<dyn Error>> {
    let tools: Vec<Json> = serde_json::from_str(TOOLS)?;
    tools
        .into_iter()
        .find(|tool| tool["name"] == name)
        .ok_or_else(|| format!("no tool {name}").into())
}

/// Whether `value` has one of the JSON Schema types `types` names (a string or a list).
fn has_type(value: &Json, types: &Json) -> bool {
    let named: Vec<&str> = match types {
        Json::String(one) => vec![one.as_str()],
        Json::Array(many) => many.iter().filter_map(Json::as_str).collect(),
        _ => return true,
    };
    named.iter().any(|name| match *name {
        "null" => value.is_null(),
        "string" => value.is_string(),
        "integer" => value.is_i64() || value.is_u64(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        _ => true,
    })
}

/// Checks `arguments` against `tool`'s input schema: no unknown field, every required one, each
/// of its declared type and within its bounds.
fn check_arguments(tool: &Json, arguments: &Json) -> Result<(), String> {
    let name = &tool["name"];
    let schema = &tool["inputSchema"];
    let given = arguments
        .as_object()
        .ok_or_else(|| format!("{name}'s arguments aren't an object: {arguments}"))?;
    for required in schema["required"].as_array().into_iter().flatten() {
        let key = required.as_str().unwrap_or_default();
        if !given.contains_key(key) {
            return Err(format!("{name} needs {key}: {arguments}"));
        }
    }
    for (key, value) in given {
        let property = &schema["properties"][key];
        if property.is_null() {
            return Err(format!("{name} takes no {key}: {arguments}"));
        }
        if !has_type(value, &property["type"]) {
            return Err(format!(
                "{name}'s {key} isn't a {}: {value}",
                property["type"]
            ));
        }
        if let Some(number) = value.as_f64() {
            let low = property["minimum"].as_f64().unwrap_or(f64::MIN);
            let high = property["maximum"].as_f64().unwrap_or(f64::MAX);
            if number < low || number > high {
                return Err(format!("{name}'s {key} is outside {low}..={high}: {value}"));
            }
        }
    }
    Ok(())
}

/// Whether `text` is a position as `changes` takes one: an even number of hex digits.
fn is_position(text: &str) -> bool {
    !text.is_empty()
        && text.len().is_multiple_of(2)
        && text.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[test]
fn the_tool_calls_in_the_skill_and_the_quickstart_match_the_tools() -> Result<(), Box<dyn Error>> {
    let mut calls = Vec::new();
    for (page, markdown) in [("SKILL.md", SKILL), ("quickstart.md", QUICKSTART)] {
        for value in json_values(markdown)? {
            let (Some(name), Some(arguments)) = (value["tool"].as_str(), value.get("arguments"))
            else {
                continue;
            };
            check_arguments(&tool(name)?, arguments).map_err(|error| format!("{page}: {error}"))?;
            calls.push((page, name.to_owned(), arguments.clone()));
        }
    }
    // The skill's `follow changes` pattern starts without a position and continues after one.
    let changes: Vec<&Json> = calls
        .iter()
        .filter(|(page, name, _)| *page == "SKILL.md" && name == "changes")
        .map(|(_, _, arguments)| arguments)
        .collect();
    assert!(
        changes
            .iter()
            .any(|arguments| arguments.get("after").is_none()),
        "the skill starts following without after"
    );
    assert!(
        changes
            .iter()
            .any(|arguments| arguments.get("after").is_some()),
        "the skill continues after a position"
    );
    for (page, name, arguments) in &calls {
        if let Some(after) = arguments.get("after") {
            assert!(
                after.as_str().is_some_and(is_position),
                "{page}: {name}'s after isn't a position: {after}"
            );
        }
    }
    Ok(())
}

#[test]
fn the_argument_check_refuses_what_the_server_refuses() -> Result<(), Box<dyn Error>> {
    let changes = tool("changes")?;
    let check = |arguments: Json| check_arguments(&changes, &arguments);
    assert!(check(serde_json::json!({"database": "agent-42-inbox", "limit": 100})).is_ok());
    for refused in [
        serde_json::json!({}),
        serde_json::json!({"database": "agent-42-inbox", "from": "00"}),
        serde_json::json!({"database": "agent-42-inbox", "limit": 101}),
        serde_json::json!({"database": "agent-42-inbox", "limit": "20"}),
        serde_json::json!({"database": 42}),
    ] {
        assert!(check(refused.clone()).is_err(), "{refused}");
    }
    assert!(!is_position("0") && !is_position("zz") && is_position("00ff"));
    Ok(())
}

#[test]
fn the_skills_changes_result_has_the_tools_fields() -> Result<(), Box<dyn Error>> {
    let changes = tool("changes")?;
    let fields = |schema: &Json| -> Vec<String> {
        let mut keys: Vec<String> = schema["properties"]
            .as_object()
            .map(|properties| properties.keys().cloned().collect())
            .unwrap_or_default();
        keys.sort();
        keys
    };
    let keys = |value: &Json| -> Vec<String> {
        let mut keys: Vec<String> = value
            .as_object()
            .map(|object| object.keys().cloned().collect())
            .unwrap_or_default();
        keys.sort();
        keys
    };
    let result = json_values(SKILL)?
        .into_iter()
        .find(|value| value.get("transactions").is_some())
        .ok_or("the skill shows no changes result")?;
    let output = &changes["outputSchema"];
    assert_eq!(keys(&result), fields(output));
    let definitions = &output["$defs"];
    let transaction = &result["transactions"][0];
    assert_eq!(keys(transaction), fields(&definitions["TransactionOut"]));
    let change = &transaction["changes"][0];
    assert_eq!(keys(change), fields(&definitions["RowChangeOut"]));
    assert_eq!(keys(&change["key"][0]), fields(&definitions["ColumnOut"]));
    for position in [
        &transaction["start"],
        &transaction["position"],
        &result["next_after"],
    ] {
        assert!(position.as_str().is_some_and(is_position), "{position}");
    }
    assert_eq!(result["next_after"], transaction["position"]);
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
