//! A small MCP client for the tests: it drives `fleetring mcp` as an MCP client does, writing
//! newline-delimited JSON-RPC to the server's stdin and reading it from its stdout.

use std::error::Error;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout};

/// Guards against a hang only.
pub(crate) const HANG_GUARD: Duration = Duration::from_secs(60);

/// A running `fleetring mcp`, and every line it wrote to stdout.
pub(crate) struct Session {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    lines: Vec<String>,
    /// Everything the server writes to stderr, read as it comes so the pipe never fills.
    stderr: tokio::task::JoinHandle<std::io::Result<String>>,
    next_id: u64,
    /// The instructions the server gave at `initialize`.
    pub(crate) instructions: String,
}

impl Session {
    /// Starts `command`, which runs the MCP server, with every log line on stderr, then opens a
    /// session as an MCP client does.
    ///
    /// # Errors
    ///
    /// If the server can't be started or doesn't answer `initialize`, with what it wrote to
    /// stderr.
    pub(crate) async fn spawn(
        mut command: tokio::process::Command,
    ) -> Result<Self, Box<dyn Error>> {
        command
            // Every log line the process writes, so that none of them can hide on stdout.
            .env("RUST_LOG", "trace")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let stdin = child.stdin.take().ok_or("no stdin")?;
        let stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
        let mut pipe = child.stderr.take().ok_or("no stderr")?;
        let stderr = tokio::spawn(async move {
            let mut text = String::new();
            pipe.read_to_string(&mut text).await?;
            Ok(text)
        });
        let mut session = Self {
            child,
            stdin,
            stdout,
            lines: Vec::new(),
            stderr,
            next_id: 0,
            instructions: String::new(),
        };
        let initialized = match session
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "fleetring-tests", "version": "0"},
                }),
            )
            .await
        {
            Ok(initialized) => initialized,
            Err(error) => {
                let stderr = session.kill().await;
                return Err(format!("no answer to initialize ({error}); stderr: {stderr}").into());
            }
        };
        let result = &initialized["result"];
        assert_eq!(result["serverInfo"]["name"], "fleetringd", "{initialized}");
        assert!(result["capabilities"]["tools"].is_object(), "{initialized}");
        session.instructions = result["instructions"]
            .as_str()
            .ok_or("no instructions")?
            .to_owned();
        session
            .send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .await?;
        Ok(session)
    }

    pub(crate) async fn send(&mut self, message: &Value) -> Result<(), Box<dyn Error>> {
        let mut line = serde_json::to_vec(message)?;
        line.push(b'\n');
        self.stdin.write_all(&line).await?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Sends a request and returns the response with its ID: a `result` or an `error`.
    pub(crate) async fn request(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Value, Box<dyn Error>> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await?;
        tokio::time::timeout(HANG_GUARD, async {
            loop {
                let line = self
                    .stdout
                    .next_line()
                    .await?
                    .ok_or("the server closed stdout")?;
                self.lines.push(line.clone());
                let message: Value = serde_json::from_str(&line)?;
                if message["id"] == json!(id) {
                    return Ok::<_, Box<dyn Error>>(message);
                }
            }
        })
        .await?
    }

    pub(crate) async fn tools(&mut self) -> Result<Vec<Value>, Box<dyn Error>> {
        let listed = self.request("tools/list", json!({})).await?;
        Ok(listed["result"]["tools"]
            .as_array()
            .ok_or("no tools")?
            .clone())
    }

    /// Calls `tool` and returns the JSON-RPC response.
    pub(crate) async fn call(
        &mut self,
        tool: &str,
        arguments: Value,
    ) -> Result<Value, Box<dyn Error>> {
        self.request("tools/call", json!({"name": tool, "arguments": arguments}))
            .await
    }

    /// Calls `tool`, which must succeed, and returns its structured content, after checking
    /// that its one text block holds the same JSON.
    pub(crate) async fn ok(
        &mut self,
        tool: &str,
        arguments: Value,
    ) -> Result<Value, Box<dyn Error>> {
        let response = self.call(tool, arguments).await?;
        let result = &response["result"];
        assert_eq!(result["isError"], json!(false), "{tool}: {response}");
        let structured = result["structuredContent"].clone();
        let content = result["content"].as_array().ok_or("no content")?;
        assert_eq!(content.len(), 1, "{response}");
        let text = content[0]["text"].as_str().ok_or("no text block")?;
        assert!(!text.contains('\n'), "the text block is one line: {text}");
        assert_eq!(
            serde_json::from_str::<Value>(text)?,
            structured,
            "the text block holds the structured content"
        );
        Ok(structured)
    }

    /// Calls `tool`, which must fail as a tool, and returns its `error` object.
    pub(crate) async fn failed(
        &mut self,
        tool: &str,
        arguments: Value,
    ) -> Result<Value, Box<dyn Error>> {
        let response = self.call(tool, arguments).await?;
        let result = &response["result"];
        assert_eq!(result["isError"], json!(true), "{tool}: {response}");
        assert!(
            result.get("structuredContent").is_none(),
            "an error matches no output schema: {response}"
        );
        let text = result["content"][0]["text"]
            .as_str()
            .ok_or("no text block")?;
        let error: Value = serde_json::from_str(text)?;
        Ok(error["error"].clone())
    }

    /// Calls `tool` with arguments it refuses, and returns the JSON-RPC error.
    pub(crate) async fn invalid(
        &mut self,
        tool: &str,
        arguments: Value,
    ) -> Result<Value, Box<dyn Error>> {
        let response = self.call(tool, arguments).await?;
        assert!(response.get("result").is_none(), "{tool}: {response}");
        assert_eq!(response["error"]["code"], json!(-32602), "{response}");
        Ok(response["error"].clone())
    }

    /// Closes stdin, as a client that is done does, and checks that the server exited
    /// cleanly and wrote nothing to stdout but JSON-RPC messages. Returns its stderr.
    pub(crate) async fn close(mut self) -> Result<String, Box<dyn Error>> {
        drop(self.stdin);
        while let Some(line) = tokio::time::timeout(HANG_GUARD, self.stdout.next_line()).await?? {
            self.lines.push(line);
        }
        let status = tokio::time::timeout(HANG_GUARD, self.child.wait()).await??;
        let stderr = tokio::time::timeout(HANG_GUARD, self.stderr).await???;
        assert!(status.success(), "exit {status}: {stderr}");
        assert!(!self.lines.is_empty());
        for line in &self.lines {
            let message: Value = serde_json::from_str(line).map_err(|error| {
                format!("stdout carried a line that isn't JSON ({error}): {line}")
            })?;
            assert_eq!(message["jsonrpc"], "2.0", "{line}");
        }
        Ok(stderr)
    }

    /// Kills the server and returns what it wrote to stderr.
    async fn kill(mut self) -> String {
        let _ = self.child.kill().await;
        match tokio::time::timeout(HANG_GUARD, self.stderr).await {
            Ok(Ok(Ok(text))) => text,
            _ => String::new(),
        }
    }
}
