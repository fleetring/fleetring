//! A pinned snapshot's files as one checksummed byte stream, packed and unpacked a chunk at a
//! time, so neither side ever holds the whole snapshot in memory.
//!
//! The engine decides what a snapshot holds; this module only moves its files. The stream is each
//! file in the snapshot's order, as a big-endian `u16` name length, the UTF-8 name, a big-endian
//! `u64` content length, and the content. Its checksum is XXH3-128 over the whole stream,
//! big-endian. XXH3 catches corruption in transit or on disk; it is not a cryptographic hash and
//! doesn't stop a peer that forges a stream on purpose. Peers are trusted members of the cluster.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};

use twox_hash::XxHash3_128;

use super::epochs::sync_dir;
use crate::engine::{PinnedSnapshot, Position};

/// The length of a stream's checksum.
pub(super) const CHECKSUM_LEN: usize = 16;

/// Reads a pinned snapshot's files as one stream.
pub(super) struct Packer {
    snapshot: PinnedSnapshot,
    /// The index of the next file whose header is due.
    next_file: usize,
    /// The current file's header, and how much of it has been emitted.
    header: Vec<u8>,
    header_sent: usize,
    /// Content bytes of the current file still to emit.
    remaining: u64,
    offset: u64,
    hasher: XxHash3_128,
}

impl std::fmt::Debug for Packer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Packer")
            .field("snapshot", &self.snapshot)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl Packer {
    /// Packs `snapshot`'s files.
    ///
    /// # Errors
    ///
    /// An I/O error if a file name isn't a plain name that fits the format.
    pub(super) fn new(snapshot: PinnedSnapshot) -> io::Result<Self> {
        for file in snapshot.files() {
            if !is_plain_name(file.name()) || u16::try_from(file.name().len()).is_err() {
                return Err(invalid(format!(
                    "snapshot file name {:?} isn't a plain name",
                    file.name()
                )));
            }
        }
        Ok(Self {
            snapshot,
            next_file: 0,
            header: Vec::new(),
            header_sent: 0,
            remaining: 0,
            offset: 0,
            hasher: XxHash3_128::new(),
        })
    }

    /// The position the snapshot holds everything up to.
    pub(super) fn position(&self) -> Option<&Position> {
        self.snapshot.position()
    }

    /// How many bytes of the stream have been emitted.
    pub(super) fn offset(&self) -> u64 {
        self.offset
    }

    /// The next up to `max` bytes of the stream, or `None` once all of it has been emitted.
    ///
    /// # Errors
    ///
    /// An I/O error if a file can't be read or ends before its pinned length.
    pub(super) fn next_chunk(&mut self, max: usize) -> io::Result<Option<Vec<u8>>> {
        let mut out = Vec::with_capacity(max);
        while out.len() < max {
            if self.header_sent < self.header.len() {
                let take = (self.header.len() - self.header_sent).min(max - out.len());
                out.extend_from_slice(&self.header[self.header_sent..self.header_sent + take]);
                self.header_sent += take;
            } else if self.remaining > 0 {
                let file = &self.snapshot.files()[self.next_file - 1];
                let want = usize::try_from(self.remaining)
                    .unwrap_or(usize::MAX)
                    .min(max - out.len());
                let start = out.len();
                out.resize(start + want, 0);
                let read = (&mut file.file()).read(&mut out[start..])?;
                out.truncate(start + read);
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "snapshot file {} ended {} bytes early",
                            file.name(),
                            self.remaining
                        ),
                    ));
                }
                self.remaining -= u64::try_from(read).unwrap_or(u64::MAX);
            } else if let Some(file) = self.snapshot.files().get(self.next_file) {
                self.header = file_header(file.name(), file.len());
                self.header_sent = 0;
                self.remaining = file.len();
                self.next_file += 1;
            } else {
                break;
            }
        }
        if out.is_empty() {
            return Ok(None);
        }
        self.hasher.write(&out);
        self.offset += u64::try_from(out.len()).unwrap_or(u64::MAX);
        Ok(Some(out))
    }

    /// The whole stream's checksum, once [`next_chunk`](Self::next_chunk) has returned `None`,
    /// after confirming with the engine that the files didn't change while they were read.
    ///
    /// # Errors
    ///
    /// An I/O error if the stream isn't finished, or the files changed.
    pub(super) fn finish(&self) -> io::Result<[u8; CHECKSUM_LEN]> {
        if self.remaining > 0
            || self.header_sent < self.header.len()
            || self.next_file < self.snapshot.files().len()
        {
            return Err(io::Error::other("the snapshot stream isn't finished"));
        }
        self.snapshot.confirm().map_err(io::Error::other)?;
        Ok(self.hasher.finish_128().to_be_bytes())
    }
}

fn file_header(name: &str, len: u64) -> Vec<u8> {
    let name_len = u16::try_from(name.len()).unwrap_or(u16::MAX);
    let mut header = Vec::with_capacity(2 + name.len() + 8);
    header.extend_from_slice(&name_len.to_be_bytes());
    header.extend_from_slice(name.as_bytes());
    header.extend_from_slice(&len.to_be_bytes());
    header
}

/// Writes a stream into a new directory as it arrives, and verifies it at the end.
///
/// Nothing it writes is ever in a database's place: the caller moves the directory there only
/// after [`finish`](Self::finish) succeeds, and removes it otherwise.
pub(super) struct Unpacker {
    dir: PathBuf,
    state: State,
    names: BTreeSet<String>,
    offset: u64,
    hasher: XxHash3_128,
}

impl std::fmt::Debug for Unpacker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Unpacker")
            .field("dir", &self.dir)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

/// Where the unpacker is in the stream.
#[derive(Debug)]
enum State {
    /// Reading a file's name length.
    NameLen(Vec<u8>),
    /// Reading a file's name.
    Name { len: usize, bytes: Vec<u8> },
    /// Reading a file's content length.
    ContentLen { name: String, bytes: Vec<u8> },
    /// Writing a file's content.
    Content { file: File, remaining: u64 },
}

impl Unpacker {
    /// Starts unpacking into `dir`, replacing anything a failed transfer left there.
    ///
    /// # Errors
    ///
    /// An I/O error if the directory can't be created.
    pub(super) fn create(dir: &Path) -> io::Result<Self> {
        remove_dir_if_present(dir)?;
        if let Some(parent) = dir.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir(dir)?;
        Ok(Self {
            dir: dir.to_owned(),
            state: State::NameLen(Vec::new()),
            names: BTreeSet::new(),
            offset: 0,
            hasher: XxHash3_128::new(),
        })
    }

    /// How many bytes of the stream have arrived.
    pub(super) fn offset(&self) -> u64 {
        self.offset
    }

    /// Takes the next bytes of the stream.
    ///
    /// # Errors
    ///
    /// An I/O error if the stream is malformed, names a file outside the directory or twice, or
    /// a file can't be written.
    pub(super) fn write(&mut self, mut data: &[u8]) -> io::Result<()> {
        self.hasher.write(data);
        self.offset += u64::try_from(data.len()).unwrap_or(u64::MAX);
        while !data.is_empty() {
            let state = std::mem::replace(&mut self.state, State::NameLen(Vec::new()));
            self.state = match state {
                State::NameLen(mut bytes) => {
                    fill(&mut bytes, 2, &mut data);
                    match <[u8; 2]>::try_from(bytes.as_slice()) {
                        Ok(len) => State::Name {
                            len: usize::from(u16::from_be_bytes(len)),
                            bytes: Vec::new(),
                        },
                        Err(_) => State::NameLen(bytes),
                    }
                }
                State::Name { len, mut bytes } => {
                    fill(&mut bytes, len, &mut data);
                    if bytes.len() < len {
                        State::Name { len, bytes }
                    } else {
                        let name = String::from_utf8(bytes)
                            .map_err(|_| invalid("snapshot file name isn't UTF-8".to_owned()))?;
                        if !is_plain_name(&name) {
                            return Err(invalid(format!(
                                "snapshot file name {name:?} isn't a plain name"
                            )));
                        }
                        if !self.names.insert(name.clone()) {
                            return Err(invalid(format!("snapshot file {name} appears twice")));
                        }
                        State::ContentLen {
                            name,
                            bytes: Vec::new(),
                        }
                    }
                }
                State::ContentLen { name, mut bytes } => {
                    fill(&mut bytes, 8, &mut data);
                    match <[u8; 8]>::try_from(bytes.as_slice()) {
                        Ok(len) => self.start_file(&name, u64::from_be_bytes(len))?,
                        Err(_) => State::ContentLen { name, bytes },
                    }
                }
                State::Content {
                    mut file,
                    remaining,
                } => {
                    let take = usize::try_from(remaining)
                        .unwrap_or(usize::MAX)
                        .min(data.len());
                    file.write_all(&data[..take])?;
                    data = &data[take..];
                    let remaining = remaining - u64::try_from(take).unwrap_or(u64::MAX);
                    if remaining == 0 {
                        file.sync_all()?;
                        State::NameLen(Vec::new())
                    } else {
                        State::Content { file, remaining }
                    }
                }
            };
        }
        Ok(())
    }

    fn start_file(&self, name: &str, len: u64) -> io::Result<State> {
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(self.dir.join(name))?;
        if len == 0 {
            file.sync_all()?;
            return Ok(State::NameLen(Vec::new()));
        }
        Ok(State::Content {
            file,
            remaining: len,
        })
    }

    /// Ends the stream: checks that it ended between files and matches `checksum`, and makes the
    /// directory's entries durable.
    ///
    /// # Errors
    ///
    /// An I/O error of kind [`io::ErrorKind::InvalidData`] if the stream ended early or doesn't
    /// match, and another I/O error if the directory can't be synced.
    pub(super) fn finish(self, checksum: &[u8]) -> io::Result<()> {
        if !matches!(&self.state, State::NameLen(bytes) if bytes.is_empty()) {
            return Err(invalid(
                "the snapshot stream ended inside a file".to_owned(),
            ));
        }
        if checksum != self.hasher.finish_128().to_be_bytes() {
            return Err(invalid("the snapshot's checksum doesn't match".to_owned()));
        }
        sync_dir(&self.dir)
    }
}

/// Moves up to `want - bytes.len()` bytes from the front of `data` to `bytes`.
fn fill(bytes: &mut Vec<u8>, want: usize, data: &mut &[u8]) {
    let take = want.saturating_sub(bytes.len()).min(data.len());
    bytes.extend_from_slice(&data[..take]);
    *data = &data[take..];
}

/// Whether `name` names a file directly inside a directory, and nothing else.
fn is_plain_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', '\0', ':'])
}

/// Removes `dir` and everything in it, if it exists.
pub(super) fn remove_dir_if_present(dir: &Path) -> io::Result<()> {
    match fs::remove_dir_all(dir) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use crate::engine::{EngineError, SnapshotFile, SnapshotGuard};

    use super::*;

    #[derive(Debug)]
    struct Unchanged;

    impl SnapshotGuard for Unchanged {
        fn confirm(&self) -> Result<(), EngineError> {
            Ok(())
        }
    }

    fn snapshot(source: &Path, files: &[(&str, &[u8])]) -> PinnedSnapshot {
        fs::create_dir_all(source).expect("source dir");
        let files = files
            .iter()
            .map(|(name, content)| {
                let path = source.join(name);
                fs::write(&path, content).expect("file");
                let len = u64::try_from(content.len()).expect("small");
                SnapshotFile::new(*name, File::open(&path).expect("open"), len)
            })
            .collect();
        PinnedSnapshot::new(Some(Position::new([7])), files, Unchanged)
    }

    /// Packs `snapshot` in `chunk`-byte chunks.
    fn pack(snapshot: PinnedSnapshot, chunk: usize) -> (Vec<Vec<u8>>, [u8; CHECKSUM_LEN]) {
        let mut packer = Packer::new(snapshot).expect("packer");
        let mut chunks = Vec::new();
        while let Some(data) = packer.next_chunk(chunk).expect("chunk") {
            assert!(data.len() <= chunk, "chunks stay within the limit");
            chunks.push(data);
        }
        (chunks, packer.finish().expect("checksum"))
    }

    #[test]
    fn a_snapshot_round_trips_in_chunks_of_any_size() {
        let dir = tempfile::tempdir().expect("temp dir");
        let big: Vec<u8> = (0..5_000_u32)
            .map(|i| u8::try_from(i % 251).expect("small"))
            .collect();
        for chunk in [1, 3, 7, 64, 4096, 1 << 20] {
            let source = dir.path().join(format!("source-{chunk}"));
            let files: [(&str, &[u8]); 3] = [
                ("data.db-checkpoint", b"marker"),
                ("data.db", &big),
                ("empty", b""),
            ];
            let (chunks, checksum) = pack(snapshot(&source, &files), chunk);
            let target = dir.path().join(format!("target-{chunk}"));
            let mut unpacker = Unpacker::create(&target).expect("unpacker");
            for data in &chunks {
                unpacker.write(data).expect("write");
            }
            unpacker.finish(&checksum).expect("finish");
            for (name, content) in files {
                assert_eq!(
                    fs::read(target.join(name)).expect("read"),
                    content,
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn a_corrupted_stream_fails_its_checksum() {
        let dir = tempfile::tempdir().expect("temp dir");
        let source = dir.path().join("source");
        let (mut chunks, checksum) = pack(snapshot(&source, &[("data.db", &[5; 300])]), 100);
        chunks[1][10] ^= 1;
        let mut unpacker = Unpacker::create(&dir.path().join("target")).expect("unpacker");
        for data in &chunks {
            unpacker.write(data).expect("write");
        }
        let error = unpacker
            .finish(&checksum)
            .expect_err("corruption is caught");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_stream_cut_inside_a_file_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let source = dir.path().join("source");
        let (chunks, checksum) = pack(snapshot(&source, &[("data.db", &[5; 300])]), 100);
        let mut unpacker = Unpacker::create(&dir.path().join("target")).expect("unpacker");
        unpacker.write(&chunks[0]).expect("write");
        assert!(unpacker.finish(&checksum).is_err());
    }

    #[test]
    fn names_that_escape_the_directory_or_repeat_are_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut escaping = file_header("../x", 0);
        let mut unpacker = Unpacker::create(&dir.path().join("t")).expect("unpacker");
        assert!(unpacker.write(&escaping).is_err());
        escaping = file_header("a", 0);
        escaping.extend_from_slice(&file_header("a", 0));
        let mut unpacker = Unpacker::create(&dir.path().join("u")).expect("unpacker");
        assert!(unpacker.write(&escaping).is_err());
    }

    #[test]
    fn creating_replaces_what_a_failed_transfer_left() {
        let dir = tempfile::tempdir().expect("temp dir");
        let target = dir.path().join("t");
        fs::create_dir(&target).expect("dir");
        fs::write(target.join("stale"), b"x").expect("file");
        let unpacker = Unpacker::create(&target).expect("unpacker");
        assert!(!target.join("stale").exists());
        assert_eq!(unpacker.offset(), 0);
    }
}
