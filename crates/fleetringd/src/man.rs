//! The man pages, one per command (`fleetring-db-create.1`), rendered from the command tree
//! with `clap_mangen` (`docs/cli.md`, section 9).
//!
//! They are checked in under `crates/fleetringd/man/`, where the release workflow takes them
//! into every archive's `man/` directory. A test fails when a page there differs from what the
//! tree renders; `UPDATE_GOLDEN=1` rewrites them, and a reviewer reads the diff. The pages name
//! no release, so a version bump changes none of them.

use std::io;

use clap_mangen::Man;

use crate::compat::ProgramName;
use crate::help;

/// The directory the pages are checked in to.
const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/man");

/// Every page, by file name, in command-tree order: `fleetring.1`, then each command's.
fn pages() -> io::Result<Vec<(String, Vec<u8>)>> {
    let mut root = without_help(help::root_command(ProgramName::Fleetring));
    root.build();
    let mut pages = Vec::new();
    collect(&root, &mut pages)?;
    Ok(pages)
}

/// `command` without clap's `help` subcommands, which a man page has no use for.
fn without_help(command: clap::Command) -> clap::Command {
    let names: Vec<String> = command
        .get_subcommands()
        .map(|sub| sub.get_name().to_owned())
        .collect();
    let mut command = command.disable_help_subcommand(true);
    for name in names {
        command = command.mut_subcommand(name, without_help);
    }
    command
}

fn collect(command: &clap::Command, pages: &mut Vec<(String, Vec<u8>)>) -> io::Result<()> {
    let man = Man::new(command.clone())
        .source("FleetRing")
        .manual("FleetRing manual");
    pages.push((man.get_filename(), render(&man)?));
    for sub in command.get_subcommands() {
        collect(sub, pages)?;
    }
    Ok(())
}

/// The section `Man::render` ends `fleetring.1` with, which names the release.
const VERSION_SECTION: &str = "\n.SH VERSION\n";

/// The page, as `Man::render` writes it but without the version section, which would change
/// the root page at every release.
fn render(man: &Man) -> io::Result<Vec<u8>> {
    let mut page = Vec::new();
    man.render(&mut page)?;
    let mut page = String::from_utf8(page).map_err(io::Error::other)?;
    if let Some(start) = page.find(VERSION_SECTION) {
        let rest = &page[start + VERSION_SECTION.len()..];
        let end = rest
            .find("\n.SH ")
            .map_or(page.len(), |next| start + VERSION_SECTION.len() + next);
        let keep = if end == page.len() { "\n" } else { "" };
        page.replace_range(start..end, keep);
    }
    Ok(page.into_bytes())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;

    use super::*;

    #[test]
    fn the_checked_in_pages_match_the_command_tree() -> Result<(), Box<dyn std::error::Error>> {
        let pages = pages()?;
        let dir = Path::new(DIR);
        if std::env::var_os("UPDATE_GOLDEN").is_some() {
            if dir.exists() {
                std::fs::remove_dir_all(dir)?;
            }
            std::fs::create_dir_all(dir)?;
            for (name, page) in &pages {
                std::fs::write(dir.join(name), page)?;
            }
        }
        let rendered: BTreeSet<&str> = pages.iter().map(|(name, _)| name.as_str()).collect();
        let mut checked_in = BTreeSet::new();
        for entry in std::fs::read_dir(dir)? {
            checked_in.insert(entry?.file_name().to_string_lossy().into_owned());
        }
        let checked_in: BTreeSet<&str> = checked_in.iter().map(String::as_str).collect();
        assert_eq!(
            rendered, checked_in,
            "the pages differ from {DIR}; regenerate them with UPDATE_GOLDEN=1"
        );
        for (name, page) in &pages {
            let golden = std::fs::read(dir.join(name))?;
            assert!(
                *page == golden,
                "{name} changed; regenerate {DIR} with UPDATE_GOLDEN=1 and review the diff"
            );
        }
        Ok(())
    }

    #[test]
    fn every_command_has_a_page() -> io::Result<()> {
        fn names(command: &clap::Command, prefix: &str, found: &mut Vec<String>) {
            let name = if prefix.is_empty() {
                command.get_name().to_owned()
            } else {
                format!("{prefix}-{}", command.get_name())
            };
            for sub in command.get_subcommands() {
                if sub.get_name() != "help" {
                    names(sub, &name, found);
                }
            }
            found.push(format!("{name}.1"));
        }
        let mut expected = Vec::new();
        names(
            &help::root_command(ProgramName::Fleetring),
            "",
            &mut expected,
        );
        let pages = pages()?;
        for name in expected {
            assert!(
                pages.iter().any(|(page, _)| *page == name),
                "no page {name}"
            );
        }
        Ok(())
    }

    #[test]
    fn the_db_create_page_names_its_flags() -> Result<(), Box<dyn std::error::Error>> {
        let pages = pages()?;
        let (_, page) = pages
            .iter()
            .find(|(name, _)| name == "fleetring-db-create.1")
            .ok_or("no fleetring-db-create.1")?;
        let page = String::from_utf8(page.clone())?;
        assert!(page.contains("\n.TH fleetring-db-create 1"), "{page}");
        for flag in [
            "replication\\-factor",
            "acks",
            "min\\-replicas",
            "description",
            "expires\\-in",
            "context",
        ] {
            assert!(page.contains(&format!("\\-\\-{flag}")), "--{flag}:\n{page}");
        }
        Ok(())
    }

    #[test]
    fn the_root_page_lists_the_exit_codes() -> Result<(), Box<dyn std::error::Error>> {
        let pages = pages()?;
        let (_, page) = pages.first().ok_or("no pages")?;
        let page = String::from_utf8(page.clone())?;
        assert!(page.contains("\n.TH fleetring 1"), "{page}");
        assert!(!page.contains(".SH VERSION"), "{page}");
        assert!(page.contains("Exit codes:"), "{page}");
        Ok(())
    }
}
