# FR-097 (#147): the fork usability trial

You are trying forks cold, the way a person who has never seen them would. **Work only from `fleetring --help` (and `--help` on each command), `docs/cli.md`, and the messages the CLI prints.** Don't read the source, the ADRs, or the plans, and don't ask an agent how forks work. When something is unclear, that's a finding: write it down before you work it out.

It takes about 45 minutes.

## Before you start

1. `bash experiments/fr097/kit.sh setup`, from the repository root (`release/0.8.0`). It builds `fleetring`, starts an isolated dev node, and fills two databases: `notes` (about 300 MiB of notes) and `archive` (about 1.5 GiB, notes plus attachments). It takes a few minutes and never touches your own `~/.fleetring`.
2. Open a **new terminal** and run `. /tmp/fr097/env.sh`. Do the trial in that terminal.
3. Copy `experiments/fr097/NOTES.md` to `/tmp/fr097/NOTES.md` and fill it in as you go. Paste messages **verbatim**, and note the clock time at the start and end of each step.

## The steps

1. **Fork it.** Make a fork of `notes` called `notes-try`. Can you understand the operation ID, the progress, and the final state? How long did each part take?
2. **Experiment on the fork.** Change `notes-try`'s schema and write some rows. Then confirm that `notes` is unchanged.
3. **Inspect status.** Using `db show notes-try` and `db list`: can you tell where the fork came from, and whether it's usable?
4. **Cancel and retry.**
   - Start a fork of `archive`, and press Ctrl-C in the terminal while it's still running. Then find the operation and resume it.
   - Start another fork and cancel it.
   - Try to resume the operation you cancelled.
   - Retry an operation with a deliberately different argument, and read the conflict message.
5. **Clean up.** Drop every fork. Does the disk space come back? Is `orphan list` empty, or understandable if it isn't?
6. **Hit a failure on purpose.** In your **first** terminal, run `bash experiments/fr097/kit.sh tight-disk`, which restarts the node so that no fork has enough disk. Back in the trial terminal, try a fork. Can you tell what went wrong and what you'd do about it? Then run `bash experiments/fr097/kit.sh normal` in the first terminal.

## Afterwards

- Hand `/tmp/fr097/NOTES.md` back. An agent turns it into `docs/experiments/0.8-fork-usability.md`, files each confusion as an issue labelled `fork` and `ux`, and takes the agreed words into the docs (FR-095).
- `bash experiments/fr097/kit.sh teardown` stops the node and deletes `/tmp/fr097`. Copy your notes out first.
