# FR-097 trial notes

- **Trialist:**
- **Date:**
- **Build:** (the output of `fleetring --version`)

For each step: the clock time at the start and end, the commands you ran, every message **verbatim**, what confused you, and what you expected instead. Don't tidy anything up; the rough version is the useful one.

## 1. Fork it

- Start / end:
- Commands:
- Messages (verbatim):
- Was the operation ID clear? The progress? The final state?
- Confusing / expected instead:

## 2. Experiment on the fork

- Start / end:
- Commands:
- Messages (verbatim):
- Confusing / expected instead:

## 3. Inspect status

- Start / end:
- Commands:
- Messages (verbatim):
- Could you tell where the fork came from (source, incarnation, position)? Whether it was usable?
- Confusing / expected instead:

## 4. Cancel and retry

- Start / end:
- Ctrl-C, then finding and resuming the operation:
- Cancelling another fork:
- Resuming the cancelled operation:
- Retrying with a different argument (the conflict message):
- Messages (verbatim):
- Confusing / expected instead:

## 5. Clean up

- Start / end:
- Commands:
- Did the disk space come back? What did `orphan list` say?
- Messages (verbatim):
- Confusing / expected instead:

## 6. A failure on purpose

- Start / end:
- Commands:
- Messages (verbatim):
- Could you tell what went wrong, and what to do?
- Confusing / expected instead:

## Words

Did these need explaining? In what words would you explain them to someone else?

- `seeding`:
- `seeded` / opening:
- `ready`:
- `failed` / `cancelled`:
- the operation ID:

## Anything else
