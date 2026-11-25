# A format-2 control log that 0.9.0 opened

`control.db` is `control-0.9.0-migrated/control.db` after `fleetringd-v0.9.0`'s own `CommandLog::open` refused it, leaving the empty `commands` table behind; `scripts/write-control-fixtures.sh stray` wrote it with `control-writers/open_format2_0_9_0.rs`. It is never regenerated.
