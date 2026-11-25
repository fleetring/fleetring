//! The screen every control command passes before it is logged.
//!
//! The control node's writer runs each submitted action through a [`Screen`] before it logs
//! anything, and a proposer that logs through another log must run the same screen, or the
//! guarantees below break. Given the state the log has reached, the commands already accepted
//! for logging after it, the action, and the time to stamp, the screen decides, purely:
//!
//! - which command to log first, if any: [`Action::EnableHandover`] before the first
//!   membership change that would keep a replica set (ADR-0031, amendment 2),
//!   [`Action::EnableIncarnations`] before the first creation under `always` (ADR-0046's FR-110
//!   amendment), and [`Action::SetTombstoneWindow`] before a drop while the state's tombstone
//!   window isn't this node's (ADR-0050);
//! - whether the action is refused before it is logged: a join, or a command that needs a fleet
//!   feature, that [`State::check_admission`] refuses (ADR-0038, point 4); and a command that
//!   needs the `delegation` fleet feature that `apply` would reject (ADR-0047, point 6.2);
//! - whether the action resends a delegation the state already records, which is answered from
//!   the state and never logged (ADR-0047, point 5.7);
//! - and the command itself, stamped with the time and this node's recorder (ADR-0047,
//!   point 6.1).
//!
//! The screen reads no clock, no randomness, and no storage: the time arrives with each action.

use fleetring::NodeId;
use fleetring_control::delegation::Resent;
use fleetring_control::{
    Action, Applied, Command, Feature, Millis, Outcome, Rejection, State, Timestamp, apply,
};

/// What this node writes into the commands it logs, beside the action: its recorder and its
/// tombstone window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Settings {
    recorder: Option<NodeId>,
    tombstone_window: Millis,
}

impl Settings {
    /// Returns the settings of a node that stamps `recorder`, if any, on every command it logs,
    /// and keeps a new tombstone for `tombstone_window`.
    pub(crate) fn new(recorder: Option<NodeId>, tombstone_window: Millis) -> Self {
        Self {
            recorder,
            tombstone_window,
        }
    }

    /// Returns `action` stamped `at` and, if this node has one, with its recorder, as every
    /// command it logs is, its own commands included (ADR-0047, point 6.1).
    fn stamp(&self, at: Timestamp, action: Action) -> Command {
        match &self.recorder {
            Some(by) => Command::stamped(at, by.clone(), action),
            None => Command::new(at, action),
        }
    }
}

/// The screen of one run of submissions against the state a log has reached.
///
/// Each call to [`Screen::screen`] sees the state after `committed` and every command the screen
/// accepted for logging before it, in order: the caller logs those commands in that order, after
/// `committed`, so the state each command is screened against is the one `apply` will see. The
/// screen builds that state, its preview, only once an action needs it.
#[derive(Debug)]
pub(crate) struct Screen<'s> {
    committed: &'s State,
    settings: &'s Settings,
    /// `committed` with the accepted commands applied, once an action needed it.
    preview: Option<State>,
    /// How many of the accepted commands the preview has applied.
    previewed: usize,
}

/// What [`Screen::screen`] decided for one action.
#[derive(Debug)]
#[must_use]
pub(crate) struct Screened {
    /// A command the screen logs before the action's, whatever the verdict: one that enables a
    /// fleet feature on its first use, or sets the tombstone window.
    pub(crate) enabling: Option<Command>,
    /// What becomes of the action itself.
    pub(crate) verdict: Verdict,
}

impl Screened {
    /// Returns the commands to log, in order: the enabling command, then the action's, as far as
    /// either exists.
    #[cfg(test)]
    fn appended(&self) -> Vec<&Command> {
        let command = match &self.verdict {
            Verdict::Append(command) => Some(command),
            Verdict::Refused(_) | Verdict::Resent { .. } => None,
        };
        self.enabling.iter().chain(command).collect()
    }
}

/// What becomes of a screened action.
#[derive(Debug)]
pub(crate) enum Verdict {
    /// Log the stamped command after the enabling command, if any.
    Append(Command),
    /// Refused before it was logged, so nothing changed for it: answer the rejection and log
    /// nothing for the action.
    Refused(Rejection),
    /// A resend of a delegation the state records: log nothing for it, and answer `outcome`,
    /// [`Resent::Yes`], once every command accepted before it is durable, since the record may be
    /// one of them.
    Resent {
        /// The stamped command, as it would have been logged.
        command: Command,
        /// The recorded delegation.
        outcome: Outcome,
    },
}

impl<'s> Screen<'s> {
    /// Returns a screen against `committed`, the state the log has reached, for a node with
    /// `settings`.
    pub(crate) fn new(committed: &'s State, settings: &'s Settings) -> Self {
        Self {
            committed,
            settings,
            preview: None,
            previewed: 0,
        }
    }

    /// Screens `action`, stamped `at`, against the state after `committed` and `accepted`.
    ///
    /// `accepted` holds every command the screen returned to log so far, enabling commands
    /// included, in order; the caller logs a [`Screened`]'s commands, in order, before it
    /// screens the next action.
    pub(crate) fn screen(
        &mut self,
        action: Action,
        at: Timestamp,
        accepted: &[Command],
    ) -> Screened {
        let settings = self.settings;
        let membership = matches!(action, Action::Join { .. } | Action::Leave { .. });
        // Before a drop, while the state's tombstone window isn't the configured one.
        let dropping = matches!(
            action,
            Action::DropDatabase { .. } | Action::ExpireDatabase { .. }
        ) && self.committed.tombstones().window() != Some(settings.tombstone_window);
        let mut enabling = None;
        if needs_screening(&action)
            || dropping
            || (membership && !self.committed.is_enabled(Feature::Handover))
        {
            let preview = self.preview(accepted);
            if needs_screening(&action)
                && let Err(rejection) = preview.check_admission(&action)
            {
                return Screened {
                    enabling: None,
                    verdict: Verdict::Refused(rejection),
                };
            }
            // The first membership change that would keep a replica set, once every member
            // supports `handover`, is preceded by the command that enables it, so that what a
            // change does depends on the log alone (ADR-0031, amendment 2). So is the first
            // creation under `always`, so that a release without `incarnations` refuses the log
            // rather than replay it to the legacy incarnation (ADR-0046's FR-110 amendment). And
            // so is a drop, while the state's window isn't this node's, by the command that sets
            // it, so that a replay never reads the configuration (ADR-0050).
            let enable = if preview.handover_due(&action) {
                Some(Action::EnableHandover)
            } else if preview.incarnations_due(&action) {
                Some(Action::EnableIncarnations)
            } else if preview.tombstones_due(&action, settings.tombstone_window) {
                Some(Action::SetTombstoneWindow {
                    window: settings.tombstone_window,
                })
            } else {
                None
            };
            if let Some(enable) = enable {
                let command = settings.stamp(at, enable);
                let _outcome = apply(preview, command.clone());
                self.previewed += 1;
                enabling = Some(command);
            }
        }
        let unlogged_if_refused = refused_unlogged(&action);
        let command = settings.stamp(at, action);
        if let Some(preview) = &mut self.preview {
            // A resend changes nothing, so it costs the log nothing either: N racing identical
            // calls log one command, not N with full claims each.
            if let Some(Ok((id, record))) = preview.delegation_resend(&command.action, command.at) {
                let outcome = Ok(Applied::DelegationRecorded {
                    id: id.clone(),
                    claims: record.claims.clone(),
                    resent: Resent::Yes,
                });
                return Screened {
                    enabling,
                    verdict: Verdict::Resent { command, outcome },
                };
            }
            let outcome = apply(preview, command.clone());
            // The preview applied exactly the command the log will hold, to exactly the state
            // `apply` will see then, so its refusal is the one the log would hold; a refusal
            // changes nothing, `State::now` included, so leaving it out replays the same.
            if unlogged_if_refused && let Err(rejection) = outcome {
                return Screened {
                    enabling,
                    verdict: Verdict::Refused(rejection),
                };
            }
            self.previewed += 1;
        }
        Screened {
            enabling,
            verdict: Verdict::Append(command),
        }
    }

    /// Returns the preview, building it from `committed` and `accepted` the first time.
    fn preview(&mut self, accepted: &[Command]) -> &mut State {
        if self.preview.is_some() {
            debug_assert_eq!(
                self.previewed,
                accepted.len(),
                "the caller logs exactly the commands the screen accepted"
            );
        }
        let committed = self.committed;
        let previewed = &mut self.previewed;
        self.preview.get_or_insert_with(|| {
            let mut state = committed.clone();
            for command in accepted {
                let _outcome = apply(&mut state, command.clone());
            }
            *previewed = accepted.len();
            state
        })
    }
}

/// Whether `action` may be refused for software, so the screen checks it before it is logged: a
/// join, or a command that needs any fleet feature.
fn needs_screening(action: &Action) -> bool {
    !action.features().is_empty() || matches!(action, Action::Join { .. })
}

/// Whether the screen refuses `action` unlogged when `apply` would reject it: a command that
/// needs the `delegation` fleet feature (ADR-0047, points 5.10 and 6.2).
///
/// An agent's token drives these, so a refusal, a rate limit's above all, must cost the log
/// nothing; and a refused command doesn't enable its feature, so logging one would leave a
/// command the previous release line can't decode in a log it may still roll back to
/// (ADR-0038, point 4). Every such command [`needs_screening`], so the preview exists.
fn refused_unlogged(action: &Action) -> bool {
    action.features().contains(&Feature::Delegation)
}

#[cfg(test)]
mod tests {
    use fleetring::ReplicationFactor;
    use fleetring_control::delegation::StructureViolation;
    use fleetring_control::{
        ControlConfig, DEFAULT_TOMBSTONE_WINDOW, Engine, Incarnation, IncarnationRule, Member,
        Release, Software,
    };

    use super::*;
    use crate::control::test_support::{member_with_every_feature, root_delegation};

    const AT: Timestamp = Timestamp::from_millis(1_000_000);

    fn recorder() -> NodeId {
        "control-1".parse().expect("an id")
    }

    fn settings() -> Settings {
        Settings::new(Some(recorder()), DEFAULT_TOMBSTONE_WINDOW)
    }

    fn join(member: Member) -> Action {
        Action::Join { member }
    }

    fn create(name: &str, rule: IncarnationRule) -> Action {
        Action::CreateDatabase {
            name: name.parse().expect("a name"),
            replication_factor: ReplicationFactor::new(3).expect("nonzero"),
            engine: Engine::Sqlite,
            durability: None,
            metadata: None,
            incarnation: match rule {
                IncarnationRule::Always => Incarnation::new(0x22),
                _ => Incarnation::LEGACY,
            },
            incarnation_rule: rule,
        }
    }

    /// The state after `actions`, each applied, unscreened, and accepted.
    fn state_after(actions: impl IntoIterator<Item = Action>) -> State {
        let mut state = State::new(ControlConfig::new());
        for action in actions {
            let outcome = apply(&mut state, Command::new(AT, action));
            assert!(outcome.is_ok(), "{outcome:?}");
        }
        state
    }

    /// Screens `action` and returns the verdict's command, which must be appended.
    fn appended(screened: Screened) -> Command {
        match screened.verdict {
            Verdict::Append(command) => command,
            other => panic!("expected the command to be appended: {other:?}"),
        }
    }

    /// A command nothing gates passes through unchanged: no enabling command, the action as
    /// submitted, stamped with the time and the recorder.
    #[test]
    fn a_plain_command_passes_through() {
        let state = state_after([join(member_with_every_feature("data-1"))]);
        let settings = settings();
        let mut screen = Screen::new(&state, &settings);
        let screened = screen.screen(Action::Tick, AT, &[]);
        assert!(screened.enabling.is_none(), "{screened:?}");
        let command = appended(screened);
        assert_eq!(command, Command::stamped(AT, recorder(), Action::Tick));
    }

    /// Every command the screen returns carries this node's recorder, its enabling commands
    /// included, and none without one (ADR-0047, point 6.1).
    #[test]
    fn the_screen_stamps_the_recorder() {
        let state = state_after([join(member_with_every_feature("data-1"))]);
        let action = create("fresh", IncarnationRule::Always);

        let settings = settings();
        let screened = Screen::new(&state, &settings).screen(action.clone(), AT, &[]);
        let stamps: Vec<Option<&NodeId>> = screened
            .appended()
            .iter()
            .map(|command| command.recorded_by.as_ref())
            .collect();
        assert_eq!(stamps, [Some(&recorder()), Some(&recorder())]);

        let unstamped = Settings::new(None, DEFAULT_TOMBSTONE_WINDOW);
        let screened = Screen::new(&state, &unstamped).screen(action, AT, &[]);
        let stamps: Vec<Option<&NodeId>> = screened
            .appended()
            .iter()
            .map(|command| command.recorded_by.as_ref())
            .collect();
        assert_eq!(stamps, [None, None]);
    }

    /// The first join that would move a database that may keep its replica set, once every
    /// member supports `handover`, follows an `EnableHandover` the screen adds (ADR-0031,
    /// amendment 2); a later one, screened behind it, doesn't.
    #[test]
    fn handover_is_enabled_before_the_first_change_that_keeps_a_set() {
        let member = member_with_every_feature;
        // A database whose replica set `d`'s join moves, with a lease, so it may keep its set.
        let (state, joining) = (0..200)
            .find_map(|n| {
                let name = format!("t{n}");
                let database: fleetring_control::DatabaseName = name.parse().expect("a name");
                let mut state = state_after([
                    join(member("a")),
                    join(member("b")),
                    join(member("c")),
                    create(&name, IncarnationRule::OnceForked),
                ]);
                let before = state.replica_set(&database).ok()?;
                let lease = Action::AcquireLease {
                    database: database.clone(),
                    node: before[0].clone(),
                };
                apply(&mut state, Command::new(AT, lease)).ok()?;
                let mut moved = state.clone();
                apply(&mut moved, Command::new(AT, join(member("d")))).ok()?;
                let after = moved.replica_set(&database).ok()?;
                (after != before).then_some((state, join(member("d"))))
            })
            .expect("a name that d's join moves");

        let settings = settings();
        let mut screen = Screen::new(&state, &settings);
        let screened = screen.screen(joining, AT, &[]);
        let actions: Vec<&Action> = screened
            .appended()
            .iter()
            .map(|command| &command.action)
            .collect();
        assert!(
            matches!(
                actions[..],
                [Action::EnableHandover, Action::Join { member }] if member.id().as_str() == "d"
            ),
            "{actions:?}"
        );
        let accepted: Vec<Command> = screened.appended().into_iter().cloned().collect();
        let screened = screen.screen(join(member("e")), AT, &accepted);
        assert!(screened.enabling.is_none(), "{screened:?}");
        let _join = appended(screened);
    }

    /// The first creation under `always`, once every member supports `incarnations`, follows an
    /// `EnableIncarnations` the screen adds (ADR-0046's FR-110 amendment); while a member lacks
    /// the feature, the creation is refused unlogged instead.
    #[test]
    fn incarnations_are_enabled_before_the_first_creation_under_always() {
        let state = state_after([join(member_with_every_feature("a"))]);
        let settings = settings();
        let screened = Screen::new(&state, &settings).screen(
            create("fresh", IncarnationRule::Always),
            AT,
            &[],
        );
        let actions: Vec<&Action> = screened
            .appended()
            .iter()
            .map(|command| &command.action)
            .collect();
        assert!(
            matches!(
                actions[..],
                [Action::EnableIncarnations, Action::CreateDatabase { .. }]
            ),
            "{actions:?}"
        );

        let without = Feature::ALL
            .into_iter()
            .filter(|feature| *feature != Feature::Incarnations)
            .fold(
                Software::new(Release::new(0, 8, 0), 1),
                Software::with_feature,
            );
        let old = Member::new("b".parse().expect("an id")).with_software(without);
        let state = state_after([join(member_with_every_feature("a")), join(old)]);
        let screened = Screen::new(&state, &settings).screen(
            create("early", IncarnationRule::Always),
            AT,
            &[],
        );
        assert!(screened.enabling.is_none(), "{screened:?}");
        assert!(
            matches!(
                screened.verdict,
                Verdict::Refused(Rejection::FeatureUnsupported {
                    feature: Feature::Incarnations,
                    ..
                })
            ),
            "{screened:?}"
        );
    }

    /// A drop while the state's tombstone window isn't this node's follows a
    /// `SetTombstoneWindow` with this node's window (ADR-0050); a drop screened behind it
    /// doesn't.
    #[test]
    fn the_tombstone_window_is_set_before_a_drop() {
        let state = state_after([
            join(member_with_every_feature("a")),
            create("one", IncarnationRule::OnceForked),
            create("two", IncarnationRule::OnceForked),
        ]);
        let window = Millis::new(60_000);
        let settings = Settings::new(Some(recorder()), window);
        let mut screen = Screen::new(&state, &settings);
        let drop_one = Action::DropDatabase {
            name: "one".parse().expect("a name"),
        };
        let screened = screen.screen(drop_one, AT, &[]);
        let actions: Vec<&Action> = screened
            .appended()
            .iter()
            .map(|command| &command.action)
            .collect();
        assert!(
            matches!(
                actions[..],
                [Action::SetTombstoneWindow { window: set }, Action::DropDatabase { .. }]
                    if *set == window
            ),
            "{actions:?}"
        );
        let accepted: Vec<Command> = screened.appended().into_iter().cloned().collect();
        let drop_two = Action::DropDatabase {
            name: "two".parse().expect("a name"),
        };
        let screened = screen.screen(drop_two, AT, &accepted);
        assert!(screened.enabling.is_none(), "{screened:?}");
        let _drop = appended(screened);
    }

    /// A join `check_admission` refuses is refused, and the screen goes on from the state
    /// before it.
    #[test]
    fn a_join_outside_the_window_is_refused() {
        let state = state_after([join(member_with_every_feature("a"))]);
        let settings = settings();
        let mut screen = Screen::new(&state, &settings);
        let far = Member::new("far".parse().expect("an id"))
            .with_software(Software::new(Release::new(9, 0, 0), 1));
        let screened = screen.screen(join(far), AT, &[]);
        assert!(screened.enabling.is_none(), "{screened:?}");
        assert!(
            matches!(
                screened.verdict,
                Verdict::Refused(Rejection::IncompatibleVersion { .. })
            ),
            "{screened:?}"
        );
        let _join = appended(screen.screen(join(member_with_every_feature("b")), AT, &[]));
    }

    /// A delegation `apply` would reject is refused unlogged with `apply`'s rejection
    /// (ADR-0047, point 6.2), and leaves the preview as it was: the same delegation, corrected,
    /// is then appended.
    #[test]
    fn a_rejected_delegation_is_refused_unlogged() {
        let state = state_after([join(member_with_every_feature("data-1"))]);
        let settings = settings();
        let mut screen = Screen::new(&state, &settings);
        let mut planted = root_delegation(AT, 1, 60_000);
        if let Action::RecordDelegation { requested_by, .. } = &mut planted {
            *requested_by = "agent-43".parse().expect("a name");
        }
        let screened = screen.screen(planted, AT, &[]);
        assert!(screened.enabling.is_none(), "{screened:?}");
        assert!(
            matches!(
                screened.verdict,
                Verdict::Refused(Rejection::DelegationStructure {
                    reason: StructureViolation::RequesterNotCaller
                })
            ),
            "{screened:?}"
        );
        let screened = screen.screen(root_delegation(AT, 1, 60_000), AT, &[]);
        let _recorded = appended(screened);
    }

    /// A delegation the state records, or one the screen accepted earlier in the run, is a
    /// resend: answered `Resent::Yes` with the recorded delegation, and never appended
    /// (ADR-0047, point 5.7).
    #[test]
    fn a_resend_is_answered_from_the_state() {
        let state = state_after([join(member_with_every_feature("data-1"))]);
        let settings = settings();
        let action = root_delegation(AT, 1, 60_000);
        let mut screen = Screen::new(&state, &settings);
        let first = appended(screen.screen(action.clone(), AT, &[]));
        let accepted = [first];
        let resent = |screened: Screened| match screened.verdict {
            Verdict::Resent {
                command,
                outcome:
                    Ok(Applied::DelegationRecorded {
                        resent: Resent::Yes,
                        ..
                    }),
            } => command,
            other => panic!("expected a resend: {other:?}"),
        };
        // Behind the accepted record, in the same run.
        let command = resent(screen.screen(action.clone(), AT, &accepted));
        assert_eq!(command.action, action);

        // Once the record is in the state the log reached.
        let mut committed = state.clone();
        let [first] = accepted;
        let outcome = apply(&mut committed, first);
        assert!(outcome.is_ok(), "{outcome:?}");
        let mut screen = Screen::new(&committed, &settings);
        let _command = resent(screen.screen(action, AT, &[]));
    }

    /// The preview includes the commands accepted before an action needed it, so an action is
    /// screened against the state the log will have reached, not the committed state alone: a
    /// delegation the committed state refuses for a member without `delegation` is appended
    /// behind that member's accepted leave.
    #[test]
    fn the_preview_includes_what_was_accepted_before_it() {
        let without = Feature::ALL
            .into_iter()
            .filter(|feature| *feature != Feature::Delegation)
            .fold(
                Software::new(Release::new(0, 8, 0), 1),
                Software::with_feature,
            );
        let old = Member::new("old".parse().expect("an id")).with_software(without);
        // With `handover` enabled, a leave needs no screening and builds no preview.
        let state = state_after([
            join(member_with_every_feature("a")),
            join(old),
            Action::EnableHandover,
        ]);
        let settings = settings();
        let delegation = root_delegation(AT, 1, 60_000);
        let refused = Screen::new(&state, &settings).screen(delegation.clone(), AT, &[]);
        assert!(
            matches!(
                refused.verdict,
                Verdict::Refused(Rejection::FeatureUnsupported {
                    feature: Feature::Delegation,
                    ..
                })
            ),
            "{refused:?}"
        );

        let mut screen = Screen::new(&state, &settings);
        let leave = Action::Leave {
            id: "old".parse().expect("an id"),
        };
        let left = appended(screen.screen(leave, AT, &[]));
        assert!(screen.preview.is_none(), "a leave builds no preview");
        let _recorded = appended(screen.screen(delegation, AT, &[left]));
    }
}
