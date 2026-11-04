//! Label values for gRPC status codes and control actions: closed sets fixed by the code.

use fleetring_control::Action;
use tonic::Code;

/// Every value [`code_label`] returns.
pub(crate) const CODES: &[&str] = &[
    "ok",
    "cancelled",
    "unknown",
    "invalid_argument",
    "deadline_exceeded",
    "not_found",
    "already_exists",
    "permission_denied",
    "resource_exhausted",
    "failed_precondition",
    "aborted",
    "out_of_range",
    "unimplemented",
    "internal",
    "unavailable",
    "data_loss",
    "unauthenticated",
];

/// Every value [`action_label`] returns.
pub(crate) const ACTIONS: &[&str] = &[
    "join",
    "leave",
    "heartbeat",
    "create_database",
    "change_durability",
    "drop_database",
    "acquire_lease",
    "renew_lease",
    "start_promotion",
    "record_fenced",
    "complete_promotion",
    "tick",
    "drain",
    "transfer_lease",
    "revoke_token",
    "other",
];

/// A gRPC status code in lower snake case.
pub(crate) fn code_label(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

/// A control action's variant in snake case, as its serde name; `other` for a variant added
/// after this list.
pub(crate) fn action_label(action: &Action) -> &'static str {
    match action {
        Action::Join { .. } => "join",
        Action::Leave { .. } => "leave",
        Action::Heartbeat { .. } => "heartbeat",
        Action::CreateDatabase { .. } => "create_database",
        Action::ChangeDurability { .. } => "change_durability",
        Action::DropDatabase { .. } => "drop_database",
        Action::AcquireLease { .. } => "acquire_lease",
        Action::RenewLease { .. } => "renew_lease",
        Action::StartPromotion { .. } => "start_promotion",
        Action::RecordFenced { .. } => "record_fenced",
        Action::CompletePromotion { .. } => "complete_promotion",
        Action::Tick => "tick",
        Action::Drain { .. } => "drain",
        Action::TransferLease { .. } => "transfer_lease",
        Action::RevokeToken { .. } => "revoke_token",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_lower_snake_case() {
        for value in 0..=16 {
            let label = code_label(Code::from_i32(value));
            assert!(CODES.contains(&label), "{label} isn't in the closed list");
        }
        assert_eq!(code_label(Code::from_i32(99)), "unknown");
    }

    #[test]
    fn action_labels_are_the_serde_names() {
        let tick = Action::Tick;
        let serialized = serde_json::to_value(&tick).expect("an action serializes");
        assert_eq!(serialized, serde_json::json!("tick"));
        assert_eq!(action_label(&tick), "tick");
        let leave = Action::Leave {
            id: "n1".parse().expect("a node id"),
        };
        let serialized = serde_json::to_value(&leave).expect("an action serializes");
        assert!(
            serialized.get("leave").is_some(),
            "serialized as {serialized}"
        );
        assert_eq!(action_label(&leave), "leave");
        for label in ACTIONS {
            assert!(
                label.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{label} isn't lower snake case"
            );
        }
    }
}
