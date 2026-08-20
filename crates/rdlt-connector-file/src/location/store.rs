//! The object-store recoverability rule.
//!
//! Vendored from the SPI, which stopped lending it when connectors moved
//! to their own repository. Every connector here that talks to an object
//! store must answer this question the same way — one copy, so a second
//! cannot drift and leave one connector burning its retry budget on a
//! certainty while another reports the true cause.

/// Is this store failure worth another attempt?
///
/// An ALLOW-LIST, deliberately. Every variant outside it states a
/// determined fact about the request or the configuration — a missing
/// object, a rejected credential, an unusable path, an unsupported
/// operation, an unknown setting — and retrying one spends the host's
/// budget on a certainty, then reports transient exhaustion in place of
/// the real cause. The allow-list is also the safe posture toward a
/// `#[non_exhaustive]` upstream enum: a variant added by a future release
/// costs one retry that would not have helped, never a hidden one that
/// would.
///
/// The three recoverable variants:
/// - `Generic` wraps transport-level failure — the textbook retry.
/// - `AlreadyExists` (HTTP 409) and `Precondition` (HTTP 412) are
///   determined answers only to a CONDITIONAL request, and these
///   connectors issue none. What an unconditional put, copy, or delete
///   gets a 409 for is S3's `OperationAborted` ("a conflicting
///   conditional operation is in progress; try again") — a retry-me
///   condition the store's own retry loop does not cover, since it
///   retries 409 only for its conditional put.
pub fn is_recoverable(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::Generic { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_failure_is_the_open_ended_recoverable_case() {
        assert!(is_recoverable(&object_store::Error::Generic {
            store: "S3",
            source: "connection reset by peer".into(),
        }));
    }

    #[test]
    fn determined_answers_never_ride_the_retry_budget() {
        for determined in [
            object_store::Error::NotFound {
                path: "x".into(),
                source: "gone".into(),
            },
            object_store::Error::InvalidPath {
                source: object_store::path::Error::EmptySegment {
                    path: "a//b".into(),
                },
            },
            object_store::Error::NotSupported {
                source: "range write".into(),
            },
            object_store::Error::NotImplemented,
            object_store::Error::UnknownConfigurationKey {
                store: "S3",
                key: "nope".into(),
            },
            object_store::Error::PermissionDenied {
                path: "x".into(),
                source: "denied".into(),
            },
            object_store::Error::Unauthenticated {
                path: "x".into(),
                source: "bad key".into(),
            },
        ] {
            let rendered = determined.to_string();
            assert!(
                !is_recoverable(&determined),
                "`{rendered}` cannot heal on retry"
            );
        }
    }

    #[test]
    fn conflicts_on_unconditional_requests_heal() {
        // 409/412 are determined answers only to CONDITIONAL requests,
        // which these connectors never issue — what reaches this rule is
        // the store's try-again conflict.
        for conflict in [
            object_store::Error::AlreadyExists {
                path: "x".into(),
                source: "operation aborted".into(),
            },
            object_store::Error::Precondition {
                path: "x".into(),
                source: "conflicting operation".into(),
            },
        ] {
            let rendered = conflict.to_string();
            assert!(is_recoverable(&conflict), "`{rendered}` heals on retry");
        }
    }

    /// The rule reads the error's SHAPE, never its rendered text — a
    /// message-keyed policy would change silently with any service or
    /// library rewording.
    #[test]
    fn classification_ignores_the_message() {
        assert!(is_recoverable(&object_store::Error::Generic {
            store: "S3",
            source: "".into(),
        }));
    }
}
