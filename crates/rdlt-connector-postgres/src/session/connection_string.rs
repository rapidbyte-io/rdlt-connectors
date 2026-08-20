//! The connection-string parse gate for both connectors: extract libpq's TLS
//! parameters from either libpq spelling (URL or `key=value`), hand the
//! remainder to the driver, reconcile the extractions with the `tls:` block
//! under the same agree-or-error rules `sslmode` follows — and make sure no
//! rejection is ever a bare parse error.

use crate::tls;

/// A fully parsed connection: the driver config (TLS parameters stripped,
/// `application_name` defaulted) plus the effective TLS policy.
#[derive(Debug)]
pub struct Parsed {
    pub driver: tokio_postgres::Config,
    pub tls: tls::Policy,
}

/// THE parse gate. Every consumer of a user connection string — source,
/// destination, CDC — goes through here, so an unsupported parameter, a
/// malformed escape, or a policy contradiction is refused identically
/// everywhere, before any connection exists.
pub fn parse(
    connection_string: &str,
    tls_override: Option<&tls::Policy>,
) -> Result<Parsed, tls::ConfigError> {
    let extracted = extract_tls_parameters(connection_string);
    if let Some((parameter, value)) = &extracted.malformed_escape {
        return Err(tls::ConfigError::Syntax(format!(
            "malformed percent-escape in `{parameter}` value `{value}`"
        )));
    }
    let mut driver: tokio_postgres::Config = extracted.remainder.parse().map_err(|e| {
        // A driver rejection must name the parameter when one is the cause —
        // scan the keys we KEPT for anything outside the driver's set.
        for key in &extracted.driver_keys {
            if !DRIVER_PARAMETERS.contains(&key.as_str()) {
                return tls::ConfigError::UnsupportedParameter {
                    parameter: key.clone(),
                    hint: unsupported_parameter_hint(key),
                };
            }
        }
        tls::ConfigError::Syntax(format!("{e}"))
    })?;
    let mut policy = tls::resolve(&driver, tls_override)?;

    // A connection-string `sslmode=verify-*` (libpq spelling the driver
    // rejects) sets the mode; a block may keep or STRENGTHEN it, never
    // weaken it — the same never-silently-reversed rule as the other
    // sslmode values.
    if let Some(verify_mode) = extracted.verify_mode {
        match tls_override {
            None => policy.mode = verify_mode,
            Some(block) if block.mode.strength() >= verify_mode.strength() => {}
            Some(block) => {
                return Err(tls::ConfigError::Contradiction {
                    connection_sslmode: if verify_mode == tls::Mode::VerifyCa {
                        "verify-ca"
                    } else {
                        "verify-full"
                    },
                    override_mode: block.mode,
                });
            }
        }
    }

    // Merge each extracted PEM parameter into the policy: a connection value
    // fills an absent block field, agreeing duplicates pass, disagreement is
    // a typed conflict.
    merge_pem_parameter(
        "sslrootcert",
        "root_cert",
        extracted.root_cert,
        &mut policy.root_cert,
    )?;
    merge_pem_parameter(
        "sslcert",
        "client_cert",
        extracted.client_cert,
        &mut policy.client_cert,
    )?;
    merge_pem_parameter(
        "sslkey",
        "client_key",
        extracted.client_key,
        &mut policy.client_key,
    )?;
    // The both-or-neither credential rule holds ACROSS sources of the values.
    tls::validate_credentials(&policy)?;

    // Identify ourselves unless the user chose a name.
    if driver.get_application_name().is_none() {
        driver.application_name("rdlt");
    }
    Ok(Parsed {
        driver,
        tls: policy,
    })
}

/// Merge one extracted PEM parameter with its policy field. `sslrootcert=
/// system` (libpq 16+) means the platform store — our native-roots default,
/// i.e. an EMPTY `root_cert`.
fn merge_pem_parameter(
    parameter: &'static str,
    policy_field: &'static str,
    connection_value: Option<String>,
    field: &mut Option<tls::Material>,
) -> Result<(), tls::ConfigError> {
    let Some(connection_value) = connection_value else {
        return Ok(());
    };
    if parameter == "sslrootcert" && connection_value == "system" {
        if let Some(existing) = field {
            return Err(tls::ConfigError::ParameterConflict {
                parameter,
                policy_field,
                connection_value,
                override_value: existing.describe(),
            });
        }
        return Ok(());
    }
    match field {
        Some(existing) if existing.as_str() != connection_value => {
            Err(tls::ConfigError::ParameterConflict {
                parameter,
                policy_field,
                connection_value,
                override_value: existing.describe(),
            })
        }
        Some(_) => Ok(()),
        None => {
            *field = Some(tls::Material::new(connection_value));
            Ok(())
        }
    }
}

/// The driver's accepted parameter set (tokio-postgres 0.7) — used ONLY to
/// name the offending key when the driver rejects a string.
const DRIVER_PARAMETERS: &[&str] = &[
    "user",
    "password",
    "dbname",
    "options",
    "application_name",
    "sslmode",
    "host",
    "hostaddr",
    "port",
    "connect_timeout",
    "tcp_user_timeout",
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_retries",
    "target_session_attrs",
    "channel_binding",
    "load_balance_hosts",
];

fn unsupported_parameter_hint(parameter: &str) -> String {
    match parameter {
        "sslpassword" => "encrypted client keys are unsupported — provide an \
                          unencrypted PKCS#8/RSA/SEC1 key via tls.client_key"
            .into(),
        "gssencmode" | "krbsrvname" | "gsslib" => "GSS/Kerberos transport is not supported".into(),
        "requiressl" => "use sslmode= (requiressl is the pre-7.2 libpq spelling)".into(),
        "sslcrl" | "sslcrldir" => "certificate revocation lists are not supported".into(),
        "service" => "libpq service files are not read — inline the parameters".into(),
        other => format!("`{other}` has no rdlt equivalent; see the supported parameter list"),
    }
}

/// What extraction found: the string the driver will see, the TLS parameters
/// pulled out of it, and everything needed to blame the right key later.
struct Extracted {
    /// The connection string with the TLS parameters removed.
    remainder: String,
    /// First malformed percent-escape seen in an EXTRACTED value:
    /// (parameter, raw value) — surfaced as a typed error.
    malformed_escape: Option<(String, String)>,
    /// Keys seen and KEPT for the driver (extracted ones are excluded —
    /// they cannot be the cause of a driver rejection).
    driver_keys: Vec<String>,
    root_cert: Option<String>,
    client_cert: Option<String>,
    client_key: Option<String>,
    /// `sslmode=verify-ca|verify-full` — libpq spellings the driver does not
    /// accept; translated into the policy mode.
    verify_mode: Option<tls::Mode>,
}

impl Extracted {
    fn empty() -> Self {
        Self {
            remainder: String::new(),
            malformed_escape: None,
            driver_keys: Vec::new(),
            root_cert: None,
            client_cert: None,
            client_key: None,
            verify_mode: None,
        }
    }

    /// Route one key/value: capture it if it is ours, report `false` so the
    /// caller keeps it for the driver otherwise.
    fn capture(&mut self, key: &str, value: String) -> bool {
        match key {
            "sslrootcert" => self.root_cert = Some(value),
            "sslcert" => self.client_cert = Some(value),
            "sslkey" => self.client_key = Some(value),
            "sslmode" if value == "verify-ca" => self.verify_mode = Some(tls::Mode::VerifyCa),
            "sslmode" if value == "verify-full" => self.verify_mode = Some(tls::Mode::VerifyFull),
            _ => return false,
        }
        true
    }
}

/// Strip the TLS parameters from either libpq spelling, remembering every
/// kept key so a later driver rejection can be blamed on the right one.
fn extract_tls_parameters(connection_string: &str) -> Extracted {
    if connection_string.contains("://") {
        extract_from_url(connection_string)
    } else {
        extract_from_key_values(connection_string)
    }
}

/// URL form: rewrite the query string, percent-decoding captured values
/// (the driver decodes the ones it keeps).
fn extract_from_url(connection_string: &str) -> Extracted {
    let mut extracted = Extracted::empty();
    let (base, query) = match connection_string.split_once('?') {
        Some((base, query)) => (base, query),
        None => (connection_string, ""),
    };
    let mut kept: Vec<&str> = Vec::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        let value = match percent_decode(raw_value) {
            Ok(value) => value,
            Err(()) => {
                if extracted.malformed_escape.is_none() {
                    extracted.malformed_escape = Some((key.to_string(), raw_value.to_string()));
                }
                raw_value.to_string()
            }
        };
        if !extracted.capture(key, value) {
            extracted.driver_keys.push(key.to_string());
            kept.push(pair);
        }
    }
    extracted.remainder = if kept.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", kept.join("&"))
    };
    extracted
}

/// `key=value` form: quote-aware scan (libpq allows `key = 'a value'` with
/// backslash-escaped quotes inside).
fn extract_from_key_values(connection_string: &str) -> Extracted {
    let mut extracted = Extracted::empty();
    let bytes = connection_string.as_bytes();
    let mut kept_spans: Vec<(usize, usize)> = Vec::new();
    let mut position = 0usize;
    while position < bytes.len() {
        while position < bytes.len() && bytes[position].is_ascii_whitespace() {
            position += 1;
        }
        if position >= bytes.len() {
            break;
        }
        let token_start = position;
        while position < bytes.len()
            && bytes[position] != b'='
            && !bytes[position].is_ascii_whitespace()
        {
            position += 1;
        }
        let key = connection_string[token_start..position].to_string();
        while position < bytes.len() && bytes[position].is_ascii_whitespace() {
            position += 1;
        }
        if position < bytes.len() && bytes[position] == b'=' {
            position += 1;
            while position < bytes.len() && bytes[position].is_ascii_whitespace() {
                position += 1;
            }
            let value = if position < bytes.len() && bytes[position] == b'\'' {
                position += 1;
                let value_start = position;
                while position < bytes.len() && bytes[position] != b'\'' {
                    position += if bytes[position] == b'\\' { 2 } else { 1 };
                }
                let value =
                    connection_string[value_start..position.min(bytes.len())].replace("\\'", "'");
                position = (position + 1).min(bytes.len());
                value
            } else {
                let value_start = position;
                while position < bytes.len() && !bytes[position].is_ascii_whitespace() {
                    position += 1;
                }
                connection_string[value_start..position].to_string()
            };
            if !extracted.capture(&key, value) {
                extracted.driver_keys.push(key);
                kept_spans.push((token_start, position));
            }
        } else {
            // Not a key=value token (malformed) — keep it for the driver's
            // syntax error to describe.
            kept_spans.push((token_start, position));
        }
    }
    extracted.remainder = kept_spans
        .iter()
        .map(|&(start, end)| &connection_string[start..end])
        .collect::<Vec<_>>()
        .join(" ");
    extracted
}

/// Strict: a `%` not followed by two hex digits is an error, never a silent
/// literal passthrough — a half-typed escape in a certificate path must not
/// quietly become a different path.
fn percent_decode(value: &str) -> Result<String, ()> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut position = 0;
    while position < bytes.len() {
        if bytes[position] == b'%' {
            let (Some(high), Some(low)) = (
                bytes
                    .get(position + 1)
                    .and_then(|b| (*b as char).to_digit(16)),
                bytes
                    .get(position + 2)
                    .and_then(|b| (*b as char).to_digit(16)),
            ) else {
                return Err(());
            };
            decoded.push((high * 16 + low) as u8);
            position += 3;
            continue;
        }
        decoded.push(bytes[position]);
        position += 1;
    }
    Ok(String::from_utf8_lossy(&decoded).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::{Material, Mode, Policy};

    #[test]
    fn malformed_percent_escapes_are_typed_errors() {
        // Never a silent literal passthrough.
        for bad in [
            "postgresql://u@h/db?sslrootcert=%2",
            "postgresql://u@h/db?sslkey=bad%zz&sslcert=/c.pem",
        ] {
            let err = parse(bad, None).unwrap_err();
            let message = err.to_string();
            assert!(message.contains("percent-escape"), "{bad}: {message}");
        }
        // Valid escapes still decode.
        parse("postgresql://u@h/db?sslrootcert=%2Fca.pem", None).expect("valid escape");
    }

    #[test]
    fn tls_parameters_extract_from_both_libpq_forms() {
        // URL form, percent-encoded path.
        let parsed = parse(
            "postgresql://u@h:5432/db?sslmode=verify-full&sslrootcert=%2Fetc%2Fca.pem",
            None,
        )
        .expect("url form");
        assert_eq!(parsed.tls.root_cert, Some(Material::new("/etc/ca.pem")));
        // libpq's verify-full spelling (which the driver itself rejects)
        // translates into the policy mode.
        assert_eq!(parsed.tls.mode, Mode::VerifyFull);
        // …and a weaker block mode cannot silently reverse it.
        let weaker = Policy {
            mode: Mode::Require,
            ..Policy::default()
        };
        assert!(
            parse(
                "postgresql://u@h/db?sslmode=verify-full&sslrootcert=/ca.pem",
                Some(&weaker),
            )
            .is_err(),
            "a block must not weaken the conn string's verify-full"
        );

        // key=value form with spaces and a quoted path; the full parameter set.
        let parsed = parse(
            "host=h user=u sslrootcert = '/my ca/ca.pem' sslcert=/c.pem sslkey=/k.pem",
            None,
        )
        .expect("key=value form");
        assert_eq!(parsed.tls.root_cert, Some(Material::new("/my ca/ca.pem")));
        assert_eq!(parsed.tls.client_cert, Some(Material::new("/c.pem")));
        assert_eq!(parsed.tls.client_key, Some(Material::new("/k.pem")));
        // The remainder reached the driver intact.
        assert_eq!(parsed.driver.get_user(), Some("u"));
    }

    #[test]
    fn connection_and_block_values_must_agree() {
        let block = Policy {
            root_cert: Some(Material::new("/etc/other.pem")),
            ..Policy::default()
        };
        // Disagreement: typed, names both sides.
        let err = parse("host=h sslrootcert=/etc/ca.pem", Some(&block)).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("sslrootcert") && message.contains("/etc/other.pem"),
            "{message}"
        );
        // Agreement: accepted.
        parse("host=h sslrootcert=/etc/other.pem", Some(&block)).expect("agreeing duplicate");
        // A credential split across sources composes: certificate in the
        // string, key in the block.
        let block = Policy {
            client_key: Some(Material::new("/k.pem")),
            ..Policy::default()
        };
        let parsed = parse("host=h sslcert=/c.pem", Some(&block)).expect("split credential");
        assert_eq!(parsed.tls.client_cert, Some(Material::new("/c.pem")));
        // …and the both-or-neither rule still bites across sources.
        let err = parse("host=h sslcert=/c.pem", None).unwrap_err();
        assert!(err.to_string().contains("client_key is missing"), "{err}");
    }

    #[test]
    fn sslrootcert_system_selects_the_platform_store() {
        // libpq 16+ `sslrootcert=system` = platform trust store: the policy
        // resolves to a verifying mode with NO explicit root.
        let parsed = parse(
            "postgresql://u@h/d?sslmode=verify-full&sslrootcert=system",
            None,
        )
        .expect("system root parses");
        assert_eq!(parsed.tls.mode, Mode::VerifyFull);
        assert!(
            parsed.tls.root_cert.is_none(),
            "system = platform store = no explicit root in the policy"
        );
        // An explicit root elsewhere conflicts with `system`.
        let block = Policy {
            root_cert: Some(Material::new("/etc/ca.pem")),
            ..Policy::default()
        };
        assert!(parse("host=h sslrootcert=system", Some(&block)).is_err());
    }

    #[test]
    fn rejected_parameters_are_named_never_bare() {
        // Every unsupported parameter is NAMED, with a hint where one exists.
        for (connection_string, parameter, hint_word) in [
            ("host=h sslpassword=secret", "sslpassword", "encrypted"),
            ("host=h gssencmode=require", "gssencmode", "GSS"),
            ("host=h requiressl=1", "requiressl", "sslmode"),
            ("host=h sslcrl=/crl.pem", "sslcrl", "revocation"),
            (
                "postgresql://u@h/db?connect_timeout=5&some_future_param=1",
                "some_future_param",
                "no rdlt equivalent",
            ),
        ] {
            let err = parse(connection_string, None).unwrap_err();
            let message = err.to_string();
            assert!(
                message.contains(parameter) && message.contains(hint_word),
                "{connection_string}: {message}"
            );
        }
        // Pure syntax garbage still gets the driver's description, prefixed.
        let err = parse("host=h port=notaport", None).unwrap_err();
        assert!(err.to_string().contains("does not parse"), "{err}");
    }

    #[test]
    fn application_name_defaults_and_yields() {
        let parsed = parse("host=h user=u", None).expect("parses");
        assert_eq!(parsed.driver.get_application_name(), Some("rdlt"));
        let parsed = parse("host=h user=u application_name=mine", None).expect("parses");
        assert_eq!(parsed.driver.get_application_name(), Some("mine"));
    }
}
