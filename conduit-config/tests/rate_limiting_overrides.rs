//! Locks in the baked rate-limit overrides in `docker/conduit-rate-limits.toml`.
//!
//! Conduit's limiter cannot be disabled outright (every value must be
//! non-zero), so the baked config sets effectively-unlimited values instead.
//! These tests parse the file with the same provider stack production uses
//! (figment `Toml` nested under `global`) and assert that every override
//! actually lands in the final rate-limiting config.
//!
//! Note: `authentication_failures` cannot be overridden via TOML. The flattened
//! shadow-fragment schema feeds unknown keys to the restriction map first, so
//! the key fails to parse as a `ClientRestriction` variant. The baked config
//! therefore inherits the public_large value for it — 600 failed logins per
//! minute (burst 600) per IP, raised from upstream's 1-per-minute (burst 20) —
//! which is asserted here to keep the intent documented.

use conduit_config::rate_limiting::{
    ClientRestriction, Config as RateLimitingConfig, FederationRestriction, MediaLimitation,
    MediaTimeframe, RequestLimitation, Timeframe,
};
use conduit_config::{IncompleteScopedMediaRetentionConfig, MediaRetentionScope};
use figment::{providers::{Format, Toml}, Figment};
use serde::Deserialize;
use std::{collections::HashSet, time::Duration};

const BAKED_CONFIG: &str = include_str!("../../docker/conduit-rate-limits.toml");

const EFFECTIVELY_UNLIMITED: u64 = 100_000;
const EFFECTIVELY_UNLIMITED_BANDWIDTH: u64 = 10_000_000_000; // "10 GB"
const EFFECTIVELY_UNLIMITED_BANDWIDTH_BURST: u64 = 100_000_000_000; // "100 GB"

fn baked() -> RateLimitingConfig {
    Figment::new()
        .merge(Toml::string(BAKED_CONFIG).nested())
        .extract_inner("rate_limiting")
        .expect("baked config must parse and contain [global.rate_limiting]")
}

fn assert_requests_per_second(limitation: &RequestLimitation, expected: u64, context: &str) {
    assert!(
        matches!(&limitation.timeframe, Timeframe::PerSecond(value) if value.get() == expected),
        "{context}: expected per_second = {expected}, got {limitation:?}",
    );
    assert_eq!(
        limitation.burst_capacity.get(),
        expected,
        "{context}: unexpected burst_capacity",
    );
}

fn assert_bandwidth_per_second(limitation: &MediaLimitation, expected: u64, burst: u64, context: &str) {
    assert!(
        matches!(&limitation.timeframe, MediaTimeframe::PerSecond(bytes) if bytes.as_u64() == expected),
        "{context}: expected per_second = {expected} bytes, got {limitation:?}",
    );
    assert_eq!(
        limitation.burst_capacity.as_u64(),
        burst,
        "{context}: unexpected burst_capacity",
    );
}

#[test]
fn every_client_restriction_is_effectively_unlimited() {
    let config = baked();

    let client_restrictions = [
        ClientRestriction::Registration,
        ClientRestriction::Login,
        ClientRestriction::RegistrationTokenValidity,
        ClientRestriction::SendEvent,
        ClientRestriction::Join,
        ClientRestriction::Knock,
        ClientRestriction::Invite,
        ClientRestriction::SendReport,
        ClientRestriction::CreateAlias,
        ClientRestriction::MediaDownload,
        ClientRestriction::MediaCreate,
    ];

    for restriction in &client_restrictions {
        assert_requests_per_second(
            config
                .global
                .client
                .map
                .get(restriction)
                .expect("preset guarantees an entry"),
            EFFECTIVELY_UNLIMITED,
            format!("global {restriction:?}").as_str(),
        );
        assert_requests_per_second(
            config
                .target
                .client
                .map
                .get(restriction)
                .expect("preset guarantees an entry"),
            EFFECTIVELY_UNLIMITED,
            format!("target {restriction:?}").as_str(),
        );
    }
}

#[test]
fn authentication_failures_falls_back_to_public_large_preset() {
    // The flatten-buffer schema makes this field impossible to override via
    // TOML (see module docs), so the baked config relies on
    // `inherits = "public_large"`: 600 failed logins per minute (burst 600)
    // per IP. Raised from the upstream 1-per-minute (burst 20), which was
    // low enough that a client retrying with a stale access token could lock
    // its own IP out of the server for a minute at a time.
    let config = baked();

    let authentication_failures = &config
        .target
        .client
        .additional_fields
        .authentication_failures;

    assert!(
        matches!(
            &authentication_failures.timeframe,
            Timeframe::PerMinute(value) if value.get() == 600
        ),
        "public_large authentication_failures is 600 per minute, got {authentication_failures:?}",
    );
    assert_eq!(authentication_failures.burst_capacity.get(), 600);
}

#[test]
fn client_media_bandwidth_is_effectively_unlimited() {
    let config = baked();

    for (scope, media) in [
        ("global", &config.global.client.media),
        ("target", &config.target.client.media),
    ] {
        assert_bandwidth_per_second(
            &media.download,
            EFFECTIVELY_UNLIMITED_BANDWIDTH,
            EFFECTIVELY_UNLIMITED_BANDWIDTH_BURST,
            format!("{scope} media download").as_str(),
        );
        assert_bandwidth_per_second(
            &media.upload,
            EFFECTIVELY_UNLIMITED_BANDWIDTH,
            EFFECTIVELY_UNLIMITED_BANDWIDTH_BURST,
            format!("{scope} media upload").as_str(),
        );
        assert_bandwidth_per_second(
            &media.fetch,
            EFFECTIVELY_UNLIMITED_BANDWIDTH,
            EFFECTIVELY_UNLIMITED_BANDWIDTH_BURST,
            format!("{scope} media fetch").as_str(),
        );
    }
}

#[test]
fn federation_keeps_preset_defaults() {
    let config = baked();

    let join = config
        .global
        .federation
        .map
        .get(&FederationRestriction::Join)
        .expect("preset guarantees an entry");

    assert!(
        matches!(&join.timeframe, Timeframe::PerSecond(value) if value.get() == 1),
        "public_large federation Join is 1 per second, got {join:?}",
    );
    assert_eq!(join.burst_capacity.get(), 50);
}

#[derive(Default, Deserialize)]
struct ProbeMedia {
    #[serde(default)]
    retention: Option<HashSet<IncompleteScopedMediaRetentionConfig>>,
}

#[derive(Deserialize)]
struct ProbeGlobal {
    #[serde(default, with = "humantime_serde::option")]
    message_retention: Option<Duration>,
    #[serde(default)]
    media: ProbeMedia,
}

#[test]
fn retention_policies_parse() {
    const SIXTY_DAYS: Duration = Duration::from_secs(60 * 60 * 24 * 60);

    let probe: ProbeGlobal = Figment::new()
        .merge(Toml::string(BAKED_CONFIG).nested())
        .extract()
        .expect("retention config must parse");

    assert_eq!(probe.message_retention, Some(SIXTY_DAYS));

    let retention = probe.media.retention.expect("media retention must be set");
    assert_eq!(retention.len(), 3, "unexpected media retention entries");

    let local = retention
        .iter()
        .find(|entry| entry.scope.is_none())
        .expect("scope-less media retention entry");
    assert_eq!(local.accessed, Some(SIXTY_DAYS));
    assert_eq!(
        local.space.as_ref().map(bytesize::ByteSize::as_u64),
        Some(3_000_000_000),
        "3 GB global media space cap",
    );

    for (scope, expected) in [
        (MediaRetentionScope::Remote, "remote"),
        (MediaRetentionScope::Thumbnail, "thumbnail"),
    ] {
        let entry = retention
            .iter()
            .find(|entry| entry.scope.as_ref() == Some(&scope))
            .unwrap_or_else(|| panic!("{expected} media retention entry must be set"));
        assert_eq!(entry.accessed, Some(SIXTY_DAYS));
    }
}
