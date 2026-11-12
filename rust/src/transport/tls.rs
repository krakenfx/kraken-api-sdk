//! TLS trust for every SDK connection: the bundled Mozilla roots of the CAs
//! that `kraken.com`'s CAA record authorises, plus any roots the caller adds
//! through the `tls_extra_root_certs_file` knob. REST and WebSocket share it.

use std::sync::{Arc, LazyLock};

use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConfig, RootCertStore};

use crate::build::ConfigError;

const TRUSTED_ROOT_ORGANIZATIONS: [&str; 9] = [
    "Amazon",
    "COMODO CA Limited",
    "DigiCert Inc",
    "DigiCert, Inc.",
    "Google Trust Services LLC",
    "Internet Security Research Group",
    "SSL Corporation",
    "Sectigo Limited",
    "The USERTRUST Network",
];

const TRUSTED_HOST_DOMAINS: [&str; 2] = ["kraken.com", "kraken.zone"];

static DEFAULT_CLIENT_CONFIG: LazyLock<Arc<ClientConfig>> =
    LazyLock::new(|| Arc::new(build_client_config(trusted_roots())));

pub(crate) fn default_client_config() -> Arc<ClientConfig> {
    Arc::clone(&DEFAULT_CLIENT_CONFIG)
}

pub(crate) fn client_config(extra_root_certs_file: &str) -> Result<Arc<ClientConfig>, ConfigError> {
    if extra_root_certs_file.is_empty() {
        return Ok(default_client_config());
    }
    let unreadable = |reason: String| ConfigError::InvalidConfig {
        detail: format!("tls_extra_root_certs_file: {reason}"),
    };
    let mut roots = trusted_roots();
    let mut added = 0usize;
    for cert in CertificateDer::pem_file_iter(extra_root_certs_file)
        .map_err(|e| unreadable(format!("cannot read PEM file ({e})")))?
    {
        let cert = cert.map_err(|e| unreadable(format!("invalid PEM certificate ({e})")))?;
        roots
            .add(cert)
            .map_err(|e| unreadable(format!("unusable root certificate ({e})")))?;
        added += 1;
    }
    if added == 0 {
        return Err(unreadable("no certificates in the file".to_string()));
    }
    Ok(Arc::new(build_client_config(roots)))
}

pub(crate) fn is_trusted_host(host: &str) -> bool {
    TRUSTED_HOST_DOMAINS.iter().any(|domain| {
        host == *domain
            || host
                .strip_suffix(domain)
                .is_some_and(|prefix| prefix.ends_with('.'))
    })
}

fn build_client_config(roots: RootCertStore) -> ClientConfig {
    ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("invariant: aws-lc-rs supports the safe default TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

fn trusted_roots() -> RootCertStore {
    RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS
            .iter()
            .filter(|anchor| names_trusted_organization(anchor.subject.as_ref()))
            .cloned()
            .collect(),
    }
}

fn names_trusted_organization(subject: &[u8]) -> bool {
    TRUSTED_ROOT_ORGANIZATIONS
        .iter()
        .any(|org| encodes_organization(subject, org))
}

fn encodes_organization(subject: &[u8], org: &str) -> bool {
    let Ok(len) = u8::try_from(org.len()) else {
        return false;
    };
    [0x13, 0x0c].into_iter().any(|string_tag| {
        let mut attribute = vec![0x06, 0x03, 0x55, 0x04, 0x0a, string_tag, len];
        attribute.extend_from_slice(org.as_bytes());
        subject
            .windows(attribute.len())
            .any(|window| window == attribute.as_slice())
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rustls::client::WebPkiServerVerifier;
    use rustls::client::danger::ServerCertVerifier;
    use rustls::pki_types::{ServerName, UnixTime};
    use rustls::{CertificateError, Error};

    use super::*;

    const CHAINS_CAPTURED_AT: u64 = 1_794_387_012;
    const UWS_LEAF: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/tls/uws_kraken_com_leaf.der"
    ));
    const GTS_WE1: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/tls/gts_we1_intermediate.der"
    ));
    const GLOBALSIGN_LEAF: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/tls/www_globalsign_com_leaf.der"
    ));
    const GLOBALSIGN_INTERMEDIATE: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/tls/globalsign_gcc_r46_ev_intermediate.der"
    ));
    const CAA_ISSUERS: [&str; 6] = [
        "amazonaws.com",
        "comodoca.com",
        "digicert.com",
        "letsencrypt.org",
        "pki.goog",
        "ssl.com",
    ];

    fn verify(
        roots: RootCertStore,
        leaf: &'static [u8],
        intermediate: &'static [u8],
        host: &'static str,
    ) -> Result<(), Error> {
        WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::clone(default_client_config().crypto_provider()),
        )
        .build()
        .expect("verifier builds")
        .verify_server_cert(
            &CertificateDer::from(leaf),
            &[CertificateDer::from(intermediate)],
            &ServerName::try_from(host).expect("valid server name"),
            &[],
            UnixTime::since_unix_epoch(Duration::from_secs(CHAINS_CAPTURED_AT)),
        )
        .map(|_| ())
    }

    fn mozilla_roots() -> RootCertStore {
        RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        }
    }

    #[test]
    fn the_kraken_chain_verifies_against_the_trusted_roots() {
        assert_eq!(
            verify(trusted_roots(), UWS_LEAF, GTS_WE1, "uws.kraken.com"),
            Ok(())
        );
    }

    #[test]
    fn a_chain_from_a_ca_outside_the_caa_record_is_refused() {
        assert_eq!(
            verify(
                mozilla_roots(),
                GLOBALSIGN_LEAF,
                GLOBALSIGN_INTERMEDIATE,
                "www.globalsign.com"
            ),
            Ok(()),
            "the fixture chain must be valid under the full Mozilla set"
        );
        assert_eq!(
            verify(
                trusted_roots(),
                GLOBALSIGN_LEAF,
                GLOBALSIGN_INTERMEDIATE,
                "www.globalsign.com"
            ),
            Err(Error::InvalidCertificate(CertificateError::UnknownIssuer))
        );
    }

    #[test]
    fn every_trusted_organization_names_a_bundled_root() {
        for org in TRUSTED_ROOT_ORGANIZATIONS {
            assert!(
                webpki_roots::TLS_SERVER_ROOTS
                    .iter()
                    .any(|anchor| encodes_organization(anchor.subject.as_ref(), org)),
                "no bundled root has organization `{org}`"
            );
        }
        assert!(trusted_roots().roots.len() < webpki_roots::TLS_SERVER_ROOTS.len());
    }

    #[test]
    fn extra_roots_file_adds_its_certificates_and_rejects_empty_or_missing_files() {
        let dir = std::env::temp_dir().join(format!("kraken-sdk-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let pem = dir.join("roots.pem");
        let empty = dir.join("empty.pem");
        use base64::Engine;
        let body = format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
            base64::engine::general_purpose::STANDARD.encode(GTS_WE1)
        );
        std::fs::write(&pem, body).expect("write pem");
        std::fs::write(&empty, "").expect("write empty");

        assert!(client_config(pem.to_str().expect("utf-8 path")).is_ok());
        for bad in [
            empty.to_str().expect("utf-8 path"),
            "/nonexistent/roots.pem",
        ] {
            assert!(
                matches!(client_config(bad), Err(ConfigError::InvalidConfig { .. })),
                "{bad}"
            );
        }
        assert!(Arc::ptr_eq(
            &client_config("").expect("default"),
            &default_client_config()
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn trusted_hosts_are_kraken_domains_and_their_subdomains() {
        // host, trusted
        let cases = [
            ("api.kraken.com", true),
            ("kraken.com", true),
            ("uws.test.kraken.com", true),
            ("api.test.kraken.zone", true),
            ("evilkraken.com", false),
            ("kraken.com.evil.example", false),
            ("kraken.zone.evil.example", false),
            ("example.com", false),
        ];
        for (host, trusted) in cases {
            assert_eq!(is_trusted_host(host), trusted, "host={host}");
        }
    }

    #[tokio::test]
    #[ignore = "live network: Kraken production endpoints"]
    async fn kraken_endpoints_complete_tls_with_the_trusted_roots() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let knobs = crate::build::knobs::Knobs::defaults();
        for url in [
            knobs.ws_public_url.as_str(),
            knobs.ws_auth_url.as_str(),
            knobs.unified_ws_public_url.as_str(),
            knobs.unified_ws_private_url.as_str(),
        ] {
            let request = url.into_client_request().expect("request");
            let result = crate::transport::ws::connect_tls(request, default_client_config()).await;
            assert!(result.is_ok(), "{url}: {:?}", result.err());
        }
        let rest = crate::transport::ReqwestHttpTransport::new();
        let result = crate::transport::HttpTransport::get_json(&rest, "/0/public/Time", &[]).await;
        assert!(
            result.is_ok(),
            "{}: {:?}",
            knobs.rest_base_url,
            result.err()
        );

        let outside = "wss://www.globalsign.com/"
            .into_client_request()
            .expect("request");
        let refused = crate::transport::ws::connect_tls(outside, default_client_config()).await;
        assert!(
            refused
                .as_ref()
                .is_err_and(|e| format!("{e:?}").contains("UnknownIssuer")),
            "a WS peer outside the CAA set must fail certificate verification: {refused:?}"
        );
        let rest_outside =
            crate::transport::ReqwestHttpTransport::with_base_url("https://www.globalsign.com");
        let refused =
            crate::transport::HttpTransport::get_json(&rest_outside, "/0/public/Time", &[]).await;
        assert!(
            refused.as_ref().is_err_and(|e| matches!(
                e.kind,
                crate::transport::TransportErrorKind::TcpRefused
                    | crate::transport::TransportErrorKind::TlsHandshakeFailure
            )),
            "a REST peer outside the CAA set must fail before the request is sent: {refused:?}"
        );
    }

    #[tokio::test]
    #[ignore = "live network: DNS-over-HTTPS lookup of the kraken.com CAA record"]
    async fn the_kraken_caa_record_names_the_issuers_the_trusted_roots_cover() {
        let body: serde_json::Value =
            reqwest::get("https://dns.google/resolve?name=kraken.com&type=CAA")
                .await
                .expect("DNS-over-HTTPS reachable")
                .json()
                .await
                .expect("JSON answer");
        let mut issuers: Vec<String> = body["Answer"]
            .as_array()
            .expect("CAA answers")
            .iter()
            .filter_map(|answer| answer["data"].as_str())
            .filter_map(|data| {
                let mut fields = data.splitn(3, ' ');
                let tag = fields.nth(1)?;
                let value = fields.next()?.trim_matches('"');
                matches!(tag, "issue" | "issuewild")
                    .then(|| value.split(';').next().unwrap_or("").trim().to_string())
            })
            .collect();
        issuers.sort();
        issuers.dedup();
        assert_eq!(
            issuers, CAA_ISSUERS,
            "kraken.com CAA issuers changed: update TRUSTED_ROOT_ORGANIZATIONS and CAA_ISSUERS"
        );
    }
}
