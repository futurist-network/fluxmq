//! Fast path against itself and against rustls' buffered implementation (tokio-rustls), in
//! both roles: data both ways (tiny to 1 MiB writes, random splits), every suite, TLS 1.2,
//! KeyUpdate (peer- and self-initiated, requested and not), truncation, tampering,
//! certificate rejection, mTLS, CRL revocation, ALPN.

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyIdMethod, KeyPair, KeyUsagePurpose,
    RevokedCertParams, SerialNumber,
};
use rustls::crypto::CryptoProvider;
use rustls::server::WebPkiClientVerifier;
use rustls::{
    CipherSuite, ClientConfig, ProtocolVersion, RootCertStore, ServerConfig, SupportedCipherSuite,
    Tls12CipherSuite, Tls13CipherSuite,
};
use rustls_pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};

use crate::stream::{self, SplitIo, TlsImpl, TlsRead, TlsSplit, TlsWrite};

// ------------------------------------------------------------------------------- PKI

struct Pki {
    ca: CertificateDer<'static>,
    server: (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>),
    client: (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>),
    /// A second client certificate (same CA) that the CRL revokes.
    revoked_client: (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>),
    crl: CertificateRevocationListDer<'static>,
    /// A certificate/key from an unrelated CA.
    stranger: (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>),
    stranger_ca: CertificateDer<'static>,
}

fn dn(cn: &str) -> DistinguishedName {
    let mut d = DistinguishedName::new();
    d.push(DnType::CommonName, cn);
    d
}

fn make_ca(cn: &str) -> (Issuer<'static, KeyPair>, CertificateDer<'static>) {
    let key = KeyPair::generate().unwrap();
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.distinguished_name = dn(cn);
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let cert = p.self_signed(&key).unwrap();
    let der = cert.der().clone();
    (Issuer::new(p, key), der)
}

fn leaf(
    ca: &Issuer<'static, KeyPair>,
    cn: &str,
    serial: u64,
    server: bool,
) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let key = KeyPair::generate().unwrap();
    let mut p = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    p.distinguished_name = dn(cn);
    p.serial_number = Some(SerialNumber::from(serial));
    p.extended_key_usages = vec![if server {
        ExtendedKeyUsagePurpose::ServerAuth
    } else {
        ExtendedKeyUsagePurpose::ClientAuth
    }];
    let cert = p.signed_by(&key, ca).unwrap();
    (
        vec![cert.der().clone()],
        PrivateKeyDer::try_from(key.serialize_der()).unwrap(),
    )
}

fn pki() -> &'static Pki {
    static PKI: OnceLock<Pki> = OnceLock::new();
    PKI.get_or_init(|| {
        let (issuer, ca) = make_ca("test CA");
        let server = leaf(&issuer, "server", 10, true);
        let client = leaf(&issuer, "client-1", 11, false);
        let revoked_client = leaf(&issuer, "client-revoked", 12, false);
        let now = time::OffsetDateTime::now_utc();
        let crl = CertificateRevocationListParams {
            this_update: now - time::Duration::days(1),
            next_update: now + time::Duration::days(30),
            crl_number: SerialNumber::from(1u64),
            issuing_distribution_point: None,
            revoked_certs: vec![RevokedCertParams {
                serial_number: SerialNumber::from(12u64),
                revocation_time: now - time::Duration::hours(1),
                reason_code: None,
                invalidity_date: None,
            }],
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(&issuer)
        .unwrap();
        let (sissuer, stranger_ca) = make_ca("stranger CA");
        let stranger = leaf(&sissuer, "stranger", 20, false);
        Pki {
            ca,
            server,
            client,
            revoked_client,
            crl: crl.der().clone(),
            stranger,
            stranger_ca,
        }
    })
}

fn roots(ca: &CertificateDer<'static>) -> RootCertStore {
    let mut r = RootCertStore::empty();
    r.add(ca.clone()).unwrap();
    r
}

// ------------------------------------------------------------------------------- configs

#[derive(Clone, Copy, PartialEq)]
enum ClientAuth {
    None,
    Required,
    RequiredWithCrl,
}

fn provider_with(suites: Option<&[CipherSuite]>, limit: Option<u64>) -> Arc<CryptoProvider> {
    let mut p = rustls::crypto::aws_lc_rs::default_provider();
    if let Some(keep) = suites {
        p.cipher_suites.retain(|s| keep.contains(&s.suite()));
        // Preserve the requested order.
        p.cipher_suites
            .sort_by_key(|s| keep.iter().position(|k| *k == s.suite()));
    }
    if let Some(limit) = limit {
        p.cipher_suites = p
            .cipher_suites
            .iter()
            .map(|s| match s {
                SupportedCipherSuite::Tls13(t) => {
                    let common = rustls::CipherSuiteCommon {
                        suite: t.common.suite,
                        hash_provider: t.common.hash_provider,
                        confidentiality_limit: limit,
                    };
                    SupportedCipherSuite::Tls13(Box::leak(Box::new(Tls13CipherSuite {
                        common,
                        hkdf_provider: t.hkdf_provider,
                        aead_alg: t.aead_alg,
                        quic: t.quic,
                    })))
                }
                SupportedCipherSuite::Tls12(t) => {
                    let common = rustls::CipherSuiteCommon {
                        suite: t.common.suite,
                        hash_provider: t.common.hash_provider,
                        confidentiality_limit: limit,
                    };
                    SupportedCipherSuite::Tls12(Box::leak(Box::new(Tls12CipherSuite {
                        common,
                        prf_provider: t.prf_provider,
                        kx: t.kx,
                        sign: t.sign,
                        aead_alg: t.aead_alg,
                    })))
                }
            })
            .collect();
    }
    Arc::new(p)
}

#[derive(Clone)]
struct Opts {
    tls12_only: bool,
    suites: Option<Vec<CipherSuite>>,
    limit: Option<u64>,
    auth: ClientAuth,
    alpn: Vec<Vec<u8>>,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            tls12_only: false,
            suites: None,
            limit: None,
            auth: ClientAuth::None,
            alpn: vec![],
        }
    }
}

fn versions(o: &Opts) -> &'static [&'static rustls::SupportedProtocolVersion] {
    static TLS12_ONLY: [&rustls::SupportedProtocolVersion; 1] = [&rustls::version::TLS12];
    if o.tls12_only {
        &TLS12_ONLY
    } else {
        rustls::DEFAULT_VERSIONS
    }
}

fn server_config(o: &Opts) -> Arc<ServerConfig> {
    let pki = pki();
    let provider = provider_with(o.suites.as_deref(), o.limit);
    let b = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(versions(o))
        .unwrap();
    let b = match o.auth {
        ClientAuth::None => b.with_no_client_auth(),
        ClientAuth::Required => b.with_client_cert_verifier(
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots(&pki.ca)), provider)
                .build()
                .unwrap(),
        ),
        ClientAuth::RequiredWithCrl => b.with_client_cert_verifier(
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots(&pki.ca)), provider)
                .with_crls(vec![pki.crl.clone()])
                .build()
                .unwrap(),
        ),
    };
    let mut c = b
        .with_single_cert(pki.server.0.clone(), pki.server.1.clone_key())
        .unwrap();
    c.alpn_protocols = o.alpn.clone();
    Arc::new(c)
}

fn client_config_with(
    o: &Opts,
    ca: &CertificateDer<'static>,
    identity: Option<&(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
) -> Arc<ClientConfig> {
    let provider = provider_with(o.suites.as_deref(), o.limit);
    let b = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(versions(o))
        .unwrap()
        .with_root_certificates(roots(ca));
    let mut c = match identity {
        Some((chain, key)) => b
            .with_client_auth_cert(chain.clone(), key.clone_key())
            .unwrap(),
        None => b.with_no_client_auth(),
    };
    c.alpn_protocols = o.alpn.clone();
    Arc::new(c)
}

fn client_config(o: &Opts) -> Arc<ClientConfig> {
    let pki = pki();
    let id =
        matches!(o.auth, ClientAuth::Required | ClientAuth::RequiredWithCrl).then_some(&pki.client);
    client_config_with(o, &pki.ca, id)
}

fn name() -> ServerName<'static> {
    ServerName::try_from("localhost").unwrap()
}

// ------------------------------------------------------------------------------- plumbing

type Pair<IO> = (TlsSplit<IO>, TlsSplit<IO>);

async fn establish<IO: SplitIo>(
    a: IO,
    b: IO,
    server_impl: TlsImpl,
    client_impl: TlsImpl,
    scfg: Arc<ServerConfig>,
    ccfg: Arc<ClientConfig>,
) -> std::io::Result<Pair<IO>> {
    let srv = stream::accept(server_impl, scfg, a);
    let cli = stream::connect(client_impl, ccfg, name(), b);
    let (s, c) = tokio::join!(srv, cli);
    Ok((s?, c?))
}

async fn tcp_pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let (a, b) = tokio::join!(l.accept(), tokio::net::TcpStream::connect(addr));
    let a = a.unwrap().0;
    let b = b.unwrap();
    a.set_nodelay(true).unwrap();
    b.set_nodelay(true).unwrap();
    (a, b)
}

/// xorshift64*: deterministic random sizes.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// Mostly small, sometimes record-sized or larger.
    fn size(&mut self, max: usize) -> usize {
        let cap = match self.below(10) {
            0..=4 => 64,
            5..=7 => 4096,
            8 => 20_000,
            _ => max,
        };
        1 + self.below(cap.min(max))
    }
}

fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut r = Rng(seed | 1);
    (0..len).map(|_| r.next() as u8).collect()
}

/// Writes `data` with random write sizes, mixing plain and vectored writes, then flushes
/// and shuts down (close_notify).
async fn write_random<W: AsyncWrite + Unpin>(w: &mut W, data: &[u8], seed: u64) {
    let mut r = Rng(seed | 1);
    let mut pos = 0;
    while pos < data.len() {
        if r.below(3) == 0 {
            // A vectored write of up to 5 slices.
            let mut slices = Vec::new();
            let mut p = pos;
            for _ in 0..1 + r.below(5) {
                if p >= data.len() {
                    break;
                }
                let n = r.size(data.len() - p).min(data.len() - p);
                slices.push(std::io::IoSlice::new(&data[p..p + n]));
                p += n;
            }
            let n = w.write_vectored(&slices).await.unwrap();
            assert!(n > 0);
            pos += n;
        } else {
            let n = r.size(data.len() - pos).min(data.len() - pos);
            w.write_all(&data[pos..pos + n]).await.unwrap();
            pos += n;
        }
        if r.below(4) == 0 {
            w.flush().await.unwrap();
        }
    }
    w.flush().await.unwrap();
    w.shutdown().await.unwrap();
}

/// Reads to a clean EOF with random buffer sizes.
async fn read_random<R: AsyncRead + Unpin>(rd: &mut R, seed: u64) -> std::io::Result<Vec<u8>> {
    let mut r = Rng(seed | 1);
    let mut out = Vec::new();
    loop {
        let mut buf = vec![0u8; r.size(70_000)];
        let n = rd.read(&mut buf).await?;
        if n == 0 {
            return Ok(out);
        }
        out.extend_from_slice(&buf[..n]);
    }
}

/// Both directions at once, byte-identical, clean close both ways.
async fn exchange<IO: SplitIo>(pair: Pair<IO>, up: usize, down: usize, seed: u64) {
    let (srv, cli) = pair;
    let a = pattern(up, seed);
    let b = pattern(down, seed ^ 0x55);
    let (
        TlsSplit {
            read: mut sr,
            write: mut sw,
            ..
        },
        TlsSplit {
            read: mut cr,
            write: mut cw,
            ..
        },
    ) = (srv, cli);
    let (a2, b2) = (a.clone(), b.clone());
    let t1 = tokio::spawn(async move { write_random(&mut cw, &a2, seed).await });
    let t2 = tokio::spawn(async move { write_random(&mut sw, &b2, seed + 1).await });
    let t3 = tokio::spawn(async move { read_random(&mut sr, seed + 2).await.unwrap() });
    let t4 = tokio::spawn(async move { read_random(&mut cr, seed + 3).await.unwrap() });
    let timeout = Duration::from_secs(60);
    tokio::time::timeout(timeout, t1).await.unwrap().unwrap();
    tokio::time::timeout(timeout, t2).await.unwrap().unwrap();
    let got_a = tokio::time::timeout(timeout, t3).await.unwrap().unwrap();
    let got_b = tokio::time::timeout(timeout, t4).await.unwrap().unwrap();
    assert!(
        got_a == a,
        "client→server data differs ({} vs {} bytes)",
        got_a.len(),
        a.len()
    );
    assert!(
        got_b == b,
        "server→client data differs ({} vs {} bytes)",
        got_b.len(),
        b.len()
    );
}

const IMPLS: [(TlsImpl, TlsImpl); 4] = [
    (TlsImpl::Kernel, TlsImpl::Kernel),
    (TlsImpl::Kernel, TlsImpl::Classic),
    (TlsImpl::Classic, TlsImpl::Kernel),
    (TlsImpl::Classic, TlsImpl::Classic),
];

/// Over TCP `Fast` means kernel TLS where the kernel offers it: pair it with every other
/// implementation, and keep the userspace record layer covered too.
const TCP_IMPLS: [(TlsImpl, TlsImpl); 8] = [
    (TlsImpl::Kernel, TlsImpl::Kernel),
    (TlsImpl::Kernel, TlsImpl::Classic),
    (TlsImpl::Classic, TlsImpl::Kernel),
    (TlsImpl::Classic, TlsImpl::Classic),
    (TlsImpl::Kernel, TlsImpl::Fast),
    (TlsImpl::Fast, TlsImpl::Kernel),
    (TlsImpl::Fast, TlsImpl::Fast),
    (TlsImpl::Fast, TlsImpl::Classic),
];

// ------------------------------------------------------------------------------- tests

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn roundtrip_all_impl_pairs_tcp() {
    let o = Opts::default();
    for (i, (s, c)) in TCP_IMPLS.into_iter().enumerate() {
        let (a, b) = tcp_pair().await;
        let pair = establish(a, b, s, c, server_config(&o), client_config(&o))
            .await
            .unwrap();
        assert_eq!(pair.0.info.protocol_version, Some(ProtocolVersion::TLSv1_3));
        exchange(pair, 3 << 20, 2 << 20, 100 + i as u64).await;
    }
}

/// Property test: random write sizes / splits / read sizes through a small duplex pipe
/// (partial records on every read, write backpressure), many seeds, both roles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_splits_through_duplex_pipe() {
    let o = Opts::default();
    let (scfg, ccfg) = (server_config(&o), client_config(&o));
    for seed in 1..=24u64 {
        let (s, c) = IMPLS[(seed % 3) as usize];
        let pipe = [977, 4096, 17_000, 65_536][(seed % 4) as usize];
        let (a, b): (DuplexStream, DuplexStream) = tokio::io::duplex(pipe);
        let pair = establish(a, b, s, c, scfg.clone(), ccfg.clone())
            .await
            .unwrap();
        let mut r = Rng(seed * 7919);
        let up = r.below(600_000);
        let down = r.below(600_000);
        exchange(pair, up, down, seed).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_suite_and_tls12() {
    let suites13 = [
        CipherSuite::TLS13_AES_128_GCM_SHA256,
        CipherSuite::TLS13_AES_256_GCM_SHA384,
        CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
    ];
    let suites12 = [
        CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
        CipherSuite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
    ];
    let mut n = 0u64;
    for (suite, tls12) in suites13
        .iter()
        .map(|s| (*s, false))
        .chain(suites12.iter().map(|s| (*s, true)))
    {
        for (s, c) in IMPLS.into_iter().take(3) {
            let o = Opts {
                tls12_only: tls12,
                suites: Some(vec![suite]),
                ..Opts::default()
            };
            let (a, b) = tokio::io::duplex(16_384);
            let pair = establish(a, b, s, c, server_config(&o), client_config(&o))
                .await
                .unwrap_or_else(|e| panic!("{suite:?}: {e}"));
            assert_eq!(pair.1.info.cipher_suite.map(|s| s.suite()), Some(suite));
            let want = if tls12 {
                ProtocolVersion::TLSv1_2
            } else {
                ProtocolVersion::TLSv1_3
            };
            assert_eq!(pair.1.info.protocol_version, Some(want));
            n += 1;
            exchange(pair, 300_000, 200_000, 500 + n).await;
        }
    }
}

fn fast_shared<IO: SplitIo>(r: &TlsRead<IO>) -> &Arc<super::record::Shared> {
    match r {
        TlsRead::Fast(r) => r.shared(),
        _ => panic!("not the fast path"),
    }
}

/// A tiny confidentiality limit makes both sides rotate keys every few records: the fast
/// side initiates KeyUpdates and applies the classic peer's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_updates_at_confidentiality_limit() {
    for (s, c) in IMPLS.into_iter().take(3) {
        let o = Opts {
            limit: Some(5),
            ..Opts::default()
        };
        let (a, b) = tokio::io::duplex(32_768);
        let pair = establish(a, b, s, c, server_config(&o), client_config(&o))
            .await
            .unwrap();
        let fast_side = if s == TlsImpl::Kernel {
            &pair.0
        } else {
            &pair.1
        };
        let shared = fast_shared(&fast_side.read).clone();
        exchange(pair, 1 << 20, 1 << 20, 900).await;
        // ~64 records each way at limit 5: well over 10 updates per direction.
        assert!(
            shared.tx_updates.load(Ordering::Relaxed) >= 10,
            "tx updates"
        );
        assert!(
            shared.rx_updates.load(Ordering::Relaxed) >= 10,
            "rx updates"
        );
    }
}

/// KeyUpdate(update_requested) sent by the fast side mid-stream: the peer (classic or fast)
/// must answer with its own KeyUpdate, which the fast side applies; data stays intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_update_requested_mid_stream() {
    for (s, c) in [
        (TlsImpl::Kernel, TlsImpl::Classic),
        (TlsImpl::Classic, TlsImpl::Kernel),
        (TlsImpl::Kernel, TlsImpl::Kernel),
    ] {
        let o = Opts::default();
        let (a, b) = tcp_pair().await;
        let (srv, cli) = establish(a, b, s, c, server_config(&o), client_config(&o))
            .await
            .unwrap();
        // The requester is the client if it is fast, else the server.
        let (req, peer) = if c == TlsImpl::Kernel {
            (cli, srv)
        } else {
            (srv, cli)
        };
        let TlsSplit {
            read: mut rr,
            write: mut rw,
            ..
        } = req;
        let TlsSplit {
            read: mut pr,
            write: mut pw,
            ..
        } = peer;
        let shared = fast_shared(&rr).clone();
        let data = pattern(200_000, 77);
        // Some data, then the request, then more data.
        rw.write_all(&data[..50_000]).await.unwrap();
        match &mut rw {
            TlsWrite::Fast(w) => w.request_key_update(),
            _ => unreachable!(),
        }
        rw.write_all(&data[50_000..]).await.unwrap();
        rw.flush().await.unwrap();
        let mut got = vec![0u8; data.len()];
        pr.read_exact(&mut got).await.unwrap();
        assert!(got == data);
        // The peer's answer arrives before its next application data.
        pw.write_all(b"pong").await.unwrap();
        pw.flush().await.unwrap();
        let mut pong = [0u8; 4];
        rr.read_exact(&mut pong).await.unwrap();
        assert_eq!(&pong, b"pong");
        // A userspace fast server also rotated once at its first record (hand-over at seq
        // 2); the kernel takes any starting sequence number.
        let kernel = matches!(&rw, TlsWrite::Fast(w) if w.is_kernel_tls());
        let rotations = if c == TlsImpl::Kernel || kernel { 0 } else { 1 };
        assert_eq!(shared.tx_updates.load(Ordering::Relaxed), 1 + rotations);
        assert_eq!(
            shared.rx_updates.load(Ordering::Relaxed),
            1,
            "peer answered the request"
        );
        // And the stream keeps working afterwards in both directions.
        rw.write_all(b"after").await.unwrap();
        rw.flush().await.unwrap();
        let mut after = [0u8; 5];
        pr.read_exact(&mut after).await.unwrap();
        assert_eq!(&after, b"after");
    }
}

/// TLS 1.2 has no key update: at the limit the fast writer sends close_notify and fails
/// (rustls' behaviour), the peer sees a clean close.
#[tokio::test]
async fn tls12_limit_closes() {
    let o = Opts {
        tls12_only: true,
        limit: Some(4),
        ..Opts::default()
    };
    let (a, b) = tokio::io::duplex(1 << 20);
    let (srv, cli) = establish(
        a,
        b,
        TlsImpl::Classic,
        TlsImpl::Kernel,
        server_config(&o),
        client_config(&o),
    )
    .await
    .unwrap();
    let TlsSplit { write: mut cw, .. } = cli;
    let TlsSplit { read: mut sr, .. } = srv;
    let mut failed = false;
    for _ in 0..10 {
        if cw.write_all(&[7u8; 1000]).await.is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed, "writes past the TLS 1.2 limit must fail");
    let _ = cw.flush().await;
    drop(cw);
    let mut buf = Vec::new();
    // The peer reads what was sent, then a clean EOF (close_notify).
    sr.read_to_end(&mut buf).await.unwrap();
    assert!(buf.len() <= 4000);
}

/// EOF without close_notify must be an error on the fast read half (truncation attack), at
/// a record boundary and in the middle of a record. With close_notify it is a clean EOF.
#[tokio::test]
async fn truncation_is_an_error() {
    let o = Opts::default();
    for cut_mid_record in [false, true] {
        // Fast client, classic server whose raw socket we cut.
        let (a, b) = tcp_pair().await;
        let scfg = server_config(&o);
        let srv = async move {
            let mut s = tokio_rustls::TlsAcceptor::from(scfg)
                .accept(a)
                .await
                .unwrap();
            s.write_all(&pattern(40_000, 1)).await.unwrap();
            s.flush().await.unwrap();
            let (tcp, _) = s.into_inner();
            if cut_mid_record {
                // Raw garbage that looks like the start of a record, then EOF.
                let mut tcp = tcp;
                tcp.write_all(&[23, 3, 3, 0x40, 0x00, 1, 2, 3])
                    .await
                    .unwrap();
                drop(tcp);
            } else {
                drop(tcp);
            }
        };
        let cli = stream::connect(TlsImpl::Kernel, client_config(&o), name(), b);
        let ((), cli) = tokio::join!(srv, cli);
        let mut cr = cli.unwrap().read;
        let mut got = Vec::new();
        let err = cr.read_to_end(&mut got).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof, "{err}");
        assert_eq!(got.len(), 40_000, "data before the cut is delivered");
    }
    // Clean close: shutdown sends close_notify.
    let (a, b) = tokio::io::duplex(8192);
    let (srv, cli) = establish(
        a,
        b,
        TlsImpl::Kernel,
        TlsImpl::Kernel,
        server_config(&o),
        client_config(&o),
    )
    .await
    .unwrap();
    let mut sw = srv.write;
    sw.write_all(b"bye").await.unwrap();
    sw.shutdown().await.unwrap();
    let mut cr = cli.read;
    let mut got = Vec::new();
    cr.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"bye");
    // Writing after shutdown is refused.
    assert!(sw.write_all(b"x").await.is_err());
}

/// A man in the middle flips one ciphertext bit: the fast reader rejects the record
/// (AEAD tag), never returns altered data, and the connection is dead afterwards.
#[tokio::test]
async fn tampering_is_detected() {
    let o = Opts::default();
    let (srv_io, mitm_s) = tokio::io::duplex(1 << 16);
    let (mitm_c, cli_io) = tokio::io::duplex(1 << 16);
    // Relay; after the handshake (first 8 KiB client-bound) flip a bit in server→client data.
    let relay = tokio::spawn(async move {
        let (mut sr, mut sw) = tokio::io::split(mitm_s);
        let (mut cr, mut cw) = tokio::io::split(mitm_c);
        let up = tokio::spawn(async move { tokio::io::copy(&mut cr, &mut sw).await });
        let mut seen = 0usize;
        let mut buf = vec![0u8; 4096];
        loop {
            let n = match sr.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            if seen < 20_000 && seen + n > 20_000 {
                buf[20_000 - seen] ^= 0x01;
            }
            seen += n;
            if cw.write_all(&buf[..n]).await.is_err() {
                break;
            }
        }
        let _ = cw.shutdown().await;
        up.abort();
    });
    let (srv, cli) = establish(
        srv_io,
        cli_io,
        TlsImpl::Classic,
        TlsImpl::Kernel,
        server_config(&o),
        client_config(&o),
    )
    .await
    .unwrap();
    let data = pattern(100_000, 3);
    let mut sw = srv.write;
    let d2 = data.clone();
    let w = tokio::spawn(async move {
        let _ = sw.write_all(&d2).await;
        let _ = sw.flush().await;
        sw
    });
    let mut cr = cli.read;
    let mut got = Vec::new();
    let err = cr.read_to_end(&mut got).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
    assert!(err.to_string().to_lowercase().contains("decrypt"), "{err}");
    assert!(got.len() < 20_000);
    assert_eq!(
        &got[..],
        &data[..got.len()],
        "only authentic data before the bad record"
    );
    // The write half refuses to continue on a failed connection.
    let mut cw = cli.write;
    assert!(cw.write_all(b"x").await.is_err());
    drop(w);
    relay.abort();
}

async fn handshake_result(
    server_impl: TlsImpl,
    client_impl: TlsImpl,
    scfg: Arc<ServerConfig>,
    ccfg: Arc<ClientConfig>,
) -> (std::io::Result<()>, std::io::Result<()>) {
    let (a, b) = tokio::io::duplex(1 << 16);
    let srv = async {
        let mut s = stream::accept(server_impl, scfg, a).await?;
        // Make the server notice a client that silently failed.
        let mut b1 = [0u8; 1];
        let _ = tokio::time::timeout(Duration::from_millis(200), s.read.read(&mut b1)).await;
        Ok(())
    };
    let cli = async {
        let mut c = stream::connect(client_impl, ccfg, name(), b).await?;
        // TLS 1.3 client auth failures surface on the client's first read.
        let mut b1 = [0u8; 1];
        match tokio::time::timeout(Duration::from_millis(500), c.read.read(&mut b1)).await {
            Ok(Err(e)) => Err(e),
            _ => Ok(()),
        }
    };
    tokio::join!(srv, cli)
}

#[tokio::test]
async fn untrusted_server_certificate_is_rejected() {
    let o = Opts::default();
    let pki = pki();
    for (s, c) in IMPLS.into_iter().take(3) {
        let ccfg = client_config_with(&o, &pki.stranger_ca, None);
        let (_srv, cli) = handshake_result(s, c, server_config(&o), ccfg).await;
        let err = cli.unwrap_err().to_string();
        assert!(
            err.contains("UnknownIssuer") || err.contains("certificate"),
            "{err}"
        );
    }
}

#[tokio::test]
async fn mtls_required_and_verified() {
    let pki = pki();
    let o = Opts {
        auth: ClientAuth::Required,
        ..Opts::default()
    };
    for (s, c) in IMPLS.into_iter().take(3) {
        // No client certificate: refused.
        let ccfg = client_config_with(&o, &pki.ca, None);
        let (srv, cli) = handshake_result(s, c, server_config(&o), ccfg).await;
        assert!(
            srv.is_err(),
            "server must refuse a client without certificate ({s:?}/{c:?})"
        );
        assert!(cli.is_err(), "client must see the refusal ({s:?}/{c:?})");
        // Certificate from another CA: refused.
        let ccfg = client_config_with(&o, &pki.ca, Some(&pki.stranger));
        let (srv, _cli) = handshake_result(s, c, server_config(&o), ccfg).await;
        let err = srv.unwrap_err().to_string();
        assert!(
            err.contains("UnknownIssuer") || err.contains("certificate"),
            "{err}"
        );
        // Valid certificate: accepted, CN visible to the server.
        let (a, b) = tokio::io::duplex(1 << 16);
        let (srv, cli) = establish(a, b, s, c, server_config(&o), client_config(&o))
            .await
            .unwrap();
        let cn = crate::info::peer_common_name(srv.info.peer_certificates.as_deref());
        assert_eq!(cn.as_deref(), Some("client-1"));
        exchange((srv, cli), 10_000, 10_000, 4).await;
    }
}

#[tokio::test]
async fn revoked_client_certificate_is_rejected() {
    let pki = pki();
    let o = Opts {
        auth: ClientAuth::RequiredWithCrl,
        ..Opts::default()
    };
    for (s, c) in IMPLS.into_iter().take(3) {
        let ccfg = client_config_with(&o, &pki.ca, Some(&pki.revoked_client));
        let (srv, _cli) = handshake_result(s, c, server_config(&o), ccfg).await;
        let err = srv.unwrap_err().to_string();
        assert!(err.contains("Revoked"), "{err}");
        // A non-revoked certificate from the same CA still works.
        let (a, b) = tokio::io::duplex(1 << 16);
        let pair = establish(a, b, s, c, server_config(&o), client_config(&o))
            .await
            .unwrap();
        exchange(pair, 1000, 1000, 5).await;
    }
}

#[tokio::test]
async fn alpn_and_version_policy_unchanged() {
    let o = Opts {
        alpn: vec![b"fluxmq".to_vec()],
        ..Opts::default()
    };
    for (s, c) in IMPLS {
        let (a, b) = tokio::io::duplex(1 << 16);
        let (srv, cli) = establish(a, b, s, c, server_config(&o), client_config(&o))
            .await
            .unwrap();
        assert_eq!(srv.info.alpn_protocol.as_deref(), Some(&b"fluxmq"[..]));
        assert_eq!(cli.info.alpn_protocol.as_deref(), Some(&b"fluxmq"[..]));
        assert_eq!(srv.info.protocol_version, Some(ProtocolVersion::TLSv1_3));
    }
    // A TLS 1.2-only client against a TLS 1.3-only server: no connection on any path.
    let s13 = {
        let pki = pki();
        let mut c = ServerConfig::builder_with_provider(provider_with(None, None))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(pki.server.0.clone(), pki.server.1.clone_key())
            .unwrap();
        c.alpn_protocols = vec![];
        Arc::new(c)
    };
    let c12 = client_config(&Opts {
        tls12_only: true,
        ..Opts::default()
    });
    for (s, c) in IMPLS.into_iter().take(3) {
        let (srv, cli) = handshake_result(s, c, s13.clone(), c12.clone()).await;
        assert!(srv.is_err() && cli.is_err());
    }
}

/// Large single writes (1 MiB frames) and a burst of tiny ones over TCP, fast both ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_and_tiny_frames() {
    let o = Opts::default();
    let (a, b) = tcp_pair().await;
    let (srv, cli) = establish(
        a,
        b,
        TlsImpl::Kernel,
        TlsImpl::Kernel,
        server_config(&o),
        client_config(&o),
    )
    .await
    .unwrap();
    let TlsSplit {
        read: mut sr,
        write: mut sw,
        ..
    } = srv;
    let TlsSplit {
        read: mut cr,
        write: mut cw,
        ..
    } = cli;
    let big = pattern(1 << 20, 9);
    let b2 = big.clone();
    let w = tokio::spawn(async move {
        for _ in 0..8 {
            cw.write_all(&b2).await.unwrap();
        }
        for i in 0..10_000u32 {
            cw.write_all(&i.to_le_bytes()[..1]).await.unwrap();
        }
        cw.shutdown().await.unwrap();
    });
    let mut got = Vec::new();
    sr.read_to_end(&mut got).await.unwrap();
    w.await.unwrap();
    assert_eq!(got.len(), 8 * (1 << 20) + 10_000);
    for k in 0..8 {
        assert!(got[k << 20..(k + 1) << 20] == big[..]);
    }
    for (i, b) in got[8 << 20..].iter().enumerate() {
        assert_eq!(*b, (i as u32).to_le_bytes()[0]);
    }
    sw.shutdown().await.unwrap();
    let mut rest = Vec::new();
    cr.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty());
}

#[test]
fn impl_selection() {
    assert_eq!(TlsImpl::parse("classic"), Some(TlsImpl::Classic));
    assert_eq!(TlsImpl::parse(" FAST "), Some(TlsImpl::Fast));
    assert_eq!(TlsImpl::parse(""), Some(TlsImpl::Fast));
    assert_eq!(TlsImpl::parse("userspace"), Some(TlsImpl::Fast));
    assert_eq!(TlsImpl::parse("KTLS"), Some(TlsImpl::Kernel));
    assert_eq!(TlsImpl::parse("kernel"), Some(TlsImpl::Kernel));
    assert_eq!(TlsImpl::parse("ktls2"), None);
}

/// Fuzz-ish: a relay corrupts, inserts or drops random bytes at a random offset of the
/// server→client stream (after the handshake). The fast reader must never panic and may only
/// ever hand out a prefix of the authentic data, followed by an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hostile_stream_never_yields_forged_data() {
    let o = Opts::default();
    let (scfg, ccfg) = (server_config(&o), client_config(&o));
    for seed in 1..=40u64 {
        let mut rng = Rng(seed * 104_729);
        let at = 6_000 + rng.below(150_000);
        let action = rng.below(3);
        let junk: Vec<u8> = (0..1 + rng.below(40)).map(|_| rng.next() as u8).collect();
        let (srv_io, mitm_s) = tokio::io::duplex(1 << 16);
        let (mitm_c, cli_io) = tokio::io::duplex(1 << 16);
        let relay = tokio::spawn(async move {
            let (mut sr, mut sw) = tokio::io::split(mitm_s);
            let (mut cr, mut cw) = tokio::io::split(mitm_c);
            let up = tokio::spawn(async move { tokio::io::copy(&mut cr, &mut sw).await });
            let mut seen = 0usize;
            let mut buf = vec![0u8; 4096];
            loop {
                let n = match sr.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let mut chunk = buf[..n].to_vec();
                if seen <= at && at < seen + n {
                    let i = at - seen;
                    match action {
                        0 => {
                            for (k, j) in junk.iter().enumerate() {
                                if i + k < chunk.len() {
                                    chunk[i + k] ^= j | 1;
                                }
                            }
                        }
                        1 => {
                            chunk.splice(i..i, junk.iter().copied());
                        }
                        _ => {
                            let end = (i + junk.len()).min(chunk.len());
                            chunk.drain(i..end);
                        }
                    }
                }
                seen += n;
                if cw.write_all(&chunk).await.is_err() {
                    break;
                }
            }
            let _ = cw.shutdown().await;
            up.abort();
        });
        let (srv, cli) = establish(
            srv_io,
            cli_io,
            TlsImpl::Classic,
            TlsImpl::Kernel,
            scfg.clone(),
            ccfg.clone(),
        )
        .await
        .unwrap();
        let data = pattern(200_000, seed);
        let mut sw = srv.write;
        let d2 = data.clone();
        let w = tokio::spawn(async move {
            let _ = sw.write_all(&d2).await;
            let _ = sw.shutdown().await;
        });
        let mut cr = cli.read;
        let mut got = Vec::new();
        let ended = tokio::time::timeout(Duration::from_secs(20), async {
            let mut buf = vec![0u8; 7000];
            loop {
                match cr.read(&mut buf).await {
                    Ok(0) => return Ok(()),
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                    Err(e) => return Err(e),
                }
            }
        })
        .await
        .expect("reader must not hang");
        assert!(
            ended.is_err(),
            "seed {seed}: tampered stream read to a clean end"
        );
        assert!(got.len() < data.len(), "seed {seed}");
        assert!(
            got[..] == data[..got.len()],
            "seed {seed}: forged data handed out"
        );
        // The writer may be stuck behind the stalled relay: stop both.
        w.abort();
        relay.abort();
    }
}

/// Lock-step request/response over TCP with replication's task layout (per side one reader
/// task and one writer task behind a `BufWriter` that flushes per batch), plus a one-way
/// "notify" frame after every request. The next request is only sent once the response has
/// arrived, so nothing else ever pushes bytes along: a record sealed but left unsent until
/// the next write, decrypted data left unread until the next arrival, or a missed waker
/// between the halves turns into a stall here, not a hidden delay. Returns the slowest
/// round trip.
async fn lock_step_max_rtt(server_impl: TlsImpl, client_impl: TlsImpl, rounds: u32) -> Duration {
    use tokio::io::BufWriter;
    use tokio::sync::mpsc;
    let o = Opts::default();
    let (a, b) = tcp_pair().await;
    let (srv, cli) = establish(
        a,
        b,
        server_impl,
        client_impl,
        server_config(&o),
        client_config(&o),
    )
    .await
    .unwrap();
    let TlsSplit {
        read: mut sr,
        write: sw,
        ..
    } = srv;
    let TlsSplit {
        read: mut cr,
        write: cw,
        ..
    } = cli;
    // Frames: u32 length, u32 id (0 = notify, no answer), payload.
    let (stx, mut srx) = mpsc::channel::<u32>(64);
    let server_writer = tokio::spawn(async move {
        let mut w = BufWriter::with_capacity(256 * 1024, sw);
        while let Some(id) = srx.recv().await {
            let mut ids = vec![id];
            while let Ok(id) = srx.try_recv() {
                ids.push(id);
            }
            for id in ids {
                w.write_all(&8u32.to_be_bytes()).await.unwrap();
                w.write_all(&id.to_be_bytes()).await.unwrap();
                w.write_all(&[7u8; 4]).await.unwrap();
            }
            w.flush().await.unwrap();
        }
        w.shutdown().await.unwrap();
    });
    let server_reader = tokio::spawn(async move {
        let (mut len, mut body) = ([0u8; 4], vec![0u8; 4096]);
        while sr.read_exact(&mut len).await.is_ok() {
            let n = u32::from_be_bytes(len) as usize;
            sr.read_exact(&mut body[..n]).await.unwrap();
            let id = u32::from_be_bytes(body[..4].try_into().unwrap());
            if id != 0 && stx.send(id).await.is_err() {
                break;
            }
        }
    });
    let (ctx, mut crx) = mpsc::channel::<u32>(64);
    let client_writer = tokio::spawn(async move {
        let mut w = BufWriter::with_capacity(64 * 1024, cw);
        let mut req = vec![0u8; 4 + 1024];
        req[..4].copy_from_slice(&1024u32.to_be_bytes());
        let mut notify = [0u8; 4 + 24];
        notify[..4].copy_from_slice(&24u32.to_be_bytes());
        while let Some(id) = crx.recv().await {
            req[4..8].copy_from_slice(&id.to_be_bytes());
            w.write_all(&req).await.unwrap();
            w.flush().await.unwrap();
            // A separate small write + flush (replication's progress notify).
            w.write_all(&notify).await.unwrap();
            w.flush().await.unwrap();
        }
        w.shutdown().await.unwrap();
    });
    let (dtx, mut drx) = mpsc::channel::<u32>(64);
    let client_reader = tokio::spawn(async move {
        let mut frame = [0u8; 12];
        while cr.read_exact(&mut frame).await.is_ok() {
            let id = u32::from_be_bytes(frame[4..8].try_into().unwrap());
            if dtx.send(id).await.is_err() {
                break;
            }
        }
    });
    let mut max = Duration::ZERO;
    for id in 1..=rounds {
        let t0 = std::time::Instant::now();
        ctx.send(id).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), drx.recv())
            .await
            .unwrap_or_else(|_| panic!("response {id} stalled"));
        assert_eq!(got, Some(id));
        max = max.max(t0.elapsed());
        if id % 4 == 0 {
            // Idle gaps: the halves must not depend on traffic to make progress.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    drop(ctx);
    client_writer.await.unwrap();
    server_reader.await.unwrap();
    server_writer.await.unwrap();
    client_reader.await.unwrap();
    max
}

/// Every implementation pair answers each lock-step request promptly (the replication
/// fetch / progress pattern at a low rate).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_step_request_response_never_stalls() {
    for (s, c) in [
        (TlsImpl::Kernel, TlsImpl::Kernel),
        (TlsImpl::Kernel, TlsImpl::Classic),
        (TlsImpl::Classic, TlsImpl::Kernel),
        (TlsImpl::Classic, TlsImpl::Classic),
    ] {
        let max = lock_step_max_rtt(s, c, 1000).await;
        eprintln!("server {s:?} client {c:?}: slowest round trip {max:?}");
        // Generous for loaded CI hosts; a stall waits for a timer or the next write (seconds
        // here, since nothing else is written).
        assert!(
            max < Duration::from_millis(500),
            "server {s:?} client {c:?}: slowest round trip {max:?}"
        );
    }
}

// ------------------------------------------------------------------- kernel TLS (v0.10)

/// True when this host gives a TCP connection kernel TLS; otherwise the kTLS tests print
/// why and pass vacuously (macOS, or Linux without the `tls` module: `modprobe tls`).
async fn kernel_tls_here() -> bool {
    if !super::ktls::available() {
        eprintln!("kernel TLS not available here: skipping");
        return false;
    }
    let o = Opts::default();
    let (a, b) = tcp_pair().await;
    let pair = establish(
        a,
        b,
        TlsImpl::Kernel,
        TlsImpl::Kernel,
        server_config(&o),
        client_config(&o),
    )
    .await
    .unwrap();
    if !pair.0.info.kernel_tls {
        eprintln!("kernel TLS refused (no `tls` module?): skipping");
        return false;
    }
    true
}

fn is_kernel(p: &TlsSplit<tokio::net::TcpStream>) -> bool {
    match (&p.read, &p.write) {
        (TlsRead::Fast(r), TlsWrite::Fast(w)) => {
            assert_eq!(r.is_kernel_tls(), w.is_kernel_tls());
            assert_eq!(r.is_kernel_tls(), p.info.kernel_tls);
            r.is_kernel_tls()
        }
        _ => false,
    }
}

/// Every TLS 1.3 suite on kernel TLS, against every peer implementation, both roles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ktls_every_suite_every_peer() {
    if !kernel_tls_here().await {
        return;
    }
    let mut n = 0;
    for suite in [
        CipherSuite::TLS13_AES_128_GCM_SHA256,
        CipherSuite::TLS13_AES_256_GCM_SHA384,
        CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
    ] {
        for (s, c) in TCP_IMPLS {
            let o = Opts {
                suites: Some(vec![suite]),
                ..Opts::default()
            };
            let (a, b) = tcp_pair().await;
            let pair = establish(a, b, s, c, server_config(&o), client_config(&o))
                .await
                .unwrap_or_else(|e| panic!("{suite:?} {s:?}/{c:?}: {e}"));
            // AES-GCM is in every kTLS kernel; ChaCha20-Poly1305 only where the kernel has
            // CONFIG_CRYPTO_CHACHA20POLY1305 (not in WSL): then the userspace layer serves.
            let aes = suite != CipherSuite::TLS13_CHACHA20_POLY1305_SHA256;
            for (side, imp) in [(&pair.0, s), (&pair.1, c)] {
                let k = is_kernel(side);
                assert!(
                    !k || imp == TlsImpl::Kernel,
                    "{suite:?}: kernel TLS on {imp:?}"
                );
                if aes {
                    assert_eq!(k, imp == TlsImpl::Kernel, "{suite:?} {imp:?}");
                }
            }
            n += 1;
            exchange(pair, 700_000, 500_000, 3000 + n).await;
        }
    }
}

/// The kernel rotates keys at the confidentiality limit (our KeyUpdate, the kernel's new
/// key) and applies the peer's KeyUpdates (re-keying needs Linux 6.14+).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ktls_key_updates_at_confidentiality_limit() {
    if !kernel_tls_here().await {
        return;
    }
    for (s, c) in [
        (TlsImpl::Kernel, TlsImpl::Kernel),
        (TlsImpl::Kernel, TlsImpl::Classic),
        (TlsImpl::Classic, TlsImpl::Kernel),
        (TlsImpl::Fast, TlsImpl::Kernel),
    ] {
        let o = Opts {
            limit: Some(5),
            ..Opts::default()
        };
        let (a, b) = tcp_pair().await;
        let pair = establish(a, b, s, c, server_config(&o), client_config(&o))
            .await
            .unwrap();
        let k = if s == TlsImpl::Kernel {
            &pair.0
        } else {
            &pair.1
        };
        assert!(is_kernel(k));
        let shared = fast_shared(&k.read).clone();
        exchange(pair, 1 << 20, 1 << 20, 950).await;
        assert!(
            shared.tx_updates.load(Ordering::Relaxed) >= 10,
            "tx updates"
        );
        // How often the peer rotates depends on how its implementation cuts records.
        assert!(shared.rx_updates.load(Ordering::Relaxed) >= 5, "rx updates");
    }
}

/// A bit flipped on the wire: the kernel rejects the record, nothing forged is returned,
/// and the connection refuses further writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ktls_tampering_is_detected() {
    if !kernel_tls_here().await {
        return;
    }
    let o = Opts::default();
    // server <-TCP-> relay <-TCP-> client; the relay flips one bit after 20 000 bytes.
    let (srv_io, relay_s) = tcp_pair().await;
    let (relay_c, cli_io) = tcp_pair().await;
    let relay = tokio::spawn(async move {
        let (mut sr, mut sw) = relay_s.into_split();
        let (mut cr, mut cw) = relay_c.into_split();
        let up = tokio::spawn(async move { tokio::io::copy(&mut cr, &mut sw).await });
        let mut seen = 0usize;
        let mut buf = vec![0u8; 4096];
        loop {
            let n = match sr.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            if seen < 20_000 && seen + n > 20_000 {
                buf[20_000 - seen] ^= 0x01;
            }
            seen += n;
            if cw.write_all(&buf[..n]).await.is_err() {
                break;
            }
        }
        let _ = cw.shutdown().await;
        up.abort();
    });
    let (srv, cli) = establish(
        srv_io,
        cli_io,
        TlsImpl::Classic,
        TlsImpl::Kernel,
        server_config(&o),
        client_config(&o),
    )
    .await
    .unwrap();
    assert!(is_kernel(&cli));
    let data = pattern(100_000, 3);
    let mut sw = srv.write;
    let d2 = data.clone();
    let w = tokio::spawn(async move {
        let _ = sw.write_all(&d2).await;
        let _ = sw.flush().await;
        sw
    });
    let mut cr = cli.read;
    let mut got = Vec::new();
    let err = cr.read_to_end(&mut got).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
    assert!(got.len() < 20_000);
    assert_eq!(
        &got[..],
        &data[..got.len()],
        "only authentic data before the bad record"
    );
    let mut cw = cli.write;
    assert!(cw.write_all(b"x").await.is_err());
    drop(w);
    relay.abort();
}

/// EOF without close_notify is an error on kernel TLS too, at a record boundary and in the
/// middle of a record; a received close_notify is a clean EOF.
#[tokio::test]
async fn ktls_truncation_and_clean_close() {
    if !kernel_tls_here().await {
        return;
    }
    let o = Opts::default();
    for cut_mid_record in [false, true] {
        let (a, b) = tcp_pair().await;
        let scfg = server_config(&o);
        let srv = async move {
            let mut s = tokio_rustls::TlsAcceptor::from(scfg)
                .accept(a)
                .await
                .unwrap();
            s.write_all(&pattern(40_000, 1)).await.unwrap();
            s.flush().await.unwrap();
            let (mut tcp, _) = s.into_inner();
            if cut_mid_record {
                tcp.write_all(&[23, 3, 3, 0x40, 0x00, 1, 2, 3])
                    .await
                    .unwrap();
            }
            drop(tcp);
        };
        let cli = stream::connect(TlsImpl::Kernel, client_config(&o), name(), b);
        let ((), cli) = tokio::join!(srv, cli);
        let cli = cli.unwrap();
        assert!(is_kernel(&cli));
        let mut cr = cli.read;
        let mut got = Vec::new();
        let err = cr.read_to_end(&mut got).await.unwrap_err();
        assert!(
            matches!(
                err.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::InvalidData
            ),
            "{err}"
        );
        assert_eq!(got.len(), 40_000, "data before the cut is delivered");
    }
    for (s, c) in [
        (TlsImpl::Kernel, TlsImpl::Kernel),
        (TlsImpl::Kernel, TlsImpl::Classic),
        (TlsImpl::Classic, TlsImpl::Kernel),
    ] {
        let (a, b) = tcp_pair().await;
        let (srv, cli) = establish(a, b, s, c, server_config(&o), client_config(&o))
            .await
            .unwrap();
        let mut sw = srv.write;
        sw.write_all(b"bye").await.unwrap();
        sw.shutdown().await.unwrap();
        let mut cr = cli.read;
        let mut got = Vec::new();
        cr.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, b"bye", "{s:?}/{c:?}");
        assert!(sw.write_all(b"x").await.is_err());
    }
}

/// Data the client sends right behind its Finished arrives in the same segment as the
/// handshake: the server opens it in userspace, reads up to a record boundary, then hands
/// over to the kernel. Small reads (less than one record of room) go through the stash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ktls_handover_with_buffered_records_and_small_reads() {
    if !kernel_tls_here().await {
        return;
    }
    let o = Opts::default();
    for burst in [1usize, 100, 16_384, 40_000, 300_000] {
        let (a, b) = tcp_pair().await;
        let ccfg = client_config(&o);
        let data = pattern(burst, burst as u64);
        let d2 = data.clone();
        // A classic client writes immediately after connect: its first records follow
        // the Finished flight without a round trip.
        let cli = tokio::spawn(async move {
            let mut c = tokio_rustls::TlsConnector::from(ccfg)
                .connect(name(), b)
                .await
                .unwrap();
            c.write_all(&d2).await.unwrap();
            c.flush().await.unwrap();
            let mut echo = vec![0u8; d2.len()];
            c.read_exact(&mut echo).await.unwrap();
            assert!(echo == d2);
            c.shutdown().await.unwrap();
        });
        let srv = stream::accept(TlsImpl::Kernel, server_config(&o), a)
            .await
            .unwrap();
        assert!(is_kernel(&srv), "burst {burst}");
        let TlsSplit {
            read: mut sr,
            write: mut sw,
            ..
        } = srv;
        let mut got = Vec::new();
        let mut small = [0u8; 777];
        while got.len() < data.len() {
            let n = sr.read(&mut small).await.unwrap();
            assert!(n > 0);
            got.extend_from_slice(&small[..n]);
        }
        assert!(got == data, "burst {burst}");
        sw.write_all(&got).await.unwrap();
        sw.flush().await.unwrap();
        let mut rest = Vec::new();
        sr.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
        cli.await.unwrap();
    }
}
