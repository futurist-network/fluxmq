//! Serves the web console with SPA fallback: built into the binary (`ui/dist` at compile time),
//! from a directory (`--ui-dir`), or not at all (`--ui off`).

use std::path::{Path, PathBuf};

use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};
use tower_http::services::{ServeDir, ServeFile};

/// Where the console comes from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum UiSource {
    /// Built into the binary from `ui/dist` (empty when the binary was built without it).
    #[default]
    Embedded,
    /// Served from a directory (`--ui-dir`), e.g. while developing the console.
    Dir(PathBuf),
    /// No console: everything outside the API is 404.
    Off,
}

/// `ui/dist` as it was when this crate was compiled. Release builds embed the files; debug
/// builds read them from disk, so a rebuilt console shows up without recompiling.
#[derive(rust_embed::Embed)]
#[folder = "../../ui/dist"]
#[allow_missing = true]
struct Embedded;

const NOT_BUILT: &str = r#"<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>FluxMQ</title>
<style>
  body { font-family: system-ui, sans-serif; background: #0b0d12; color: #e6e8ee; margin: 0;
         display: grid; place-items: center; min-height: 100vh; }
  main { max-width: 36rem; padding: 2rem; }
  code, pre { background: #161a22; border-radius: 6px; padding: .15rem .4rem; }
  pre { padding: .75rem 1rem; overflow-x: auto; }
  a { color: #7aa2ff; }
</style></head>
<body><main>
  <h1>FluxMQ is running</h1>
  <p>This binary was built without the web console, or <code>--ui-dir</code> points to a
     missing directory.</p>
  <p>Build the console, then rebuild the broker (or pass <code>--ui-dir ui/dist</code>):</p>
  <pre>cd ui &amp;&amp; pnpm install &amp;&amp; pnpm build</pre>
  <p>The REST API is available under <a href="/api/health">/api/health</a>.</p>
</main></body></html>
"#;

/// Adds the console as the router's fallback.
pub fn attach<S: Clone + Send + Sync + 'static>(router: Router<S>, ui: &UiSource) -> Router<S> {
    let router = match ui {
        UiSource::Off => return router.fallback(|| async { StatusCode::NOT_FOUND }),
        UiSource::Embedded if Embedded::get("index.html").is_some() => router.fallback(embedded),
        UiSource::Embedded => router.fallback(not_built),
        UiSource::Dir(dir) => attach_dir(router, dir),
    };
    router.layer(axum::middleware::from_fn(console_headers))
}

/// Caching and hardening headers for console responses (API, WebSocket and cluster routes are
/// left alone). Hashed `/assets/*` files never change; `index.html` (every client-side route)
/// must be revalidated, or a browser keeps a cached page that points at the previous build's
/// chunks after an upgrade. The console is never framed (clickjacking).
async fn console_headers(req: Request, next: Next) -> Response {
    let path = req.uri().path();
    if ["/api/", "/ws", "/internal/"]
        .iter()
        .any(|p| path.starts_with(p))
    {
        return next.run(req).await;
    }
    let asset = path.starts_with("/assets/");
    let mut res = next.run(req).await;
    // A missing hashed asset (a stale page asking for the previous build) is a 404, not the
    // SPA fallback: HTML served as a script fails confusingly and must never be cached.
    let html = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/html"));
    if asset && html {
        res = StatusCode::NOT_FOUND.into_response();
    }
    let cache = if asset && res.status() == StatusCode::OK {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    let h = res.headers_mut();
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("same-origin"),
    );
    res
}

fn attach_dir<S: Clone + Send + Sync + 'static>(router: Router<S>, dir: &Path) -> Router<S> {
    if !dir.join("index.html").is_file() {
        tracing::warn!(dir = %dir.display(), "UI not found; serving placeholder page");
        return router.fallback(not_built);
    }
    // `fallback` (not `not_found_service`) keeps 200 for client-side routes.
    let serve = ServeDir::new(dir)
        .append_index_html_on_directories(true)
        .fallback(ServeFile::new(dir.join("index.html")));
    router.fallback_service(serve)
}

/// The embedded file, or `index.html` for client-side routes (same as the directory mode).
async fn embedded(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let file = Embedded::get(path)
        .filter(|_| !path.is_empty())
        .or_else(|| Embedded::get("index.html"));
    match file {
        Some(f) => (
            [(header::CONTENT_TYPE, f.metadata.mimetype().to_string())],
            f.data,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn not_built(uri: Uri) -> impl IntoResponse {
    let status = if uri.path() == "/" || uri.path() == "/index.html" {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    };
    (status, Html(NOT_BUILT))
}
