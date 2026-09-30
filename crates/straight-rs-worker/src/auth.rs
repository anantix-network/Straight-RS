use crate::config::SecretString;
use axum::{
    Json, Router,
    extract::{ConnectInfo, Request, State},
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug)]
pub struct RateLimitConfig {
    pub requests: usize,
    pub window: Duration,
    pub table_capacity: usize,
}
struct Bucket {
    started: Instant,
    count: usize,
}
struct Inner {
    token: Box<[u8]>,
    limits: RateLimitConfig,
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
}
#[derive(Clone)]
pub struct AuthState(Arc<Inner>);
impl std::fmt::Debug for AuthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthState([REDACTED])")
    }
}
impl AuthState {
    pub fn new(api_token: SecretString, limits: RateLimitConfig) -> Self {
        Self(Arc::new(Inner {
            token: api_token.expose_secret().as_bytes().into(),
            limits,
            buckets: Mutex::new(HashMap::new()),
        }))
    }
    fn admit(&self, peer: IpAddr) -> bool {
        let mut buckets = self.0.buckets.lock().expect("rate limiter lock poisoned");
        let now = Instant::now();
        if let Some(bucket) = buckets.get_mut(&peer) {
            if now.duration_since(bucket.started) >= self.0.limits.window {
                bucket.started = now;
                bucket.count = 0;
            }
            if bucket.count >= self.0.limits.requests {
                return false;
            }
            bucket.count += 1;
            return true;
        }
        if buckets.len() >= self.0.limits.table_capacity {
            buckets.retain(|_, bucket| now.duration_since(bucket.started) < self.0.limits.window);
        }
        if buckets.len() >= self.0.limits.table_capacity {
            return false;
        }
        buckets.insert(
            peer,
            Bucket {
                started: now,
                count: 1,
            },
        );
        true
    }
}
#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}
#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}
pub(crate) fn error_response(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorBody { code, message },
        }),
    )
        .into_response()
}
async fn authenticate(State(state): State<AuthState>, request: Request, next: Next) -> Response {
    let valid = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| {
            h.get(..7)
                .filter(|scheme| scheme.eq_ignore_ascii_case("Bearer "))
                .map(|_| &h[7..])
        })
        .map(|candidate| bool::from(candidate.as_bytes().ct_eq(state.0.token.as_ref())))
        .unwrap_or(false);
    if !valid {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid bearer token is required.",
        );
    }
    let Some(ConnectInfo(peer)) = request.extensions().get::<ConnectInfo<SocketAddr>>() else {
        return error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Request limit exceeded.",
        );
    };
    if !state.admit(peer.ip()) {
        return error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Request limit exceeded.",
        );
    }
    next.run(request).await
}
async fn sanitize(request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    let status = response.status();
    if status.is_client_error() || status.is_server_error() {
        let (code, msg) = match status {
            StatusCode::NOT_FOUND => ("not_found", "Route or resource not found."),
            StatusCode::UNAUTHORIZED => ("unauthorized", "A valid bearer token is required."),
            StatusCode::TOO_MANY_REQUESTS => ("rate_limited", "Request limit exceeded."),
            StatusCode::PAYLOAD_TOO_LARGE => (
                "body_too_large",
                "Request body exceeds the configured limit.",
            ),
            _ if status.is_server_error() => {
                ("internal_error", "The request could not be completed.")
            }
            _ => ("invalid_request", "Request is invalid."),
        };
        return error_response(status, code, msg);
    }
    response
}
pub fn secure(router: Router, state: AuthState) -> Router {
    router
        .layer(middleware::from_fn_with_state(state, authenticate))
        .layer(middleware::from_fn(sanitize))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn table_capacity_rejects_new_peer() {
        let s = AuthState::new(
            SecretString::new("synthetic-api-secret-32-bytes-long"),
            RateLimitConfig {
                requests: 1,
                window: Duration::from_secs(1),
                table_capacity: 1,
            },
        );
        assert!(s.admit("127.0.0.1".parse().unwrap()));
        assert!(!s.admit("127.0.0.2".parse().unwrap()));
    }
    #[test]
    fn debug_does_not_expose_secret() {
        let s = AuthState::new(
            SecretString::new("synthetic-api-secret-32-bytes-long"),
            RateLimitConfig {
                requests: 1,
                window: Duration::from_secs(1),
                table_capacity: 1,
            },
        );
        assert!(!format!("{s:?}").contains("synthetic-api-secret"));
    }
}
