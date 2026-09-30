use crate::{Error, NodeConfig, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::de::DeserializeOwned;
use std::time::Duration;
use straight_rs_model::{
    GuildId, Info, LoadResult, Player, RestError, RoutePlannerStatus, Session, SessionUpdate,
    Stats, Track, UpdatePlayer,
};

#[cfg(feature = "tls")]
type Conn = hyper_rustls::HttpsConnector<HttpConnector>;
#[cfg(not(feature = "tls"))]
type Conn = HttpConnector;

const RETRIES: u32 = 2;

/// NON_ALPHANUMERIC minus the RFC 3986 unreserved marks, so ids like `mock-session` stay readable.
const ENC: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

#[derive(Clone)]
pub struct RestClient {
    client: Client<Conn, Full<Bytes>>,
    origin: String,
    password: String,
    user_agent: String,
    timeout: Duration,
}

fn enc(s: &str) -> String {
    utf8_percent_encode(s, ENC).to_string()
}

impl RestClient {
    pub fn new(cfg: &NodeConfig, client_name: &str) -> Self {
        #[allow(unused_mut)]
        let mut http = HttpConnector::new();
        http.set_nodelay(true);
        #[cfg(feature = "tls")]
        let conn = {
            http.enforce_http(false);
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_webpki_roots()
                .https_or_http()
                .enable_http1()
                .wrap_connector(http)
        };
        #[cfg(not(feature = "tls"))]
        let conn = http;
        Self {
            client: Client::builder(TokioExecutor::new()).build(conn),
            origin: format!(
                "{}://{}",
                if cfg.secure { "https" } else { "http" },
                cfg.host
            ),
            password: cfg.password.clone(),
            user_agent: client_name.to_owned(),
            timeout: cfg.request_timeout,
        }
    }

    async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<(StatusCode, Bytes)> {
        let b = Request::builder()
            .method(method)
            .uri(format!("{}{}", self.origin, path))
            .header("Authorization", &self.password)
            .header("User-Agent", &self.user_agent);
        let req = match body {
            Some(v) => b
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(v))),
            None => b.body(Full::new(Bytes::new())),
        }
        .map_err(|e| Error::Config(e.to_string()))?;
        let fut = async {
            let resp = self.client.request(req).await?;
            let status = resp.status();
            let bytes = resp.into_body().collect().await?.to_bytes();
            Ok::<_, Error>((status, bytes))
        };
        tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| Error::Timeout)?
    }

    fn check(status: StatusCode, path: &str, body: &[u8]) -> Result<()> {
        if status.is_success() {
            return Ok(());
        }
        Err(match serde_json::from_slice::<RestError>(body) {
            Ok(e) => Error::Lavalink {
                status: if e.status == 0 {
                    status.as_u16()
                } else {
                    e.status
                },
                error: e.error,
                message: e.message,
                path: e.path,
            },
            Err(_) => Error::Lavalink {
                status: status.as_u16(),
                error: status.canonical_reason().unwrap_or("").to_owned(),
                message: String::from_utf8_lossy(body).chars().take(200).collect(),
                path: path.to_owned(),
            },
        })
    }

    /// Idempotent GET with retries on transient failures.
    async fn get(&self, path: &str) -> Result<Bytes> {
        let mut attempt = 0;
        loop {
            let res = self.send(Method::GET, path, None).await;
            let transient = match &res {
                Ok((s, _)) => matches!(s.as_u16(), 502..=504),
                Err(Error::Http(_) | Error::HttpClient(_) | Error::Timeout) => true,
                Err(_) => false,
            };
            if transient && attempt < RETRIES {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt))).await;
                continue;
            }
            let (status, body) = res?;
            Self::check(status, path, &body)?;
            return Ok(body);
        }
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        Ok(serde_json::from_slice(&self.get(path).await?)?)
    }

    async fn write(&self, method: Method, path: &str, body: Option<Vec<u8>>) -> Result<Bytes> {
        let (status, bytes) = self.send(method, path, body).await?;
        Self::check(status, path, &bytes)?;
        Ok(bytes)
    }

    pub async fn load_tracks(&self, identifier: &str) -> Result<LoadResult> {
        self.get_json(&format!("/v4/loadtracks?identifier={}", enc(identifier)))
            .await
    }

    pub async fn decode_track(&self, encoded: &str) -> Result<Track> {
        self.get_json(&format!("/v4/decodetrack?encodedTrack={}", enc(encoded)))
            .await
    }

    pub async fn decode_tracks(&self, encoded: &[String]) -> Result<Vec<Track>> {
        let body = serde_json::to_vec(encoded)?;
        let out = self
            .write(Method::POST, "/v4/decodetracks", Some(body))
            .await?;
        Ok(serde_json::from_slice(&out)?)
    }

    pub async fn get_players(&self, session: &str) -> Result<Vec<Player>> {
        self.get_json(&format!("/v4/sessions/{}/players", enc(session)))
            .await
    }

    /// `GET /v4/sessions/{session}/players/{guild}`: the server's view of one player.
    pub async fn get_player(&self, session: &str, guild: GuildId) -> Result<Player> {
        self.get_json(&format!("/v4/sessions/{}/players/{}", enc(session), guild))
            .await
    }

    pub async fn update_player(
        &self,
        session: &str,
        guild: GuildId,
        upd: &UpdatePlayer,
        no_replace: bool,
    ) -> Result<Player> {
        let path = format!(
            "/v4/sessions/{}/players/{}?noReplace={}",
            enc(session),
            guild,
            no_replace
        );
        let out = self
            .write(Method::PATCH, &path, Some(serde_json::to_vec(upd)?))
            .await?;
        Ok(serde_json::from_slice(&out)?)
    }

    pub async fn destroy_player(&self, session: &str, guild: GuildId) -> Result<()> {
        let path = format!("/v4/sessions/{}/players/{}", enc(session), guild);
        self.write(Method::DELETE, &path, None).await.map(|_| ())
    }

    pub async fn update_session(&self, session: &str, upd: &SessionUpdate) -> Result<Session> {
        let path = format!("/v4/sessions/{}", enc(session));
        let out = self
            .write(Method::PATCH, &path, Some(serde_json::to_vec(upd)?))
            .await?;
        Ok(serde_json::from_slice(&out)?)
    }

    pub async fn info(&self) -> Result<Info> {
        self.get_json("/v4/info").await
    }

    pub async fn version(&self) -> Result<String> {
        Ok(String::from_utf8_lossy(&self.get("/version").await?)
            .trim()
            .to_owned())
    }

    pub async fn stats(&self) -> Result<Stats> {
        self.get_json("/v4/stats").await
    }

    pub async fn route_planner_status(&self) -> Result<RoutePlannerStatus> {
        let body = self.get("/v4/routeplanner/status").await?;
        if body.iter().all(u8::is_ascii_whitespace) {
            return Ok(RoutePlannerStatus::default());
        }
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn free_address(&self, address: &str) -> Result<()> {
        let body = serde_json::to_vec(&serde_json::json!({ "address": address }))?;
        self.write(Method::POST, "/v4/routeplanner/free/address", Some(body))
            .await
            .map(|_| ())
    }

    pub async fn free_all(&self) -> Result<()> {
        self.write(Method::POST, "/v4/routeplanner/free/all", None)
            .await
            .map(|_| ())
    }
}
