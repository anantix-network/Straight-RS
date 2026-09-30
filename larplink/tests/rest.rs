mod common;
use common::*;
use larplink::model::{SessionUpdate, UpdatePlayer};
use larplink::rest::RestClient;
use larplink::{Error, GuildId, LoadResult, NodeConfig};
use std::time::Duration;

fn rest(m: &Mock) -> RestClient {
    let mut cfg = NodeConfig::new(m.host(), "pw");
    cfg.request_timeout = Duration::from_secs(3);
    RestClient::new(&cfg, "larplink-test")
}

#[tokio::test]
async fn load_tracks_percent_encodes_identifier() {
    let m = Mock::start().await;
    let r = rest(&m).load_tracks("ytsearch:foo bar & baz?").await.unwrap();
    assert!(matches!(r, LoadResult::Empty(_)));
    let req = &m.requests_matching("GET", "/v4/loadtracks")[0];
    assert_eq!(req.query, "identifier=ytsearch%3Afoo%20bar%20%26%20baz%3F");
}

#[tokio::test]
async fn get_retries_on_503_then_succeeds() {
    let m = Mock::start().await;
    m.state.fail_next_gets.store(2, std::sync::atomic::Ordering::SeqCst);
    assert!(rest(&m).load_tracks("x").await.is_ok());
    assert_eq!(m.requests_matching("GET", "/v4/loadtracks").len(), 3);
}

#[tokio::test]
async fn non_json_error_body_becomes_lavalink_error_after_retries() {
    let m = Mock::start().await;
    *m.state.load_response.lock().unwrap() = Some((502, "<html>bad gateway</html>".into()));
    match rest(&m).load_tracks("x").await.unwrap_err() {
        Error::Lavalink { status, message, .. } => {
            assert_eq!(status, 502);
            assert!(message.contains("bad gateway"));
        }
        e => panic!("{e:?}"),
    }
    assert_eq!(m.requests_matching("GET", "/v4/loadtracks").len(), 3);
}

#[tokio::test]
async fn lavalink_json_error_is_parsed_and_not_retried() {
    let m = Mock::start().await;
    let e = rest(&m).get_players("nope").await.unwrap_err(); // mock 404s unknown routes
    match e {
        Error::Lavalink { status, message, error, .. } => {
            assert_eq!((status, message.as_str(), error.as_str()), (404, "Session not found", "Not Found"));
        }
        e => panic!("{e:?}"),
    }
    assert_eq!(m.requests().len(), 1);
}

#[tokio::test]
async fn update_and_destroy_player_and_session() {
    let m = Mock::start().await;
    let r = rest(&m);
    let upd = UpdatePlayer { volume: Some(40), ..Default::default() };
    let p = r.update_player("mock-session", GuildId(7), &upd, true).await.unwrap();
    assert_eq!(p.volume, 40);
    let req = &m.requests_matching("PATCH", "/v4/sessions/mock-session/players/7")[0];
    assert_eq!(req.query, "noReplace=true");
    assert_eq!(req.body, serde_json::json!({"volume": 40}));
    r.destroy_player("mock-session", GuildId(7)).await.unwrap();
    let s = r.update_session("mock-session", &SessionUpdate { resuming: Some(true), timeout: Some(60) }).await.unwrap();
    assert!(s.resuming && s.timeout == 60);
}

#[tokio::test]
async fn misc_endpoints() {
    let m = Mock::start().await;
    let r = rest(&m);
    assert_eq!(r.version().await.unwrap(), "4.0.8");
    assert_eq!(r.info().await.unwrap().version.major, 4);
    assert_eq!(r.stats().await.unwrap().players, 3);
    assert!(r.route_planner_status().await.unwrap().class.is_none());
    r.free_address("1.2.3.4").await.unwrap();
    r.free_all().await.unwrap();
    assert_eq!(&*r.decode_track("QAAA").await.unwrap().encoded, "QAAA");
    assert_eq!(r.decode_tracks(&["QAAA".to_string()]).await.unwrap().len(), 1);
}

#[tokio::test]
async fn timeout_is_reported() {
    let m = Mock::start().await;
    m.state.patch_delay_ms.store(500, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = NodeConfig::new(m.host(), "pw");
    cfg.request_timeout = Duration::from_millis(100);
    let r = RestClient::new(&cfg, "t");
    let e = r.update_player("s", GuildId(1), &UpdatePlayer::default(), false).await.unwrap_err();
    assert!(matches!(e, Error::Timeout));
}

#[tokio::test]
async fn kill_severs_keepalive_connections() {
    let m = Mock::start().await;
    let r = rest(&m);
    assert_eq!(r.version().await.unwrap(), "4.0.8"); // pooled keep-alive connection
    m.kill();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(r.version().await.is_err());
}

#[tokio::test]
async fn in_flight_is_released_when_request_is_dropped() {
    let m = Mock::start().await;
    m.state.patch_delay_ms.store(300, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = NodeConfig::new(m.host(), "pw");
    cfg.request_timeout = Duration::from_millis(50);
    let r = RestClient::new(&cfg, "t");
    assert!(r.update_player("s", GuildId(1), &UpdatePlayer::default(), false).await.is_err());
    eventually(Duration::from_secs(2), || m.state.in_flight.load(std::sync::atomic::Ordering::SeqCst) == 0).await;
}
