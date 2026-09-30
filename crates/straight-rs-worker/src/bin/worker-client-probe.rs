use std::{
    env,
    io::{Read, Write},
    net::TcpStream,
    process,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("worker-client-probe: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = required_env("WORKER_API_BASE_URL")?;
    let token = required_env("WORKER_API_TOKEN")?;
    let guild = required_env("GUILD_ID")?;
    let channel = required_env("CHANNEL_ID")?;
    let track = required_env("TRACK_IDENTIFIER")?;
    let operation = env::args()
        .nth(1)
        .ok_or("expected operation: join-play or query")?;
    if !base_url.starts_with("http://") {
        return Err("WORKER_API_BASE_URL must use http://".into());
    }
    let authority = base_url
        .strip_prefix("http://")
        .unwrap()
        .trim_end_matches('/');

    match operation.as_str() {
        "join-play" => {
            let join = request(
                authority,
                &token,
                "POST",
                &format!("/v1/guilds/{guild}/join"),
                &format!(r#"{{"channel_id":"{channel}"}}"#),
            )?;
            expect_status("join", join, 204)?;
            let play = request(
                authority,
                &token,
                "POST",
                &format!("/v1/guilds/{guild}/play"),
                &format!(r#"{{"identifier":{}}}"#, serde_json::to_string(&track)?),
            )?;
            expect_status("play", play, 204)?;
        }
        "query" => {
            let response = request(
                authority,
                &token,
                "GET",
                &format!("/v1/guilds/{guild}/player"),
                "",
            )?;
            if response.status != 200 {
                return Err(format!(
                    "player query returned HTTP {}: {}",
                    response.status, response.body
                )
                .into());
            }
            println!("{}", response.body);
        }
        _ => return Err("expected operation: join-play or query".into()),
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

struct Response {
    status: u16,
    body: String,
}

fn request(
    authority: &str,
    token: &str,
    method: &str,
    path: &str,
    body: &str,
) -> Result<Response, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(authority)?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or("invalid HTTP response")?;
    let status = head
        .lines()
        .next()
        .ok_or("missing HTTP status line")?
        .split_whitespace()
        .nth(1)
        .ok_or("missing HTTP status code")?
        .parse()?;
    Ok(Response {
        status,
        body: body.to_owned(),
    })
}

fn expect_status(
    operation: &str,
    response: Response,
    expected: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    if response.status != expected {
        return Err(format!(
            "{operation} returned HTTP {}: {}",
            response.status, response.body
        )
        .into());
    }
    Ok(())
}
