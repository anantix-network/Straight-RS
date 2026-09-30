use serde_json::{Value, json};
use straight_rs_model::*;

pub const TRACK: &str = r#"{"encoded":"QAAA","info":{"identifier":"dQw4w9WgXcQ","isSeekable":true,"author":"RickAstleyVEVO","length":212000,"isStream":false,"position":0,"title":"Never Gonna Give You Up","uri":"https://www.youtube.com/watch?v=dQw4w9WgXcQ","artworkUrl":"https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg","isrc":null,"sourceName":"youtube"},"pluginInfo":{},"userData":{}}"#;

#[test]
fn ids_accept_string_or_number_and_keep_precision() {
    let g: GuildId = serde_json::from_str("\"18446744073709551615\"").unwrap();
    assert_eq!(g.0, u64::MAX);
    let g: GuildId = serde_json::from_str("123").unwrap();
    assert_eq!(g, GuildId(123));
    assert_eq!(serde_json::to_string(&GuildId(5)).unwrap(), "\"5\"");
    assert!(serde_json::from_str::<GuildId>("\"abc\"").is_err());
}

#[test]
fn track_roundtrips_exactly() {
    let t: Track = serde_json::from_str(TRACK).unwrap();
    assert_eq!(&*t.encoded, "QAAA");
    assert_eq!(t.info.length, 212000);
    assert!(t.info.is_seekable && !t.info.is_stream);
    assert_eq!(t.info.isrc, None);
    assert_eq!(
        serde_json::to_value(&t).unwrap(),
        serde_json::from_str::<Value>(TRACK).unwrap()
    );
}

#[test]
fn track_keeps_plugin_info_and_user_data() {
    let mut v: Value = serde_json::from_str(TRACK).unwrap();
    v["pluginInfo"] = json!({"albumName": "x"});
    v["userData"] = json!({"requester": 1});
    let t: Track = serde_json::from_value(v.clone()).unwrap();
    assert_eq!(t.plugin_info["albumName"], "x");
    assert_eq!(serde_json::to_value(&t).unwrap(), v);
}

#[test]
fn load_result_variants() {
    let track = format!(r#"{{"loadType":"track","data":{TRACK}}}"#);
    assert!(matches!(
        serde_json::from_str::<LoadResult>(&track).unwrap(),
        LoadResult::Track(_)
    ));

    let search = format!(r#"{{"loadType":"search","data":[{TRACK},{TRACK}]}}"#);
    match serde_json::from_str::<LoadResult>(&search).unwrap() {
        LoadResult::Search(v) => assert_eq!(v.len(), 2),
        other => panic!("{other:?}"),
    }

    let pl = format!(
        r#"{{"loadType":"playlist","data":{{"info":{{"name":"Mix","selectedTrack":-1}},"pluginInfo":{{}},"tracks":[{TRACK}]}}}}"#
    );
    match serde_json::from_str::<LoadResult>(&pl).unwrap() {
        LoadResult::Playlist(p) => {
            assert_eq!(p.info.name, "Mix");
            assert_eq!(p.info.selected_track, -1);
            assert_eq!(p.tracks.len(), 1);
        }
        other => panic!("{other:?}"),
    }

    assert!(matches!(
        serde_json::from_str::<LoadResult>(r#"{"loadType":"empty","data":{}}"#).unwrap(),
        LoadResult::Empty(_)
    ));

    let err = r#"{"loadType":"error","data":{"message":"nope","severity":"fault","cause":"boom"}}"#;
    match serde_json::from_str::<LoadResult>(err).unwrap() {
        LoadResult::Error(e) => {
            assert_eq!(e.severity, Severity::Fault);
            assert_eq!(e.message.as_deref(), Some("nope"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn unknown_severity_does_not_fail() {
    let e: Exception =
        serde_json::from_str(r#"{"message":null,"severity":"weird","cause":""}"#).unwrap();
    assert_eq!(e.severity, Severity::Unknown);
}

#[test]
fn update_player_serializes_only_set_fields() {
    let u = UpdatePlayer {
        volume: Some(50),
        ..Default::default()
    };
    assert_eq!(serde_json::to_value(&u).unwrap(), json!({"volume": 50}));

    let stop = UpdatePlayer {
        track: Some(UpdateTrack {
            encoded: Some(None),
            ..Default::default()
        }),
        end_time: Some(None),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&stop).unwrap(),
        json!({"track": {"encoded": null}, "endTime": null})
    );
}

#[test]
fn player_response_parses() {
    let v = json!({"guildId":"1","track":null,"volume":100,"paused":false,
        "state":{"time":1,"position":2,"connected":true,"ping":-1},
        "voice":{"token":"t","endpoint":"e","sessionId":"s","channelId":"9"},
        "filters":{"volume":1.0,"equalizer":[{"band":0,"gain":0.25}],"pluginFilters":{"x":{"a":1}}}});
    let p: Player = serde_json::from_value(v).unwrap();
    assert_eq!(p.guild_id, GuildId(1));
    assert_eq!(p.state.ping, -1);
    assert_eq!(p.voice.channel_id, Some(ChannelId(9)));
    assert_eq!(p.filters.equalizer.unwrap()[0].gain, 0.25);
    assert!(p.filters.plugin_filters.unwrap().contains_key("x"));
}

#[test]
fn player_response_voice_without_channel_id_still_parses() {
    // Lavalink < 4.2 (and players that never joined) report no channelId.
    let v = json!({"guildId":"1","track":null,"volume":100,"paused":false,
        "state":{"time":1,"position":2,"connected":false,"ping":-1},
        "voice":{"token":"","endpoint":"","sessionId":""},"filters":{}});
    let p: Player = serde_json::from_value(v).unwrap();
    assert_eq!(p.voice.channel_id, None);
}

#[test]
fn voice_state_always_sends_channel_id_for_dave() {
    // DAVE (E2EE voice, Lavalink >= 4.2.0) requires `channelId` in every voice state.
    let vs = VoiceState {
        token: "t".into(),
        endpoint: "e".into(),
        session_id: "s".into(),
        channel_id: ChannelId(9),
    };
    assert_eq!(
        serde_json::to_value(&vs).unwrap(),
        json!({"token":"t","endpoint":"e","sessionId":"s","channelId":"9"})
    );
    assert!(
        serde_json::from_value::<VoiceState>(json!({"token":"t","endpoint":"e","sessionId":"s"}))
            .is_err(),
        "a voice state without channelId must not deserialize"
    );
}

#[test]
fn filters_all_variants_roundtrip() {
    let v = json!({"volume":1.0,"equalizer":[{"band":1,"gain":0.5}],
      "karaoke":{"level":1.0,"monoLevel":1.0,"filterBand":220.0,"filterWidth":100.0},
      "timescale":{"speed":1.0,"pitch":1.0,"rate":1.0},
      "tremolo":{"frequency":2.0,"depth":0.5},"vibrato":{"frequency":2.0,"depth":0.5},
      "rotation":{"rotationHz":0.2},
      "distortion":{"sinOffset":0.0,"sinScale":1.0,"cosOffset":0.0,"cosScale":1.0,"tanOffset":0.0,"tanScale":1.0,"offset":0.0,"scale":1.0},
      "channelMix":{"leftToLeft":1.0,"leftToRight":0.0,"rightToLeft":0.0,"rightToRight":1.0},
      "lowPass":{"smoothing":20.0},"pluginFilters":{"p":{}}});
    let f: Filters = serde_json::from_value(v.clone()).unwrap();
    assert_eq!(serde_json::to_value(&f).unwrap(), v);
}

#[test]
fn stats_with_and_without_frame_stats() {
    let base = |fs: &str| {
        format!(
            r#"{{"players":1,"playingPlayers":1,"uptime":5,"memory":{{"free":1,"used":2,"allocated":3,"reservable":4}},"cpu":{{"cores":4,"systemLoad":0.5,"lavalinkLoad":0.1}},"frameStats":{fs}}}"#
        )
    };
    let s: Stats =
        serde_json::from_str(&base(r#"{"sent":6000,"nulled":10,"deficit":-3}"#)).unwrap();
    assert_eq!(s.frame_stats.unwrap().deficit, -3);
    let s: Stats = serde_json::from_str(&base("null")).unwrap();
    assert!(s.frame_stats.is_none());
    let mut v: Value = serde_json::from_str(&base("null")).unwrap();
    v.as_object_mut().unwrap().remove("frameStats");
    assert!(
        serde_json::from_value::<Stats>(v)
            .unwrap()
            .frame_stats
            .is_none()
    );
}

#[test]
fn info_and_routeplanner_parse() {
    let info = json!({"version":{"semver":"4.0.8","major":4,"minor":0,"patch":8,"preRelease":null,"build":null},
      "buildTime":1,"git":{"branch":"main","commit":"abc","commitTime":2},"jvm":"17","lavaplayer":"2.2.2",
      "sourceManagers":["youtube"],"filters":["volume"],"plugins":[{"name":"p","version":"1"}]});
    let i: Info = serde_json::from_value(info).unwrap();
    assert_eq!(i.version.major, 4);
    assert_eq!(i.plugins[0].name, "p");
    let rp: RoutePlannerStatus =
        serde_json::from_value(json!({"class":null,"details":null})).unwrap();
    assert!(rp.class.is_none());
    let rp: RoutePlannerStatus = serde_json::from_value(json!({"class":"RotatingIpRoutePlanner","details":{
      "ipBlock":{"type":"Inet6Address","size":"1"},"failingAddresses":[{"failingAddress":"a","failingTimestamp":1,"failingTime":"t"}],
      "rotateIndex":"0","ipIndex":"0","currentAddress":"a","blockIndex":"0","currentAddressIndex":"0"}})).unwrap();
    assert_eq!(rp.details.unwrap().failing_addresses.len(), 1);
}

const EV_TRACK: &str = TRACK;

#[test]
fn ws_known_messages() {
    match WsMessage::parse(r#"{"op":"ready","resumed":true,"sessionId":"abc"}"#).unwrap() {
        WsMessage::Ready(r) => assert!(r.resumed && r.session_id == "abc"),
        o => panic!("{o:?}"),
    }
    match WsMessage::parse(r#"{"op":"playerUpdate","guildId":"18446744073709551615","state":{"time":1,"position":2,"connected":true,"ping":3}}"#).unwrap() {
        WsMessage::PlayerUpdate(u) => assert_eq!(u.guild_id.0, u64::MAX),
        o => panic!("{o:?}"),
    }
    let end = format!(
        r#"{{"op":"event","type":"TrackEndEvent","guildId":"1","track":{EV_TRACK},"reason":"loadFailed"}}"#
    );
    match WsMessage::parse(&end).unwrap() {
        WsMessage::Event(Event::TrackEnd {
            guild_id, reason, ..
        }) => {
            assert_eq!(guild_id, GuildId(1));
            assert_eq!(reason, TrackEndReason::LoadFailed);
            assert!(reason.may_start_next());
            assert!(!TrackEndReason::Replaced.may_start_next());
        }
        o => panic!("{o:?}"),
    }
    let exc = format!(
        r#"{{"op":"event","type":"TrackExceptionEvent","guildId":"1","track":{EV_TRACK},"exception":{{"message":"boom","severity":"fault","cause":"x"}}}}"#
    );
    assert!(matches!(
        WsMessage::parse(&exc).unwrap(),
        WsMessage::Event(Event::TrackException { .. })
    ));
    let stuck = format!(
        r#"{{"op":"event","type":"TrackStuckEvent","guildId":"1","track":{EV_TRACK},"thresholdMs":10000}}"#
    );
    assert!(matches!(
        WsMessage::parse(&stuck).unwrap(),
        WsMessage::Event(Event::TrackStuck {
            threshold_ms: 10000,
            ..
        })
    ));
    let closed = r#"{"op":"event","type":"WebSocketClosedEvent","guildId":"1","code":4006,"reason":"x","byRemote":true}"#;
    assert!(matches!(
        WsMessage::parse(closed).unwrap(),
        WsMessage::Event(Event::WebSocketClosed {
            code: 4006,
            by_remote: true,
            ..
        })
    ));
    let start =
        format!(r#"{{"op":"event","type":"TrackStartEvent","guildId":"1","track":{EV_TRACK}}}"#);
    assert!(matches!(
        WsMessage::parse(&start).unwrap(),
        WsMessage::Event(Event::TrackStart { .. })
    ));
}

#[test]
fn ws_unknown_and_malformed_never_error_unless_not_json() {
    match WsMessage::parse(r#"{"op":"weird","x":1}"#).unwrap() {
        WsMessage::Unknown { op, payload } => {
            assert_eq!(op, "weird");
            assert_eq!(payload["x"], 1);
        }
        o => panic!("{o:?}"),
    }
    match WsMessage::parse(r#"{"op":"event","type":"NewEvent","guildId":"1"}"#).unwrap() {
        WsMessage::Unknown { op, .. } => assert_eq!(op, "event"),
        o => panic!("{o:?}"),
    }
    // known op, malformed body -> Unknown, not an error
    assert!(matches!(
        WsMessage::parse(r#"{"op":"playerUpdate"}"#).unwrap(),
        WsMessage::Unknown { .. }
    ));
    assert!(matches!(
        WsMessage::parse("[1,2]").unwrap(),
        WsMessage::Unknown { .. }
    ));
    assert!(WsMessage::parse("not json").is_err());
}

#[test]
fn rest_error_body() {
    let e: RestError = serde_json::from_str(r#"{"timestamp":1,"status":404,"error":"Not Found","message":"Session not found","path":"/v4/x"}"#).unwrap();
    assert_eq!(e.status, 404);
    let e: RestError =
        serde_json::from_str(r#"{"status":400,"error":"Bad Request","path":"/v4/x"}"#).unwrap();
    assert_eq!(e.message, "");
}
