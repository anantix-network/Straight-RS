use larplink_model::*;
use serde_json::{json, Value};

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
    assert!(matches!(serde_json::from_str::<LoadResult>(&track).unwrap(), LoadResult::Track(_)));

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
    let e: Exception = serde_json::from_str(r#"{"message":null,"severity":"weird","cause":""}"#).unwrap();
    assert_eq!(e.severity, Severity::Unknown);
}
