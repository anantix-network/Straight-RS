use crate::{ChannelId, GuildId, UserId, VoiceServerUpdate, VoiceStateUpdate};
use ::serenity::model::event::VoiceServerUpdateEvent;
use ::serenity::model::voice::VoiceState as SerVoiceState;

/// `None` for other users' voice states and for events without a guild.
pub fn voice_state(vs: &SerVoiceState, bot: UserId) -> Option<(GuildId, VoiceStateUpdate)> {
    if vs.user_id.get() != bot.0 {
        return None;
    }
    let guild = GuildId(vs.guild_id?.get());
    Some((
        guild,
        VoiceStateUpdate {
            channel_id: vs.channel_id.map(|c| ChannelId(c.get())),
            session_id: vs.session_id.clone(),
        },
    ))
}

/// `None` when the event carries no guild.
pub fn voice_server(ev: &VoiceServerUpdateEvent) -> Option<(GuildId, VoiceServerUpdate)> {
    let guild = GuildId(ev.guild_id?.get());
    Some((
        guild,
        VoiceServerUpdate { token: ev.token.clone(), endpoint: ev.endpoint.clone() },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn vs(user: u64, channel: Option<u64>) -> SerVoiceState {
        serde_json::from_value(serde_json::json!({
            "channel_id": channel.map(|c| c.to_string()), "deaf": false, "guild_id": "100", "mute": false,
            "self_deaf": false, "self_mute": false, "self_video": false, "session_id": "sess",
            "suppress": false, "user_id": user.to_string(), "request_to_speak_timestamp": null
        })).unwrap()
    }
    #[test] fn maps_own_voice_state_and_ignores_others() {
        let (g, u) = voice_state(&vs(1, Some(9)), UserId(1)).unwrap();
        assert_eq!((g, u.channel_id, u.session_id.as_str()), (GuildId(100), Some(ChannelId(9)), "sess"));
        assert!(voice_state(&vs(2, Some(9)), UserId(1)).is_none());
        assert_eq!(voice_state(&vs(1, None), UserId(1)).unwrap().1.channel_id, None);
    }
    #[test] fn maps_voice_server() {
        let ev: VoiceServerUpdateEvent = serde_json::from_value(serde_json::json!({"token":"tok","guild_id":"100","endpoint":"e:443"})).unwrap();
        let (g, u) = voice_server(&ev).unwrap();
        assert_eq!((g, u.token.as_str(), u.endpoint.as_deref()), (GuildId(100), "tok", Some("e:443")));
        let none: VoiceServerUpdateEvent = serde_json::from_value(serde_json::json!({"token":"tok","guild_id":null,"endpoint":null})).unwrap();
        assert!(voice_server(&none).is_none());
    }
}
