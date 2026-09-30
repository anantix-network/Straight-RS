use crate::{ChannelId, GuildId, VoiceState};
use ::songbird::ConnectionInfo;

/// A complete connection; pass it to `LavalinkClient::voice_update`.
pub fn connection_info(info: &ConnectionInfo) -> (GuildId, VoiceState) {
    (
        GuildId(info.guild_id.0.get()),
        VoiceState {
            token: info.token.clone(),
            endpoint: info.endpoint.clone(),
            session_id: info.session_id.clone(),
            channel_id: Some(ChannelId(info.channel_id.0.get())),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maps_connection_info() {
        use ::songbird::id::{ChannelId as SbChannel, GuildId as SbGuild, UserId as SbUser};
        use std::num::NonZeroU64;
        let n = |v| NonZeroU64::new(v).unwrap();
        let info = ::songbird::ConnectionInfo {
            channel_id: SbChannel::from(n(9)),
            endpoint: "e:443".into(),
            guild_id: SbGuild::from(n(100)),
            session_id: "sess".into(),
            token: "tok".into(),
            user_id: SbUser::from(n(1)),
        };
        let (g, vs) = connection_info(&info);
        assert_eq!(g, GuildId(100));
        assert_eq!(
            (
                vs.token.as_str(),
                vs.endpoint.as_str(),
                vs.session_id.as_str(),
                vs.channel_id
            ),
            ("tok", "e:443", "sess", Some(ChannelId(9)))
        );
    }
}
