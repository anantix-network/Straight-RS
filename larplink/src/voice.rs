use larplink_model::{ChannelId, VoiceState};

#[derive(Clone, Debug)]
pub struct VoiceStateUpdate {
    /// `None` = the bot left the channel.
    pub channel_id: Option<ChannelId>,
    pub session_id: String,
}

#[derive(Clone, Debug)]
pub struct VoiceServerUpdate {
    pub token: String,
    /// `None` = Discord's voice server is unavailable.
    pub endpoint: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum VoiceOutcome {
    Pending,
    Ready(VoiceState),
    Left,
}

#[derive(Default)]
pub(crate) struct VoiceAssembler {
    session_id: Option<String>,
    channel_id: Option<ChannelId>,
    token: Option<String>,
    endpoint: Option<String>,
    sent: Option<VoiceState>,
}

impl VoiceAssembler {
    pub(crate) fn update_state(&mut self, u: VoiceStateUpdate) -> VoiceOutcome {
        match u.channel_id {
            None => {
                *self = Self::default();
                VoiceOutcome::Left
            }
            Some(c) => {
                self.channel_id = Some(c);
                self.session_id = Some(u.session_id);
                self.evaluate()
            }
        }
    }

    pub(crate) fn update_server(&mut self, u: VoiceServerUpdate) -> VoiceOutcome {
        match u.endpoint {
            None => {
                self.token = None;
                self.endpoint = None;
                VoiceOutcome::Pending
            }
            Some(e) => {
                self.token = Some(u.token);
                self.endpoint = Some(e);
                self.evaluate()
            }
        }
    }

    fn evaluate(&self) -> VoiceOutcome {
        let (Some(session_id), Some(token), Some(endpoint)) =
            (&self.session_id, &self.token, &self.endpoint)
        else {
            return VoiceOutcome::Pending;
        };
        let vs = VoiceState {
            token: token.clone(),
            endpoint: endpoint.clone(),
            session_id: session_id.clone(),
            channel_id: self.channel_id,
        };
        if self.sent.as_ref() == Some(&vs) {
            VoiceOutcome::Pending
        } else {
            VoiceOutcome::Ready(vs)
        }
    }

    /// Record that `vs` reached a node, so identical updates are not re-sent.
    pub(crate) fn mark_sent(&mut self, vs: VoiceState) {
        self.sent = Some(vs);
    }

    /// Last voice state successfully delivered to a node.
    pub(crate) fn current(&self) -> Option<VoiceState> {
        self.sent.clone()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    fn st(ch: Option<u64>, s: &str) -> VoiceStateUpdate { VoiceStateUpdate { channel_id: ch.map(ChannelId), session_id: s.into() } }
    fn sv(t: &str, e: Option<&str>) -> VoiceServerUpdate { VoiceServerUpdate { token: t.into(), endpoint: e.map(Into::into) } }
    fn expect_ready(o: VoiceOutcome) -> VoiceState { match o { VoiceOutcome::Ready(v) => v, o => panic!("{o:?}") } }

    #[test] fn state_then_server() {
        let mut a = VoiceAssembler::default();
        assert!(matches!(a.update_state(st(Some(9), "sess")), VoiceOutcome::Pending));
        let v = expect_ready(a.update_server(sv("tok", Some("e.discord.media:443"))));
        assert_eq!((v.token.as_str(), v.endpoint.as_str(), v.session_id.as_str(), v.channel_id), ("tok", "e.discord.media:443", "sess", Some(ChannelId(9))));
    }
    #[test] fn server_then_state() {
        let mut a = VoiceAssembler::default();
        assert!(matches!(a.update_server(sv("tok", Some("e"))), VoiceOutcome::Pending));
        let v = expect_ready(a.update_state(st(Some(9), "sess")));
        assert_eq!(v.session_id, "sess");
    }
    #[test] fn null_endpoint_clears_and_waits() {
        let mut a = VoiceAssembler::default();
        a.update_state(st(Some(9), "s"));
        expect_ready(a.update_server(sv("t", Some("e"))));
        assert!(matches!(a.update_server(sv("t2", None)), VoiceOutcome::Pending));
        // state update alone must not re-emit stale token/endpoint
        assert!(matches!(a.update_state(st(Some(9), "s2")), VoiceOutcome::Pending));
        expect_ready(a.update_server(sv("t3", Some("e2"))));
    }
    #[test] fn leaving_resets_everything() {
        let mut a = VoiceAssembler::default();
        a.update_state(st(Some(9), "s"));
        a.update_server(sv("t", Some("e")));
        assert!(matches!(a.update_state(st(None, "s")), VoiceOutcome::Left));
        assert!(matches!(a.update_server(sv("t", Some("e"))), VoiceOutcome::Pending));
        assert!(a.current().is_none());
    }
    #[test] fn duplicates_are_suppressed_after_mark_sent_but_changes_are_not() {
        let mut a = VoiceAssembler::default();
        a.update_state(st(Some(9), "s"));
        let v = expect_ready(a.update_server(sv("t", Some("e"))));
        // not marked sent yet -> would emit again (a failed PATCH can be retried)
        expect_ready(a.update_server(sv("t", Some("e"))));
        a.mark_sent(v.clone());
        assert_eq!(a.current(), Some(v));
        assert!(matches!(a.update_server(sv("t", Some("e"))), VoiceOutcome::Pending));
        expect_ready(a.update_server(sv("t", Some("new-region"))));
    }
}
