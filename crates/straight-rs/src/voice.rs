use straight_rs_model::{ChannelId, VoiceState};

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

    /// A complete connection handed over as a whole (e.g. from songbird).
    pub(crate) fn set(&mut self, vs: &VoiceState) {
        self.session_id = Some(vs.session_id.clone());
        self.channel_id = vs.channel_id;
        self.token = Some(vs.token.clone());
        self.endpoint = Some(vs.endpoint.clone());
    }

    fn evaluate(&self) -> VoiceOutcome {
        match self.pending() {
            Some(vs) => VoiceOutcome::Ready(vs),
            None => VoiceOutcome::Pending,
        }
    }

    /// Record that `vs` reached a node, so identical updates are not re-sent.
    pub(crate) fn mark_sent(&mut self, vs: VoiceState) {
        self.sent = Some(vs);
    }

    /// The latest complete voice state, whether or not a node has it yet.
    pub(crate) fn latest(&self) -> Option<VoiceState> {
        let (Some(session_id), Some(token), Some(endpoint)) =
            (&self.session_id, &self.token, &self.endpoint)
        else {
            return None;
        };
        Some(VoiceState {
            token: token.clone(),
            endpoint: endpoint.clone(),
            session_id: session_id.clone(),
            channel_id: self.channel_id,
        })
    }

    /// The latest complete voice state if it differs from what was last sent.
    pub(crate) fn pending(&self) -> Option<VoiceState> {
        self.latest().filter(|vs| self.sent.as_ref() != Some(vs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn st(ch: Option<u64>, s: &str) -> VoiceStateUpdate {
        VoiceStateUpdate {
            channel_id: ch.map(ChannelId),
            session_id: s.into(),
        }
    }
    fn sv(t: &str, e: Option<&str>) -> VoiceServerUpdate {
        VoiceServerUpdate {
            token: t.into(),
            endpoint: e.map(Into::into),
        }
    }
    fn expect_ready(o: VoiceOutcome) -> VoiceState {
        match o {
            VoiceOutcome::Ready(v) => v,
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn state_then_server() {
        let mut a = VoiceAssembler::default();
        assert!(matches!(
            a.update_state(st(Some(9), "sess")),
            VoiceOutcome::Pending
        ));
        let v = expect_ready(a.update_server(sv("tok", Some("e.discord.media:443"))));
        assert_eq!(
            (
                v.token.as_str(),
                v.endpoint.as_str(),
                v.session_id.as_str(),
                v.channel_id
            ),
            ("tok", "e.discord.media:443", "sess", Some(ChannelId(9)))
        );
    }
    #[test]
    fn server_then_state() {
        let mut a = VoiceAssembler::default();
        assert!(matches!(
            a.update_server(sv("tok", Some("e"))),
            VoiceOutcome::Pending
        ));
        let v = expect_ready(a.update_state(st(Some(9), "sess")));
        assert_eq!(v.session_id, "sess");
    }
    #[test]
    fn null_endpoint_clears_and_waits() {
        let mut a = VoiceAssembler::default();
        a.update_state(st(Some(9), "s"));
        expect_ready(a.update_server(sv("t", Some("e"))));
        assert!(matches!(
            a.update_server(sv("t2", None)),
            VoiceOutcome::Pending
        ));
        // state update alone must not re-emit stale token/endpoint
        assert!(matches!(
            a.update_state(st(Some(9), "s2")),
            VoiceOutcome::Pending
        ));
        expect_ready(a.update_server(sv("t3", Some("e2"))));
    }
    #[test]
    fn leaving_resets_everything() {
        let mut a = VoiceAssembler::default();
        a.update_state(st(Some(9), "s"));
        a.update_server(sv("t", Some("e")));
        assert!(matches!(a.update_state(st(None, "s")), VoiceOutcome::Left));
        assert!(matches!(
            a.update_server(sv("t", Some("e"))),
            VoiceOutcome::Pending
        ));
        assert!(a.latest().is_none());
        assert!(a.pending().is_none());
    }
    #[test]
    fn duplicates_are_suppressed_after_mark_sent_but_changes_are_not() {
        let mut a = VoiceAssembler::default();
        a.update_state(st(Some(9), "s"));
        let v = expect_ready(a.update_server(sv("t", Some("e"))));
        // not marked sent yet -> would emit again (a failed PATCH can be retried)
        expect_ready(a.update_server(sv("t", Some("e"))));
        assert_eq!(a.pending(), Some(v.clone()));
        a.mark_sent(v.clone());
        assert_eq!(a.latest(), Some(v));
        assert!(a.pending().is_none());
        assert!(matches!(
            a.update_server(sv("t", Some("e"))),
            VoiceOutcome::Pending
        ));
        expect_ready(a.update_server(sv("t", Some("new-region"))));
    }
    #[test]
    fn latest_reflects_unsent_changes() {
        let mut a = VoiceAssembler::default();
        a.update_state(st(Some(9), "s"));
        let v = expect_ready(a.update_server(sv("t", Some("e"))));
        a.mark_sent(v);
        // Region change whose PATCH failed: latest must be the new endpoint.
        a.update_server(sv("t", Some("new-region")));
        assert_eq!(a.latest().unwrap().endpoint, "new-region");
        assert_eq!(a.pending().unwrap().endpoint, "new-region");
    }
    #[test]
    fn set_takes_a_complete_state() {
        let mut a = VoiceAssembler::default();
        let vs = VoiceState {
            token: "t".into(),
            endpoint: "e".into(),
            session_id: "s".into(),
            channel_id: Some(ChannelId(3)),
        };
        a.set(&vs);
        assert_eq!(a.latest(), Some(vs));
    }
}
