use crate::hub::{Hub, Route};
use crate::state::{PlayerInner, PlayerSnapshot, lock};
use crate::voice::VoiceAssembler;
use crate::{
    ChannelId, Error, Event, Filters, GuildId, LavalinkClient, Result, Track, UpdatePlayer,
    UpdateTrack, VoiceOutcome, VoiceServerUpdate, VoiceState, VoiceStateUpdate,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// Cheap, cloneable handle to one guild's player.
#[derive(Clone)]
pub struct Player {
    pub(crate) hub: Arc<Hub>,
    pub(crate) inner: Arc<PlayerInner>,
}

/// Events of a single guild.
pub struct PlayerEvents {
    rx: broadcast::Receiver<Event>,
    guild: GuildId,
}

impl PlayerEvents {
    pub async fn recv(&mut self) -> std::result::Result<Event, RecvError> {
        loop {
            let e = self.rx.recv().await?;
            if e.guild() == Some(self.guild) {
                return Ok(e);
            }
        }
    }
}

impl LavalinkClient {
    /// Returns the player for `guild`, creating local state if needed.
    pub fn player(&self, guild: impl Into<GuildId>) -> Player {
        let guild = guild.into();
        let inner = self
            .hub
            .players
            .entry(guild)
            .or_insert_with(|| Arc::new(PlayerInner::new(guild)))
            .clone();
        Player {
            hub: self.hub.clone(),
            inner,
        }
    }

    pub fn get_player(&self, guild: impl Into<GuildId>) -> Option<Player> {
        let inner = self.hub.players.get(&guild.into())?.clone();
        Some(Player {
            hub: self.hub.clone(),
            inner,
        })
    }
}

impl Player {
    pub fn guild_id(&self) -> GuildId {
        self.inner.guild
    }
    pub fn snapshot(&self) -> Arc<PlayerSnapshot> {
        self.inner.snapshot()
    }
    /// Interpolated position in ms; never awaits.
    pub fn position(&self) -> u64 {
        self.inner.load().position_now()
    }
    pub fn track(&self) -> Option<Arc<Track>> {
        self.inner.load().track.clone()
    }
    pub fn is_paused(&self) -> bool {
        self.inner.load().paused
    }
    pub fn volume(&self) -> u16 {
        self.inner.load().volume
    }
    /// Current filters (shared, not copied).
    pub fn filters(&self) -> Arc<Filters> {
        self.inner.load().filters.clone()
    }
    pub fn node_index(&self) -> Option<usize> {
        self.inner.node_index()
    }

    /// The node's current view of this player (`GET .../players/{guild}`).
    ///
    /// A player that was never sent to a node yields a 404 `Error::Lavalink`.
    pub async fn fetch(&self) -> Result<crate::model::Player> {
        let node = self.hub.node_for(&self.inner)?;
        let sid = node.session_id().ok_or(Error::NoNode)?;
        node.rest().get_player(&sid, self.inner.guild).await
    }

    pub fn events(&self) -> PlayerEvents {
        PlayerEvents {
            rx: self.hub.events.subscribe(),
            guild: self.inner.guild,
        }
    }

    pub async fn update(&self, upd: UpdatePlayer) -> Result<()> {
        self.update_with(upd, false).await
    }

    /// `no_replace = true` keeps the current track if one is playing.
    pub async fn update_with(&self, upd: UpdatePlayer, no_replace: bool) -> Result<()> {
        let _gate = self.inner.gate.lock().await;
        self.write_locked(upd, no_replace).await
    }

    /// Sends `upd` to the player's node. The caller holds the gate.
    ///
    /// A player whose node is down (or that is orphaned) is first moved to a
    /// healthy node. If the target session does not have this player yet
    /// (lost session, move), the restore state is merged into `upd`; a
    /// complete voice state the node does not have yet (e.g. assembled while
    /// no node was ready, or whose PATCH failed) rides along too.
    async fn write_locked(&self, upd: UpdatePlayer, no_replace: bool) -> Result<()> {
        if self.hub.is_closed() {
            return Err(Error::Closed);
        }
        if self.inner.destroyed.load(Ordering::Acquire) {
            return Err(Error::PlayerNotFound);
        }
        let (node, moving) = match self.hub.route(&self.inner)? {
            Route::Stay(n) => (n, false),
            Route::Move(n) => (n, true),
        };
        let generation = node.session_gen();
        let sid = node.session_id().ok_or(Error::NoNode)?;
        let upd = self.inner.prepare(upd, node.index, generation);
        let resp = node
            .rest()
            .update_player(&sid, self.inner.guild, &upd, no_replace)
            .await?;
        self.inner.apply_player(resp);
        self.inner.mark_written(node.index, generation);
        if let Some(vs) = upd.voice {
            // Only after success, so a failed PATCH is retried by the next
            // identical update (or the next write).
            lock(&self.inner.voice).mark_sent(vs);
        }
        if moving {
            self.hub.adopt(&self.inner, &node);
        }
        Ok(())
    }

    pub async fn play(&self, track: &Track) -> Result<()> {
        let user_data = (!track.user_data.is_null()).then(|| track.user_data.clone());
        self.update(UpdatePlayer {
            track: Some(UpdateTrack {
                encoded: Some(Some(track.encoded.clone())),
                user_data,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
    }

    pub async fn stop(&self) -> Result<()> {
        self.update(UpdatePlayer {
            track: Some(UpdateTrack {
                encoded: Some(None),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
    }

    pub async fn pause(&self, paused: bool) -> Result<()> {
        self.update(UpdatePlayer {
            paused: Some(paused),
            ..Default::default()
        })
        .await
    }

    pub async fn seek(&self, position_ms: u64) -> Result<()> {
        self.update(UpdatePlayer {
            position: Some(position_ms),
            ..Default::default()
        })
        .await
    }

    /// Lavalink accepts 0..=1000.
    pub async fn set_volume(&self, volume: u16) -> Result<()> {
        self.update(UpdatePlayer {
            volume: Some(volume.min(1000)),
            ..Default::default()
        })
        .await
    }

    pub async fn set_filters(&self, filters: Filters) -> Result<()> {
        self.update(UpdatePlayer {
            filters: Some(filters),
            ..Default::default()
        })
        .await
    }

    /// Destroys the player on its node and forgets local state. A 404 is not an error.
    ///
    /// Handles to a destroyed player return `Error::PlayerNotFound` on further writes;
    /// call `LavalinkClient::player` again for a fresh one.
    pub async fn destroy(&self) -> Result<()> {
        let _gate = self.inner.gate.lock().await;
        self.destroy_locked().await
    }

    /// `destroy` with the gate already held by the caller.
    ///
    /// The DELETE and the removal of the map entry run in their own task, so
    /// they complete even if this future is dropped (the entry is removed only
    /// after the DELETE, so no new player for the guild can overlap it).
    async fn destroy_locked(&self) -> Result<()> {
        self.inner.destroyed.store(true, Ordering::Release);
        let hub = self.hub.clone();
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            let res = destroy_remote(&hub, &inner).await;
            hub.players
                .remove_if(&inner.guild, |_, v| Arc::ptr_eq(v, &inner));
            res
        });
        // JoinError only when the runtime is shutting down.
        task.await.unwrap_or(Err(Error::Closed))
    }

    /// Joins a voice channel through the configured `VoiceGateway`.
    pub async fn join(&self, channel: ChannelId) -> Result<()> {
        self.gateway()?.join(self.inner.guild, channel).await
    }

    pub async fn leave(&self) -> Result<()> {
        self.gateway()?.leave(self.inner.guild).await
    }

    fn gateway(&self) -> Result<&Arc<dyn crate::VoiceGateway>> {
        self.hub
            .gateway
            .as_ref()
            .ok_or_else(|| Error::Config("no voice gateway configured".into()))
    }
}

/// DELETE the player on its node. If the node cannot be reached now, the
/// player is marked stale there and deleted once the node resumes.
async fn destroy_remote(hub: &Arc<Hub>, p: &PlayerInner) -> Result<()> {
    if hub.is_closed() {
        return Err(Error::Closed);
    }
    let Some(idx) = p.node_index() else {
        return Ok(()); // never sent anywhere
    };
    let Some(node) = hub.nodes.get(idx) else {
        return Ok(());
    };
    let sid = match node.session_id() {
        Some(sid) if node.is_ready() => sid,
        _ => {
            hub.mark_stale(idx, p.guild);
            return Ok(());
        }
    };
    match node.rest().destroy_player(&sid, p.guild).await {
        Err(Error::Lavalink { status: 404, .. }) => Ok(()),
        Err(e @ (Error::Http(_) | Error::HttpClient(_) | Error::Timeout)) => {
            hub.mark_stale(idx, p.guild);
            Err(e)
        }
        r => r,
    }
}

impl Player {
    /// Feeds the voice assembler and sends the result, all under the gate so
    /// concurrent handlers cannot deliver an older state last.
    pub(crate) async fn voice_locked(
        &self,
        assemble: impl FnOnce(&mut VoiceAssembler) -> VoiceOutcome,
    ) -> Result<()> {
        let _gate = self.inner.gate.lock().await;
        let outcome = assemble(&mut lock(&self.inner.voice));
        match outcome {
            VoiceOutcome::Pending => Ok(()),
            VoiceOutcome::Ready(vs) => {
                self.write_locked(
                    UpdatePlayer {
                        voice: Some(vs),
                        ..Default::default()
                    },
                    false,
                )
                .await
            }
            VoiceOutcome::Left => self.destroy_locked().await,
        }
    }
}

impl LavalinkClient {
    /// Feed `VOICE_STATE_UPDATE`. `channel_id: None` destroys the player.
    ///
    /// Callers must only pass voice events for the bot's own user.
    pub async fn voice_state_update(
        &self,
        guild: impl Into<GuildId>,
        upd: VoiceStateUpdate,
    ) -> Result<()> {
        self.player(guild)
            .voice_locked(|v| v.update_state(upd))
            .await
    }

    /// Feed `VOICE_SERVER_UPDATE`.
    ///
    /// Callers must only pass voice events for the bot's own user.
    pub async fn voice_server_update(
        &self,
        guild: impl Into<GuildId>,
        upd: VoiceServerUpdate,
    ) -> Result<()> {
        self.player(guild)
            .voice_locked(|v| v.update_server(upd))
            .await
    }

    /// Hand over a complete voice connection (e.g. from songbird).
    ///
    /// Callers must only pass voice events for the bot's own user.
    pub async fn voice_update(&self, guild: impl Into<GuildId>, vs: VoiceState) -> Result<()> {
        // Always sent (not deduplicated): the caller hands over a new connection.
        self.player(guild)
            .voice_locked(|v| {
                v.set(&vs);
                VoiceOutcome::Ready(vs)
            })
            .await
    }
}
