use std::{collections::HashSet, sync::Arc};

use serde::Serialize;
use tokio::sync::{Notify, mpsc};

use crate::{broadcasting_software::BroadcastingSoftwareLogic, config};

pub struct State {
    pub config: config::Config,
    pub switcher_state: SwitcherState,
    pub broadcasting_software: BroadcastingSoftwareState,
    pub event_senders: Vec<BroadcastClient>,
}

impl State {
    // also should be done once after loading config or adding stream_servers
    pub fn set_all_switchable_scenes(&mut self) {
        let all_scenes = &mut self.switcher_state.switchable_scenes;

        let scenes = &self.config.switcher.switching_scenes;
        all_scenes.insert(scenes.low.to_owned());
        all_scenes.insert(scenes.normal.to_owned());
        all_scenes.insert(scenes.offline.to_owned());

        for servers in &self.config.switcher.stream_servers {
            if let Some(scenes) = &servers.override_scenes {
                all_scenes.insert(scenes.low.to_owned());
                all_scenes.insert(scenes.normal.to_owned());
                all_scenes.insert(scenes.offline.to_owned());
            }

            if let Some(depends_on) = &servers.depends_on {
                let scenes = &depends_on.backup_scenes;
                all_scenes.insert(scenes.low.to_owned());
                all_scenes.insert(scenes.normal.to_owned());
                all_scenes.insert(scenes.offline.to_owned());
            }
        }

        if let Some(starting_scene) = &self.config.optional_scenes.starting
            && self
                .config
                .optional_options
                .switch_from_starting_scene_to_live_scene
        {
            all_scenes.insert(starting_scene.to_owned());
        }
    }
}

pub struct SwitcherState {
    pub last_used_server: Option<String>,

    /// All switchable scenes
    pub switchable_scenes: HashSet<String>,

    switcher_enabled_notifier: Arc<Notify>,
}

impl SwitcherState {
    pub fn switcher_enabled_notifier(&self) -> Arc<Notify> {
        self.switcher_enabled_notifier.clone()
    }

    pub async fn wait_till_enabled(&self) {
        self.switcher_enabled_notifier().notified().await;
    }
}

impl Default for SwitcherState {
    fn default() -> Self {
        Self {
            last_used_server: None,
            switcher_enabled_notifier: Arc::new(Notify::new()),
            switchable_scenes: HashSet::new(),
        }
    }
}

pub struct BroadcastingSoftwareState {
    pub prev_scene: String,
    pub current_scene: String,
    pub status: ClientStatus,
    pub is_streaming: bool,
    pub last_stream_started_at: std::time::Instant,
    pub initial_stream_status: Option<StreamStatus>,
    pub stream_status: Option<StreamStatus>,

    /// A scene switch request that was accepted by the broadcasting
    /// software but not yet confirmed as the current scene. Used to avoid
    /// sending duplicate switch requests / announcements while a long
    /// transition (e.g. a stinger) is still playing out.
    pub pending_switch: Option<PendingSwitch>,

    // TODO?
    pub connection: Option<Box<dyn BroadcastingSoftwareLogic>>,

    connected_notifier: Arc<Notify>,
    start_streaming_notifier: Arc<Notify>,
    switch_scene_notifier: Arc<Notify>,
}

impl BroadcastingSoftwareState {
    /// How long a requested scene switch is considered "pending" before
    /// it's treated as stale and eligible to be retried. This is the
    /// safety net for missed/late confirmation events (e.g. a dropped
    /// connection), and should comfortably exceed any expected transition
    /// duration.
    pub const PENDING_SWITCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    pub fn connected_notifier(&self) -> Arc<Notify> {
        self.connected_notifier.clone()
    }

    pub fn start_streaming_notifier(&self) -> Arc<Notify> {
        self.start_streaming_notifier.clone()
    }

    pub fn switch_scene_notifier(&self) -> Arc<Notify> {
        self.switch_scene_notifier.clone()
    }

    /// Whether a switch to `scene` was already requested and is still
    /// within the timeout window, i.e. it doesn't need to be requested
    /// again.
    pub fn is_switch_pending(&self, scene: &str) -> bool {
        self.pending_switch.as_ref().is_some_and(|pending| {
            pending.scene == scene && pending.requested_at.elapsed() < Self::PENDING_SWITCH_TIMEOUT
        })
    }

    pub fn set_pending_switch(&mut self, scene: String) {
        self.pending_switch = Some(PendingSwitch {
            scene,
            requested_at: std::time::Instant::now(),
        });
    }

    pub fn clear_pending_switch(&mut self) {
        self.pending_switch = None;
    }

    /// Resets connection-dependent state after OBS disconnects. Any
    /// in-flight switch can no longer be confirmed by an event, so it
    /// must not be left pending until the timeout.
    pub fn mark_disconnected(&mut self) {
        self.status = ClientStatus::Disconnected;
        self.is_streaming = false;
        self.clear_pending_switch();
    }
}

#[derive(Debug, Clone)]
pub struct PendingSwitch {
    pub scene: String,
    pub requested_at: std::time::Instant,
}

impl std::fmt::Debug for BroadcastingSoftwareState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastingSoftwareState")
            .field("prev_scene", &self.prev_scene)
            .field("curent_scene", &self.current_scene)
            .field("status", &self.status)
            .field("is_streaming", &self.is_streaming)
            .field("Does have a software set", &self.connection.is_some())
            .finish()
    }
}

impl Default for BroadcastingSoftwareState {
    fn default() -> Self {
        Self {
            prev_scene: String::new(),
            current_scene: String::new(),
            status: ClientStatus::Disconnected,
            is_streaming: false,
            connection: None,
            connected_notifier: Arc::new(Notify::new()),
            start_streaming_notifier: Arc::new(Notify::new()),
            switch_scene_notifier: Arc::new(Notify::new()),
            last_stream_started_at: std::time::Instant::now(),
            stream_status: None,
            initial_stream_status: None,
            pending_switch: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStatus {
    Connected,
    Disconnected,
}

#[derive(Debug, Default, Clone)]
pub struct StreamStatus {
    pub bitrate: u64,
    pub fps: f64,
    pub num_total_frames: u64,
    pub num_dropped_frames: u64,
    pub render_total_frames: u64,
    pub render_missed_frames: u64,
    pub output_total_frames: u64,
    pub output_skipped_frames: u64,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub available_disk_space: f64,
}

impl StreamStatus {
    pub fn calculate_current(&self, old: &Self) -> Self {
        Self {
            bitrate: self.bitrate,
            fps: self.fps,
            num_total_frames: self.num_total_frames - old.num_total_frames,
            num_dropped_frames: self.num_dropped_frames - old.num_dropped_frames,
            render_total_frames: self.render_total_frames - old.render_total_frames,
            render_missed_frames: self.render_missed_frames - old.render_missed_frames,
            output_total_frames: self.output_total_frames - old.output_total_frames,
            output_skipped_frames: self.output_skipped_frames - old.output_skipped_frames,
            cpu_usage: self.cpu_usage,
            memory_usage: self.memory_usage,
            available_disk_space: self.available_disk_space,
        }
    }
}

#[derive(Debug)]
pub struct BroadcastClient {
    /// Unique token for the current client
    pub token: String,

    /// Channel used for sending to the websocket
    pub tx_chan: mpsc::UnboundedSender<String>,
}

impl BroadcastClient {
    pub fn send<T>(&self, message: T)
    where
        T: Serialize,
    {
        let json = serde_json::to_string(&message).unwrap();

        if self.tx_chan.send(json).is_err() {
            // Disconnected.. should be handled in reader
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn no_pending_switch_by_default() {
        let bs = BroadcastingSoftwareState::default();
        assert!(!bs.is_switch_pending("live"));
    }

    #[test]
    fn switch_is_pending_right_after_being_set() {
        let mut bs = BroadcastingSoftwareState::default();
        bs.set_pending_switch("live".to_string());

        assert!(bs.is_switch_pending("live"));
    }

    #[test]
    fn a_different_scene_is_not_considered_pending() {
        let mut bs = BroadcastingSoftwareState::default();
        bs.set_pending_switch("live".to_string());

        assert!(!bs.is_switch_pending("low"));
    }

    #[test]
    fn pending_switch_expires_after_the_timeout() {
        let mut bs = BroadcastingSoftwareState::default();
        bs.pending_switch = Some(PendingSwitch {
            scene: "live".to_string(),
            requested_at: std::time::Instant::now()
                - (BroadcastingSoftwareState::PENDING_SWITCH_TIMEOUT + Duration::from_secs(1)),
        });

        assert!(!bs.is_switch_pending("live"));
    }

    #[test]
    fn pending_switch_is_still_pending_just_before_the_timeout() {
        let mut bs = BroadcastingSoftwareState::default();
        bs.pending_switch = Some(PendingSwitch {
            scene: "live".to_string(),
            requested_at: std::time::Instant::now()
                - (BroadcastingSoftwareState::PENDING_SWITCH_TIMEOUT - Duration::from_secs(1)),
        });

        assert!(bs.is_switch_pending("live"));
    }

    #[test]
    fn clear_pending_switch_removes_it() {
        let mut bs = BroadcastingSoftwareState::default();
        bs.set_pending_switch("live".to_string());
        bs.clear_pending_switch();

        assert!(!bs.is_switch_pending("live"));
    }

    #[test]
    fn mark_disconnected_clears_pending_switch_and_state() {
        let mut bs = BroadcastingSoftwareState::default();
        bs.status = ClientStatus::Connected;
        bs.is_streaming = true;
        bs.set_pending_switch("live".to_string());

        bs.mark_disconnected();

        assert_eq!(bs.status, ClientStatus::Disconnected);
        assert!(!bs.is_streaming);
        assert!(!bs.is_switch_pending("live"));
    }
}
