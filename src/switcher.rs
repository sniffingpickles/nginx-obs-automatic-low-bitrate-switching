use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{sync::Notify, time::Instant};
use tracing::{Instrument, debug, error, info};

use crate::{
    chat, error,
    noalbs::{self, ChatSender},
    state::ClientStatus,
    stream_servers::{self, websocket},
};

pub struct Switcher {
    pub state: noalbs::UserState,
    pub chat_sender: ChatSender,
}

impl Switcher {
    pub fn run(switcher: Self) -> tokio::task::JoinHandle<()> {
        tracing::info!("Running switcher");

        let f = async move {
            let mut prev_switch_type: SwitchType = SwitchType::Offline;
            let mut same_type: u8 = 0;
            let mut same_type_seconds = Instant::now();
            let stats_update_notifier = websocket::stats_update_notifier();

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
                    _ = stats_update_notifier.notified() => {}
                }
                tracing::debug!("Switcher loop");

                if let Some(notifier) = switcher.get_sleep_notifier_if_necessary().await {
                    notifier.notified().await;
                    same_type_seconds = Instant::now();
                    info!("Switcher running");
                    continue;
                }

                if let Err(e) = switcher
                    .switch(
                        &mut prev_switch_type,
                        &mut same_type,
                        &mut same_type_seconds,
                    )
                    .await
                {
                    error!("Error when trying to switch: {}", e);
                }
            }
        }
        .instrument(tracing::info_span!("Switcher"));

        tokio::spawn(f)
    }

    pub async fn get_sleep_notifier_if_necessary(&self) -> Option<Arc<Notify>> {
        let state = self.state.read().await;

        if !state.config.switcher.bitrate_switcher_enabled {
            info!("Switcher disabled waiting till enabled");
            return Some(state.switcher_state.switcher_enabled_notifier());
        }

        if state.broadcasting_software.status == ClientStatus::Disconnected {
            info!("Waiting for OBS connection");
            return Some(state.broadcasting_software.connected_notifier());
        }

        // TODO: When changing only_switch_when_streaming also do a
        // notify so that it won't wait anymore
        if state.config.switcher.only_switch_when_streaming
            && !state.broadcasting_software.is_streaming
        {
            info!("Waiting till OBS starts streaming");
            return Some(state.broadcasting_software.start_streaming_notifier());
        }

        if !state
            .switcher_state
            .switchable_scenes
            .contains(&state.broadcasting_software.current_scene)
        {
            info!("Not able to switch, waiting for scene switch to a switchable scene");
            return Some(state.broadcasting_software.switch_scene_notifier());
        }

        None
    }

    async fn switch(
        &self,
        prev_switch_type: &mut SwitchType,
        same_type: &mut u8,
        same_type_seconds: &mut Instant,
    ) -> Result<(), error::Error> {
        let state = self.state.read().await;

        let switcher_config = &state.config.switcher;
        let triggers = &switcher_config.triggers;
        let stream_servers = &switcher_config.stream_servers;
        let retry_attempts = &switcher_config.retry_attempts;
        let instant_recover = switcher_config.instantly_switch_on_recover;

        let (mut server, mut current_switch_type) =
            Self::get_online_stream_server(stream_servers, triggers).await;

        let instant_recover = instant_recover
            && *prev_switch_type == SwitchType::Offline
            && current_switch_type != SwitchType::Offline;
        let instant_degrade = Self::should_instant_degrade(
            server,
            stream_servers,
            *prev_switch_type,
            current_switch_type,
        );
        let delay_normal_recovery =
            Self::should_delay_normal_recovery(server, *prev_switch_type, current_switch_type);
        let mut force_switch = (instant_recover || instant_degrade) && !delay_normal_recovery;

        if prev_switch_type == &current_switch_type {
            *same_type += 1;
        } else {
            debug!("Got different type, switching to that");

            *prev_switch_type = current_switch_type;
            *same_type = 0;
            *same_type_seconds = Instant::now();
        }

        debug!("type: {:?}, same: {:?}", current_switch_type, same_type);

        if let SwitchType::Previous = &current_switch_type
            && let Some(s) = server
            && let Some(last) = &state.switcher_state.last_used_server
            && last != &s.name
        {
            current_switch_type = SwitchType::Normal;
            force_switch = true;
        }

        let retry_satisfied = if delay_normal_recovery {
            same_type_seconds.elapsed() >= Duration::from_secs((*retry_attempts).into())
        } else {
            same_type == retry_attempts
        };

        if !(retry_satisfied || force_switch) {
            return Ok(());
        }

        // Avoid triggering the offline timeout when starting the stream.
        if !state.config.switcher.only_switch_when_streaming
            && state.broadcasting_software.last_stream_started_at.elapsed()
                <= Duration::from_secs((*retry_attempts + 5).into())
        {
            *same_type_seconds = Instant::now();
        }

        *same_type = 0;

        if current_switch_type == SwitchType::Offline {
            // TODO: Refactor the timeout code
            if let Some(min) = &state.config.optional_options.offline_timeout
                && state.broadcasting_software.is_streaming
                && same_type_seconds.elapsed() >= Duration::from_secs((min * 60).into())
            {
                info!("Offline timeout reached, stopping the stream");

                let bsc = state
                    .broadcasting_software
                    .connection
                    .as_ref()
                    .ok_or(error::Error::NoSoftwareSet)?;

                if let Err(error) = bsc.stop_streaming().await {
                    error!("Offline timeout error {:?}", error);
                    return Ok(());
                }

                if state.config.optional_options.record_while_streaming
                    && bsc.is_recording().await?
                    && let Err(error) = bsc.toggle_recording().await
                {
                    error!("Offline timeout error {:?}", error);
                    return Ok(());
                }

                if let Some(chat) = &state.config.chat {
                    let message =
                        chat::HandleMessage::InternalChatUpdate(chat::InternalChatUpdate {
                            platform: chat.platform.kind(),
                            channel: chat.username.to_owned(),
                            kind: chat::InternalUpdate::OfflineTimeout,
                        });

                    let _ = self.chat_sender.send(message).await;
                }
            }

            if let Some(name) = &state.switcher_state.last_used_server {
                server = stream_servers.iter().find(|s| &s.name == name);
            }
        }

        let scenes = if let Some(scenes) = get_optional_scenes(server, &state).await {
            scenes
        } else {
            &switcher_config.switching_scenes
        };

        let scene = if let SwitchType::Previous = &current_switch_type {
            &state.broadcasting_software.prev_scene
        } else {
            // Should be safe since previous is handled
            scenes.type_to_scene(&current_switch_type).unwrap()
        }
        .to_owned();

        let server_name = server.map(|s| s.name.to_owned());

        drop(state);

        {
            let mut state = self.state.write().await;

            // Set the previous scene when switch_type is normal or low
            if let SwitchType::Normal | SwitchType::Low = current_switch_type {
                scene.clone_into(&mut state.broadcasting_software.prev_scene);
            };

            if current_switch_type != SwitchType::Offline {
                debug!("Last used server set to {:?}", server_name);
                state.switcher_state.last_used_server = server_name;
            }
        }

        self.switch_if_necessary(&scene, current_switch_type)
            .await?;

        Ok(())
    }

    /// Gets the first online stream server with current status
    async fn get_online_stream_server<'a>(
        stream_servers: &'a [stream_servers::StreamServer],
        triggers: &'a Triggers,
    ) -> (Option<&'a stream_servers::StreamServer>, SwitchType) {
        for server in stream_servers {
            if !server.enabled {
                continue;
            }

            let switch_type = server.stream_server.switch(triggers).await;

            if switch_type == SwitchType::Offline {
                continue;
            }

            return (Some(server), switch_type);
        }

        (None, SwitchType::Offline)
    }

    fn should_instant_degrade(
        server: Option<&stream_servers::StreamServer>,
        stream_servers: &[stream_servers::StreamServer],
        previous: SwitchType,
        current: SwitchType,
    ) -> bool {
        if !matches!(previous, SwitchType::Normal | SwitchType::Low)
            || !matches!(current, SwitchType::Low | SwitchType::Offline)
            || previous == current
        {
            return false;
        }

        server.is_some_and(|server| server.stream_server.instant_degrade())
            || (current == SwitchType::Offline
                && stream_servers
                    .iter()
                    .any(|server| server.enabled && server.stream_server.instant_degrade()))
    }

    fn should_delay_normal_recovery(
        server: Option<&stream_servers::StreamServer>,
        previous: SwitchType,
        current: SwitchType,
    ) -> bool {
        current == SwitchType::Normal
            && previous != SwitchType::Normal
            && server.is_some_and(|server| server.stream_server.delay_normal_recovery())
    }

    pub async fn switch_if_necessary(
        &self,
        switch_scene: &str,
        switch_type: SwitchType,
    ) -> Result<(), error::Error> {
        debug!(
            "Switch scene: {} Switch type: {:?}",
            switch_scene, switch_type
        );

        let state = self.state.read().await;
        let current_scene = &state.broadcasting_software.current_scene;

        if current_scene == switch_scene {
            return Ok(());
        }

        // A switch to this scene has already been requested and OBS
        // hasn't confirmed the change yet (e.g. while a long stinger
        // transition is still playing out). Avoid sending a duplicate
        // request and a duplicate chat announcement; the pending switch
        // is cleared once `CurrentProgramSceneChanged` is received, or
        // after `PENDING_SWITCH_TIMEOUT` if that confirmation never
        // arrives.
        if state.broadcasting_software.is_switch_pending(switch_scene) {
            return Ok(());
        }

        let skip = state
            .config
            .optional_scenes
            .starting
            .as_ref()
            .is_some_and(|starting_scene| {
                let switch_to_live = state
                    .config
                    .optional_options
                    .switch_from_starting_scene_to_live_scene;
                current_scene == starting_scene
                    && switch_to_live
                    && (switch_type == SwitchType::Offline)
            });

        if skip
            || !state
                .switcher_state
                .switchable_scenes
                .contains(&state.broadcasting_software.current_scene)
        {
            return Ok(());
        }

        // Ignore the error.. it should work at some point
        if let Err(error) = state
            .broadcasting_software
            .connection
            .as_ref()
            .ok_or(error::Error::NoSoftwareSet)?
            .switch_scene(switch_scene)
            .await
        {
            error!("Switch scene error {:?}", error);
            return Ok(());
        }

        info!("Scene switched to [{:?}] {}", switch_type, switch_scene);

        let should_announce = state.broadcasting_software.is_streaming
            && state.config.switcher.auto_switch_notification;
        let chat_info = state
            .config
            .chat
            .as_ref()
            .map(|chat| (chat.platform.kind(), chat.username.to_owned()));

        drop(state);

        // The request was accepted by OBS; treat it as pending (not yet
        // completed) until confirmed by `CurrentProgramSceneChanged`.
        self.state
            .write()
            .await
            .broadcasting_software
            .set_pending_switch(switch_scene.to_owned());

        if should_announce && let Some((platform, channel)) = chat_info {
            let message =
                chat::HandleMessage::AutomaticSwitchingScene(chat::AutomaticSwitchingScene {
                    platform,
                    channel,
                    scene: switch_scene.to_owned(),
                    switch_type,
                });

            let _ = self.chat_sender.send(message).await;
        }

        Ok(())
    }
}

async fn get_optional_scenes<'a>(
    server: Option<&'a stream_servers::StreamServer>,
    state: &tokio::sync::RwLockReadGuard<'_, crate::state::State>,
) -> Option<&'a SwitchingScenes> {
    if let Some(depends) = &server?.depends_on
        && !is_stream_server_online(&depends.name, state).await
    {
        debug!("The depended stream server is offline. Going to use the backup scenes.");
        return Some(&depends.backup_scenes);
    }

    server?.override_scenes.as_ref()
}

async fn is_stream_server_online(
    server_name: &str,
    state: &tokio::sync::RwLockReadGuard<'_, crate::state::State>,
) -> bool {
    match state
        .config
        .switcher
        .stream_servers
        .iter()
        .find(|&x| x.name == server_name)
    {
        Some(server) => server.stream_server.bitrate().await.message.is_some(),
        None => false,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwitchingScenes {
    pub normal: String,
    pub low: String,
    pub offline: String,
}

impl SwitchingScenes {
    pub fn new<N, L, O>(normal: N, low: L, offline: O) -> Self
    where
        N: Into<String>,
        L: Into<String>,
        O: Into<String>,
    {
        SwitchingScenes {
            normal: normal.into(),
            low: low.into(),
            offline: offline.into(),
        }
    }

    pub fn type_to_scene(&self, s_type: &SwitchType) -> Result<&str, error::Error> {
        Ok(match s_type {
            SwitchType::Normal => &self.normal,
            SwitchType::Low => &self.low,
            SwitchType::Offline => &self.offline,
            _ => return Err(error::Error::SwitchTypeNotSupported),
        })
    }
}

#[derive(Debug)]
pub enum TriggerType {
    Low,
    Rtt,
    Offline,
    RttOffline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Triggers {
    /// Trigger to switch to the low scene
    pub low: Option<u32>,

    /// Trigger to switch to the low scene when RTT is high
    pub rtt: Option<u32>,

    /// Trigger to switch to the offline scene
    pub offline: Option<u32>,

    /// Trigger to switch to the offline scene when RTT is high
    pub rtt_offline: Option<u32>,
}

impl Triggers {
    pub fn set_low(&mut self, value: Option<u32>) {
        self.low = value;
    }
}

impl Default for Triggers {
    fn default() -> Self {
        Self {
            low: Some(800),
            rtt: Some(2500),
            offline: None,
            rtt_offline: None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SwitchType {
    Normal,
    Low,
    Previous,
    Offline,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{broadcasting_software::BroadcastingSoftwareLogic, config, state};
    use std::{
        collections::HashSet,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };
    use tokio::sync::mpsc;

    /// A fake broadcasting software connection that records every scene it
    /// was asked to switch to. Cloning shares the same recorded state, so a
    /// handle can be kept in the test after the original is moved into
    /// `BroadcastingSoftwareState::connection`.
    #[derive(Clone)]
    struct FakeObs {
        calls: Arc<Mutex<Vec<String>>>,
        fail_next: Arc<AtomicBool>,
    }

    impl FakeObs {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                fail_next: Arc::new(AtomicBool::new(false)),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        /// Makes the next `switch_scene` call fail, simulating a genuine
        /// OBS-side failure (as opposed to a duplicate we should suppress).
        fn fail_next_call(&self) {
            self.fail_next.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl BroadcastingSoftwareLogic for FakeObs {
        async fn switch_scene(&self, scene: &str) -> Result<String, error::Error> {
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return Err(error::Error::NoSourceFound);
            }

            self.calls.lock().unwrap().push(scene.to_string());
            Ok(scene.to_string())
        }

        async fn start_streaming(&self) -> Result<(), error::Error> {
            Ok(())
        }

        async fn stop_streaming(&self) -> Result<(), error::Error> {
            Ok(())
        }

        async fn toggle_recording(&self) -> Result<(), error::Error> {
            Ok(())
        }

        async fn is_recording(&self) -> Result<bool, error::Error> {
            Ok(false)
        }

        async fn fix(&self) -> Result<(), error::Error> {
            Ok(())
        }

        async fn current_scene(&self) -> Result<String, error::Error> {
            Ok(String::new())
        }

        async fn toggle_source(&self, _source: &str) -> Result<(String, bool), error::Error> {
            Ok((String::new(), false))
        }

        async fn set_collection_and_profile(
            &self,
            _source: &config::CollectionPair,
        ) -> Result<(), error::Error> {
            Ok(())
        }

        async fn info(
            &self,
            _state: &tokio::sync::RwLockReadGuard<state::State>,
        ) -> Result<state::StreamStatus, error::Error> {
            Ok(state::StreamStatus::default())
        }
    }

    /// Builds a `Switcher` wired up to `fake` with `current_scene` already
    /// set, plus a receiver to observe automatic-switch chat announcements.
    fn build_switcher(
        fake: FakeObs,
        current_scene: &str,
    ) -> (Switcher, mpsc::Receiver<chat::HandleMessage>) {
        let config = config::Config {
            user: config::User {
                id: None,
                name: "test".to_string(),
                password_hash: None,
            },
            switcher: config::Switcher {
                auto_switch_notification: true,
                ..Default::default()
            },
            software: config::SoftwareConnection::Obs(config::ObsConfig {
                host: "localhost".to_string(),
                password: None,
                port: 4455,
                collections: None,
            }),
            chat: Some(config::Chat::default()),
            optional_scenes: config::OptionalScenes::default(),
            optional_options: config::OptionalOptions::default(),
            log_to_file: true,
        };

        let mut switcher_state = state::SwitcherState::default();
        switcher_state.switchable_scenes =
            HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()]);

        let mut broadcasting_software = state::BroadcastingSoftwareState::default();
        broadcasting_software.current_scene = current_scene.to_string();
        broadcasting_software.is_streaming = true;
        broadcasting_software.status = state::ClientStatus::Connected;
        broadcasting_software.connection = Some(Box::new(fake));

        let state = Arc::new(tokio::sync::RwLock::new(state::State {
            config,
            switcher_state,
            broadcasting_software,
            event_senders: Vec::new(),
        }));

        let (chat_tx, chat_rx) = mpsc::channel(10);

        (
            Switcher {
                state,
                chat_sender: chat_tx,
            },
            chat_rx,
        )
    }

    #[tokio::test]
    async fn immediate_switch_sends_request_and_announces_once() {
        let fake = FakeObs::new();
        let (switcher, mut chat_rx) = build_switcher(fake.clone(), "a");

        switcher
            .switch_if_necessary("b", SwitchType::Normal)
            .await
            .unwrap();

        assert_eq!(fake.calls(), vec!["b".to_string()]);
        assert!(chat_rx.try_recv().is_ok(), "expected one announcement");
    }

    #[tokio::test]
    async fn already_on_target_scene_is_a_no_op() {
        let fake = FakeObs::new();
        let (switcher, mut chat_rx) = build_switcher(fake.clone(), "b");

        switcher
            .switch_if_necessary("b", SwitchType::Normal)
            .await
            .unwrap();

        assert!(fake.calls().is_empty());
        assert!(chat_rx.try_recv().is_err());
    }

    /// Regression test for a long OBS transition (e.g. a stinger): OBS
    /// accepts `SetCurrentProgramScene`, but `current_scene` doesn't update
    /// until the transition actually finishes. Repeated polling cycles
    /// during that window must not send duplicate requests or duplicate
    /// announcements.
    #[tokio::test]
    async fn duplicate_switch_suppressed_while_transition_pending() {
        let fake = FakeObs::new();
        let (switcher, mut chat_rx) = build_switcher(fake.clone(), "a");

        // 1 & 2: NOALBS decides to switch A -> B; SetCurrentProgramScene
        // succeeds.
        switcher
            .switch_if_necessary("b", SwitchType::Normal)
            .await
            .unwrap();
        assert_eq!(fake.calls(), vec!["b".to_string()]);
        assert!(chat_rx.try_recv().is_ok(), "expected one announcement");

        // 3 & 4: OBS does not immediately report scene B as current (the
        // transition is still playing out), and several polling cycles
        // occur while it's still active.
        for _ in 0..3 {
            switcher
                .switch_if_necessary("b", SwitchType::Normal)
                .await
                .unwrap();
        }

        // 5 & 6: must not have sent another identical request or repeated
        // the announcement.
        assert_eq!(fake.calls(), vec!["b".to_string()]);
        assert!(
            chat_rx.try_recv().is_err(),
            "must not repeat the announcement while the switch is pending"
        );

        // 7: the transition ends / `CurrentProgramSceneChanged` reports
        // scene B (this is what the real event handler does).
        {
            let mut state = switcher.state.write().await;
            state.broadcasting_software.current_scene = "b".to_string();
            state.broadcasting_software.clear_pending_switch();
        }

        // 8: back to normal operation -- already on the target scene, so
        // this is a no-op.
        switcher
            .switch_if_necessary("b", SwitchType::Normal)
            .await
            .unwrap();
        assert_eq!(fake.calls(), vec!["b".to_string()]);

        // 9: a later legitimate switch to a new scene must still work.
        switcher
            .switch_if_necessary("c", SwitchType::Low)
            .await
            .unwrap();
        assert_eq!(fake.calls(), vec!["b".to_string(), "c".to_string()]);
        assert!(chat_rx.try_recv().is_ok(), "the new switch should announce");
    }

    #[tokio::test]
    async fn different_target_scene_is_not_blocked_by_a_pending_switch() {
        let fake = FakeObs::new();
        let (switcher, _chat_rx) = build_switcher(fake.clone(), "a");

        switcher
            .switch_if_necessary("b", SwitchType::Normal)
            .await
            .unwrap();

        // A different target is requested while the switch to "b" is still
        // pending (current_scene is still "a" in this fixture).
        switcher
            .switch_if_necessary("c", SwitchType::Low)
            .await
            .unwrap();

        assert_eq!(fake.calls(), vec!["b".to_string(), "c".to_string()]);
    }

    #[tokio::test]
    async fn pending_switch_times_out_and_allows_retry() {
        let fake = FakeObs::new();
        let (switcher, _chat_rx) = build_switcher(fake.clone(), "a");

        switcher
            .switch_if_necessary("b", SwitchType::Normal)
            .await
            .unwrap();
        assert_eq!(fake.calls(), vec!["b".to_string()]);

        // Simulate the confirmation event never arriving (e.g. a dropped
        // connection) for long enough that the pending switch goes stale.
        {
            let mut state = switcher.state.write().await;
            state.broadcasting_software.pending_switch = Some(state::PendingSwitch {
                scene: "b".to_string(),
                requested_at: std::time::Instant::now()
                    - (state::BroadcastingSoftwareState::PENDING_SWITCH_TIMEOUT
                        + Duration::from_secs(1)),
            });
        }

        switcher
            .switch_if_necessary("b", SwitchType::Normal)
            .await
            .unwrap();
        assert_eq!(fake.calls(), vec!["b".to_string(), "b".to_string()]);
    }

    /// A genuine failure (as opposed to a duplicate while pending) must not
    /// be treated as accepted, so retry behavior for real failures is
    /// unaffected.
    #[tokio::test]
    async fn failed_switch_does_not_set_pending_and_can_retry() {
        let fake = FakeObs::new();
        fake.fail_next_call();
        let (switcher, mut chat_rx) = build_switcher(fake.clone(), "a");

        switcher
            .switch_if_necessary("b", SwitchType::Normal)
            .await
            .unwrap();
        assert!(
            fake.calls().is_empty(),
            "a failed call must not be recorded as accepted"
        );
        assert!(chat_rx.try_recv().is_err());

        // Retry immediately -- must not be blocked by a bogus pending
        // state left over from the failed attempt.
        switcher
            .switch_if_necessary("b", SwitchType::Normal)
            .await
            .unwrap();
        assert_eq!(fake.calls(), vec!["b".to_string()]);
        assert!(chat_rx.try_recv().is_ok());
    }
}
