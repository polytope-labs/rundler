// This file is part of Rundler.
//
// Rundler is free software: you can redistribute it and/or modify it under the
// terms of the GNU Lesser General Public License as published by the Free Software
// Foundation, either version 3 of the License, or (at your option) any later version.
//
// Rundler is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
// without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with Rundler.
// If not, see https://www.gnu.org/licenses/.

//! A shared gate that decides when idle components may stop polling the node.

use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError},
    time::{Duration, Instant},
};

use tokio::sync::watch;

/// Settings for an [`IdleGate`].
#[derive(Clone, Copy, Debug, Default)]
pub struct IdleSettings {
    /// Whether the gate may pause at all.
    pub enabled: bool,
    /// How long the gate must see no activity before it may pause.
    pub grace: Duration,
}

/// Decides when the system is idle enough to pause, and wakes it on activity.
///
/// Lock order: the gate lock is taken first, then any mempool locks taken
/// inside the `is_quiescent` or `on_pause` callbacks of [`IdleGate::try_pause`].
/// The gate lock is only held for short synchronous sections, never across an
/// await, and those callbacks must not call back into the gate.
#[derive(Clone)]
pub struct IdleGate {
    state: Arc<Mutex<State>>,
    signal_tx: Arc<watch::Sender<Signal>>,
}

#[derive(Debug)]
struct State {
    enabled: bool,
    grace: Duration,
    holds: usize,
    last_activity: Instant,
    paused: bool,
    /// Set by a pause and cleared by [`IdleGate::set_resume_floor`], so a
    /// wake alone does not let bundling resume before the chain resyncs.
    resync_pending: bool,
    resume_floor: Option<u64>,
    resume_epoch: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct Signal {
    paused: bool,
    resync_pending: bool,
}

/// Keeps an [`IdleGate`] from pausing until dropped.
#[must_use = "the gate stays held only while this value is alive"]
#[derive(Debug)]
pub struct IdleHold {
    gate: IdleGate,
}

impl IdleGate {
    /// Creates a gate from settings.
    pub fn new(settings: IdleSettings) -> Self {
        let (signal_tx, _) = watch::channel(Signal::default());
        Self {
            state: Arc::new(Mutex::new(State {
                enabled: settings.enabled,
                grace: settings.grace,
                holds: 0,
                last_activity: Instant::now(),
                paused: false,
                resync_pending: false,
                resume_floor: None,
                resume_epoch: 0,
            })),
            signal_tx: Arc::new(signal_tx),
        }
    }

    /// Creates a gate that never pauses.
    pub fn disabled() -> Self {
        Self::new(IdleSettings::default())
    }

    /// Records activity, waking the gate if it is paused.
    pub fn touch(&self) {
        let mut state = self.lock();
        state.last_activity = Instant::now();
        self.wake(&mut state);
    }

    /// Records activity and blocks pausing until the returned hold is dropped.
    pub fn hold(&self) -> IdleHold {
        let mut state = self.lock();
        state.holds += 1;
        state.last_activity = Instant::now();
        self.wake(&mut state);
        IdleHold { gate: self.clone() }
    }

    /// Pauses the gate if it is enabled, unheld, past its grace period and
    /// `is_quiescent` returns true. `on_pause` runs under the gate lock only
    /// when the pause succeeds.
    ///
    /// Returns whether the gate is paused. An already paused gate returns true
    /// without calling either callback.
    pub fn try_pause(&self, is_quiescent: impl FnOnce() -> bool, on_pause: impl FnOnce()) -> bool {
        self.try_pause_at(Instant::now(), is_quiescent, on_pause)
    }

    fn try_pause_at(
        &self,
        now: Instant,
        is_quiescent: impl FnOnce() -> bool,
        on_pause: impl FnOnce(),
    ) -> bool {
        let mut state = self.lock();
        if state.paused {
            return true;
        }
        if !state.enabled
            || state.holds > 0
            || now.saturating_duration_since(state.last_activity) < state.grace
            || !is_quiescent()
        {
            return false;
        }
        on_pause();
        state.paused = true;
        state.resync_pending = true;
        self.publish(&state);
        true
    }

    /// Returns whether the gate is paused.
    pub fn is_paused(&self) -> bool {
        self.lock().paused
    }

    /// Resolves once the gate pauses, and keeps resolving until the resync
    /// after that pause is recorded with [`IdleGate::set_resume_floor`], so a
    /// pause already undone by a wake is not missed. Never resolves for a
    /// disabled gate.
    pub async fn wait_for_pause(&self) {
        if !self.lock().enabled {
            return std::future::pending().await;
        }
        let mut rx = self.signal_tx.subscribe();
        let _ = rx
            .wait_for(|signal| signal.paused || signal.resync_pending)
            .await;
    }

    /// Resolves once the gate is awake, immediately if it is not paused.
    pub async fn wait_for_wake(&self) {
        let mut rx = self.signal_tx.subscribe();
        let _ = rx.wait_for(|signal| !signal.paused).await;
    }

    /// Sets the block number that must be reached before bundling resumes,
    /// records that the resync after a pause is done and advances the
    /// [resume epoch](IdleGate::resume_epoch). Call it only once that resync
    /// has succeeded.
    pub fn set_resume_floor(&self, block_number: u64) {
        let mut state = self.lock();
        state.resume_floor = Some(block_number);
        state.resume_epoch += 1;
        if state.resync_pending {
            state.resync_pending = false;
            self.publish(&state);
        }
    }

    /// Returns how many times [`IdleGate::set_resume_floor`] has been called,
    /// so a consumer can tell that a resync happened since it last looked.
    pub fn resume_epoch(&self) -> u64 {
        self.lock().resume_epoch
    }

    /// Returns whether bundling must wait, either because the gate is paused,
    /// because the resync after a pause has not finished, or because
    /// `last_block_number` is still below the resume floor.
    pub fn bundling_blocked(&self, last_block_number: u64) -> bool {
        let state = self.lock();
        state.paused
            || state.resync_pending
            || state
                .resume_floor
                .is_some_and(|floor| last_block_number < floor)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wake(&self, state: &mut State) {
        if state.paused {
            state.paused = false;
            self.publish(state);
        }
    }

    fn publish(&self, state: &State) {
        self.signal_tx.send_replace(Signal {
            paused: state.paused,
            resync_pending: state.resync_pending,
        });
    }
}

impl fmt::Debug for IdleGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("IdleGate");
        // Formatting may happen inside a gate callback, where the lock is held.
        match self.state.try_lock() {
            Ok(state) => s.field("state", &*state),
            Err(TryLockError::Poisoned(err)) => s.field("state", &*err.into_inner()),
            Err(TryLockError::WouldBlock) => s.field("state", &"<locked>"),
        };
        s.finish()
    }
}

impl Drop for IdleHold {
    fn drop(&mut self) {
        let mut state = self.gate.lock();
        state.holds = state.holds.saturating_sub(1);
        state.last_activity = Instant::now();
        self.gate.wake(&mut state);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tokio::{task, time::timeout};

    use super::*;

    const GRACE: Duration = Duration::from_secs(10);
    const WAIT: Duration = Duration::from_secs(5);

    fn enabled(grace: Duration) -> IdleGate {
        IdleGate::new(IdleSettings {
            enabled: true,
            grace,
        })
    }

    fn past_grace() -> Instant {
        Instant::now() + GRACE
    }

    fn idle_for(gate: &IdleGate, idle: Duration) {
        gate.lock().last_activity = Instant::now().checked_sub(idle).unwrap();
    }

    #[tokio::test]
    async fn disabled_gate_never_pauses() {
        let gate = IdleGate::disabled();

        assert!(!gate.try_pause_at(past_grace(), || true, || panic!("paused")));
        assert!(!gate.is_paused());
        assert!(!gate.bundling_blocked(0));
        assert!(
            timeout(Duration::from_millis(20), gate.wait_for_pause())
                .await
                .is_err()
        );
    }

    #[test]
    fn hold_blocks_pause() {
        let gate = enabled(GRACE);

        let hold = gate.hold();
        let second = gate.hold();
        assert!(!gate.try_pause_at(past_grace(), || true, || {}));

        drop(hold);
        assert!(!gate.try_pause_at(past_grace(), || true, || {}));

        drop(second);
        assert!(gate.try_pause_at(past_grace(), || true, || {}));
    }

    #[test]
    fn grace_period_is_respected() {
        let start = Instant::now();
        let gate = enabled(GRACE);

        assert!(!gate.try_pause_at(start, || true, || {}));
        assert!(!gate.try_pause_at(start + GRACE - Duration::from_millis(1), || true, || {}));
        assert!(gate.try_pause_at(past_grace(), || true, || {}));
    }

    #[test]
    fn touch_and_hold_restart_grace_period() {
        let gate = enabled(GRACE);

        idle_for(&gate, GRACE * 2);
        gate.touch();
        assert!(!gate.try_pause_at(Instant::now(), || true, || {}));

        idle_for(&gate, GRACE * 2);
        drop(gate.hold());
        assert!(!gate.try_pause_at(Instant::now(), || true, || {}));

        assert!(gate.try_pause_at(past_grace(), || true, || {}));
    }

    #[test]
    fn busy_pool_blocks_pause() {
        let gate = enabled(Duration::ZERO);

        assert!(!gate.try_pause_at(past_grace(), || false, || {}));
        assert!(!gate.is_paused());
        assert!(gate.try_pause_at(past_grace(), || true, || {}));
        assert!(gate.is_paused());
    }

    #[test]
    fn on_pause_runs_only_when_pause_succeeds() {
        let gate = enabled(GRACE);
        let calls = Cell::new(0);
        let on_pause = || calls.set(calls.get() + 1);

        assert!(!gate.try_pause_at(Instant::now(), || true, on_pause));
        assert!(!gate.try_pause_at(past_grace(), || false, on_pause));
        let hold = gate.hold();
        assert!(!gate.try_pause_at(past_grace(), || true, on_pause));
        drop(hold);
        assert_eq!(calls.get(), 0);

        assert!(gate.try_pause_at(past_grace(), || true, on_pause));
        assert_eq!(calls.get(), 1);

        assert!(gate.try_pause_at(past_grace(), || false, on_pause));
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn wait_for_pause_resolves_on_pause() {
        let gate = enabled(Duration::ZERO);
        let waiter = task::spawn({
            let gate = gate.clone();
            async move { gate.wait_for_pause().await }
        });
        task::yield_now().await;
        assert!(!waiter.is_finished());

        assert!(gate.try_pause(|| true, || {}));
        timeout(WAIT, waiter).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn wait_for_wake_resolves_when_awake() {
        let gate = enabled(Duration::ZERO);
        timeout(WAIT, gate.wait_for_wake()).await.unwrap();
    }

    #[tokio::test]
    async fn touch_wakes_waiter() {
        let gate = enabled(Duration::ZERO);
        assert!(gate.try_pause(|| true, || {}));

        let waiter = task::spawn({
            let gate = gate.clone();
            async move { gate.wait_for_wake().await }
        });
        task::yield_now().await;
        assert!(!waiter.is_finished());

        gate.touch();
        timeout(WAIT, waiter).await.unwrap().unwrap();
        assert!(!gate.is_paused());
    }

    #[tokio::test]
    async fn hold_wakes_waiter() {
        let gate = enabled(Duration::ZERO);
        assert!(gate.try_pause(|| true, || {}));

        let waiter = task::spawn({
            let gate = gate.clone();
            async move { gate.wait_for_wake().await }
        });
        task::yield_now().await;
        assert!(!waiter.is_finished());

        let hold = gate.hold();
        timeout(WAIT, waiter).await.unwrap().unwrap();
        assert!(!gate.try_pause(|| true, || {}));

        drop(hold);
        assert!(gate.try_pause(|| true, || {}));
    }

    #[test]
    fn resume_floor_blocks_bundling_until_reached() {
        let gate = enabled(Duration::ZERO);
        assert!(!gate.bundling_blocked(0));

        gate.set_resume_floor(10);
        assert!(gate.bundling_blocked(9));
        assert!(!gate.bundling_blocked(10));
        assert!(!gate.bundling_blocked(11));

        assert!(gate.try_pause(|| true, || {}));
        assert!(gate.bundling_blocked(11));
    }

    #[test]
    fn bundling_waits_for_resync_after_wake() {
        let gate = enabled(Duration::ZERO);
        assert!(gate.try_pause(|| true, || {}));

        gate.touch();
        assert!(!gate.is_paused());
        assert!(gate.bundling_blocked(u64::MAX));

        gate.set_resume_floor(20);
        assert!(gate.bundling_blocked(19));
        assert!(!gate.bundling_blocked(20));
    }

    #[test]
    fn resume_epoch_advances_on_each_resume_floor() {
        let gate = enabled(Duration::ZERO);
        assert_eq!(gate.resume_epoch(), 0);

        gate.set_resume_floor(10);
        assert_eq!(gate.resume_epoch(), 1);

        assert!(gate.try_pause(|| true, || {}));
        gate.touch();
        assert_eq!(gate.resume_epoch(), 1, "a wake alone is not a resync");

        gate.set_resume_floor(10);
        assert_eq!(gate.resume_epoch(), 2);
    }

    #[tokio::test]
    async fn wait_for_pause_resolves_for_pause_already_undone() {
        let gate = enabled(Duration::ZERO);
        assert!(gate.try_pause(|| true, || {}));
        gate.touch();

        timeout(WAIT, gate.wait_for_pause()).await.unwrap();
        timeout(WAIT, gate.wait_for_wake()).await.unwrap();

        gate.set_resume_floor(1);
        assert!(
            timeout(Duration::from_millis(20), gate.wait_for_pause())
                .await
                .is_err()
        );
    }
}
