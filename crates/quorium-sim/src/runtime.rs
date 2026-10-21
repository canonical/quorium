use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::io;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use openraft_rt::{AsyncRuntime, OptionalSend};
use openraft_rt_tokio::{TokioMpsc, TokioMutex, TokioOneshot, TokioWatch};
use rand::SeedableRng;
use rand::rngs::SmallRng;

use crate::rng::SimRng;

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

struct Inner {
    clock: Cell<Duration>,
    rng: RefCell<SimRng>,
    tasks: RefCell<BTreeMap<u64, (Task, i64)>>,
    next_task: Cell<u64>,
    ready: Arc<Mutex<Vec<u64>>>,
    timers: RefCell<BTreeMap<(Duration, u64), Waker>>,
    next_timer: Cell<u64>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<Inner>>> = const { RefCell::new(None) };
    static RATE_PPM: Cell<i64> = const { Cell::new(0) };
    static FALLBACK_RNG: RefCell<SimRng> = RefCell::new(SimRng::new(0));
}

fn current() -> Option<Rc<Inner>> {
    CURRENT.with(|c| c.borrow().clone())
}

fn expect_current() -> Rc<Inner> {
    current().expect("called outside of a SimRuntime")
}

const PPM: i128 = 1_000_000;

fn to_local(global: Duration, ppm: i64) -> Duration {
    let nanos = global.as_nanos() as i128 * (PPM + ppm as i128) / PPM;
    Duration::from_nanos(nanos.max(0) as u64)
}

fn to_global(local: Duration, ppm: i64) -> Duration {
    let rate = (PPM + ppm as i128).max(1);
    let nanos = (local.as_nanos() as i128 * PPM + rate - 1) / rate;
    Duration::from_nanos(nanos.max(0) as u64)
}

fn local_now() -> Duration {
    to_local(expect_current().clock.get(), RATE_PPM.with(Cell::get))
}

struct TaskWaker {
    id: u64,
    ready: Arc<Mutex<Vec<u64>>>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.lock().unwrap().push(self.id);
    }
}

struct FlagWaker(AtomicBool);

impl Wake for FlagWaker {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Inner {
    fn new(seed: u64) -> Self {
        Self {
            clock: Cell::new(Duration::ZERO),
            rng: RefCell::new(SimRng::new(seed)),
            tasks: RefCell::new(BTreeMap::new()),
            next_task: Cell::new(0),
            ready: Arc::new(Mutex::new(Vec::new())),
            timers: RefCell::new(BTreeMap::new()),
            next_timer: Cell::new(0),
        }
    }

    fn spawn_task(&self, task: Task) {
        let id = self.next_task.get();
        self.next_task.set(id + 1);
        self.tasks.borrow_mut().insert(id, (task, RATE_PPM.with(Cell::get)));
        self.ready.lock().unwrap().push(id);
    }

    fn poll_ready_tasks(&self) -> bool {
        let woken: Vec<u64> = std::mem::take(&mut *self.ready.lock().unwrap());
        if woken.is_empty() {
            return false;
        }
        let mut batch: Vec<u64> = woken.into_iter().collect::<BTreeSet<_>>().into_iter().collect();
        self.rng.borrow_mut().shuffle(&mut batch);
        for id in batch {
            let Some((mut task, ppm)) = self.tasks.borrow_mut().remove(&id) else {
                continue;
            };
            let waker = Waker::from(Arc::new(TaskWaker { id, ready: self.ready.clone() }));
            let outer = RATE_PPM.with(|r| r.replace(ppm));
            let pending = task.as_mut().poll(&mut Context::from_waker(&waker)).is_pending();
            RATE_PPM.with(|r| r.set(outer));
            if pending {
                self.tasks.borrow_mut().insert(id, (task, ppm));
            }
        }
        true
    }

    fn fire_next_timers(&self, horizon: Option<Duration>) -> bool {
        let Some(&(deadline, _)) = self.timers.borrow().keys().next() else {
            return false;
        };
        if horizon.is_some_and(|h| deadline > h) {
            return false;
        }
        self.clock.set(self.clock.get().max(deadline));
        let now = self.clock.get();
        loop {
            let mut timers = self.timers.borrow_mut();
            let Some(entry) = timers.first_entry() else { break };
            if entry.key().0 > now {
                break;
            }
            let waker = entry.remove();
            drop(timers);
            waker.wake();
        }
        true
    }

    fn register_timer(&self, deadline: Duration, waker: &Waker) -> (Duration, u64) {
        let seq = self.next_timer.get();
        self.next_timer.set(seq + 1);
        self.timers.borrow_mut().insert((deadline, seq), waker.clone());
        (deadline, seq)
    }

    fn cancel_timer(&self, key: (Duration, u64)) {
        self.timers.borrow_mut().remove(&key);
    }

    fn next_seed(&self) -> u64 {
        self.rng.borrow_mut().next_u64()
    }
}

pub struct SimRuntime {
    inner: Rc<Inner>,
}

impl fmt::Debug for SimRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimRuntime").field("now", &self.inner.clock.get()).finish()
    }
}

struct EnterGuard {
    previous: Option<Rc<Inner>>,
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.previous.take());
    }
}

impl SimRuntime {
    pub fn with_seed(seed: u64) -> Self {
        Self { inner: Rc::new(Inner::new(seed)) }
    }

    pub fn now(&self) -> Duration {
        self.inner.clock.get()
    }

    fn enter(&self) -> EnterGuard {
        let previous = CURRENT.with(|c| c.borrow_mut().replace(self.inner.clone()));
        EnterGuard { previous }
    }

    pub fn set_clock_rate(&self, ppm: i64) -> i64 {
        RATE_PPM.with(|r| r.replace(ppm))
    }

    pub fn with_clock_rate<T>(&self, ppm: i64, f: impl FnOnce() -> T) -> T {
        let outer = RATE_PPM.with(|r| r.replace(ppm));
        let out = f();
        RATE_PPM.with(|r| r.set(outer));
        out
    }

    pub fn spawn_detached<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.spawn_task(Box::pin(future));
    }

    pub fn spawn_in<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let _guard = self.enter();
        <Self as AsyncRuntime>::spawn(future)
    }

    pub fn run_until(&mut self, horizon: Duration) {
        let _guard = self.enter();
        loop {
            if self.inner.poll_ready_tasks() {
                continue;
            }
            if !self.inner.fire_next_timers(Some(horizon)) {
                self.inner.clock.set(self.inner.clock.get().max(horizon));
                return;
            }
        }
    }

    pub fn run_for(&mut self, duration: Duration) {
        let horizon = self.now() + duration;
        self.run_until(horizon);
    }

    pub fn pending_tasks(&self) -> usize {
        self.inner.tasks.borrow().len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SimInstant(Duration);

impl SimInstant {
    pub fn from_duration(d: Duration) -> Self {
        Self(d)
    }

    pub fn as_duration(&self) -> Duration {
        self.0
    }
}

impl Add<Duration> for SimInstant {
    type Output = SimInstant;
    fn add(self, rhs: Duration) -> SimInstant {
        SimInstant(self.0 + rhs)
    }
}

impl AddAssign<Duration> for SimInstant {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs;
    }
}

impl Sub<Duration> for SimInstant {
    type Output = SimInstant;
    fn sub(self, rhs: Duration) -> SimInstant {
        SimInstant(self.0.saturating_sub(rhs))
    }
}

impl SubAssign<Duration> for SimInstant {
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 = self.0.saturating_sub(rhs);
    }
}

impl Sub<SimInstant> for SimInstant {
    type Output = Duration;
    fn sub(self, rhs: SimInstant) -> Duration {
        self.0.saturating_sub(rhs.0)
    }
}

impl openraft_rt::Instant for SimInstant {
    fn now() -> Self {
        SimInstant(current().map(|c| to_local(c.clock.get(), RATE_PPM.with(Cell::get))).unwrap_or_default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinError;

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("task was dropped before completion")
    }
}

impl std::error::Error for JoinError {}

struct JoinState<T> {
    value: Option<T>,
    done: bool,
    waker: Option<Waker>,
}

pub struct JoinHandle<T> {
    state: Arc<Mutex<JoinState<T>>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();
        if let Some(v) = state.value.take() {
            return Poll::Ready(Ok(v));
        }
        if state.done {
            return Poll::Ready(Err(JoinError));
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

pub struct SimSleep {
    deadline: Duration,
    key: Option<(Duration, u64)>,
}

impl SimSleep {
    fn until(deadline: Duration) -> Self {
        Self { deadline, key: None }
    }
}

impl Future for SimSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let inner = expect_current();
        if let Some(key) = self.key.take() {
            inner.cancel_timer(key);
        }
        if inner.clock.get() >= self.deadline {
            return Poll::Ready(());
        }
        self.key = Some(inner.register_timer(self.deadline, cx.waker()));
        Poll::Pending
    }
}

impl Drop for SimSleep {
    fn drop(&mut self) {
        if let (Some(key), Some(inner)) = (self.key.take(), current()) {
            inner.cancel_timer(key);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

pin_project_lite::pin_project! {
    pub struct SimTimeout<F> {
        #[pin]
        future: F,
        #[pin]
        sleep: SimSleep,
    }
}

impl<F: Future> Future for SimTimeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(v) = this.future.poll(cx) {
            return Poll::Ready(Ok(v));
        }
        this.sleep.poll(cx).map(|_| Err(Elapsed))
    }
}

impl AsyncRuntime for SimRuntime {
    type JoinError = JoinError;
    type JoinHandle<T: OptionalSend + 'static> = JoinHandle<T>;
    type Sleep = SimSleep;
    type Instant = SimInstant;
    type TimeoutError = Elapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = SimTimeout<T>;
    type ThreadLocalRng = SmallRng;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        let state = Arc::new(Mutex::new(JoinState { value: None, done: false, waker: None }));
        let shared = state.clone();
        expect_current().spawn_task(Box::pin(async move {
            let v = future.await;
            let mut s = shared.lock().unwrap();
            s.value = Some(v);
            s.done = true;
            if let Some(w) = s.waker.take() {
                w.wake();
            }
        }));
        JoinHandle { state }
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        SimSleep::until(to_global(local_now() + duration, RATE_PPM.with(Cell::get)))
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        SimSleep::until(to_global(deadline.0, RATE_PPM.with(Cell::get)))
    }

    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        SimTimeout { future, sleep: Self::sleep(duration) }
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        SimTimeout { future, sleep: Self::sleep_until(deadline) }
    }

    fn is_panic(_join_error: &Self::JoinError) -> bool {
        false
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        let seed = match current() {
            Some(inner) => inner.next_seed(),
            None => FALLBACK_RNG.with(|r| r.borrow_mut().next_u64()),
        };
        SmallRng::seed_from_u64(seed)
    }

    type Mpsc = TokioMpsc;
    type Watch = TokioWatch;
    type Oneshot = TokioOneshot;
    type Mutex<T: OptionalSend + 'static> = TokioMutex<T>;

    fn new(_threads: usize) -> Self {
        Self::with_seed(0)
    }

    fn block_on<F, T>(&mut self, future: F) -> T
    where
        F: Future<Output = T>,
    {
        let _guard = self.enter();
        let mut future = pin!(future);
        let flag = Arc::new(FlagWaker(AtomicBool::new(true)));
        let waker = Waker::from(flag.clone());
        loop {
            if flag.0.swap(false, Ordering::SeqCst) {
                if let Poll::Ready(v) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
                    return v;
                }
                continue;
            }
            if self.inner.poll_ready_tasks() {
                continue;
            }
            if flag.0.load(Ordering::SeqCst) {
                continue;
            }
            if !self.inner.fire_next_timers(None) {
                panic!("simulation deadlock: no runnable task and no pending timer");
            }
        }
    }

    fn spawn_blocking<F, T>(f: F) -> impl Future<Output = Result<T, io::Error>> + Send
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        std::future::ready(Ok(f()))
    }
}
