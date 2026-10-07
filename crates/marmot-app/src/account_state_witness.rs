//! Test-only direct witness for account_local_ready_before_subscribe_body.

#![cfg(test)]

use std::cell::RefCell;
use std::marker::PhantomData;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

const CAPACITY: usize = 64;
const MULTIPLE: u64 = 1;
const IDENTITY: u64 = 2;
const OVERFLOW: u64 = 4;
const INCONSISTENT: u64 = 8;
const SECOND_WRITER: u64 = 16;
const CHANGED: u64 = 32;
const MISSING: u64 = 64;
#[cfg(feature = "test-policy-overrides")]
const FOREIGN_STORAGE_CALLBACK: u64 = 128;
#[cfg(feature = "test-policy-overrides")]
const MIGRATION_CURSOR_FAULT: u64 = 256;

#[derive(Clone, Copy)]
#[repr(u64)]
pub(crate) enum Phase {
    AccountState = 1,
    AccountLookup = 2,
    ReadyLock = 3,
    TestGate = 4,
    StrictCutover = 5,
    AccountStorage = 7,
    ProjectionIdentity = 8,
    DatabaseLock = 9,
    KeySelection = 10,
    EncryptedOpen = 11,
    #[cfg(feature = "test-policy-overrides")]
    PrivateFiles = 12,
    #[cfg(feature = "test-policy-overrides")]
    RusqliteOpen = 13,
    #[cfg(feature = "test-policy-overrides")]
    CipherPragmas = 14,
    #[cfg(feature = "test-policy-overrides")]
    KeyPragma = 15,
    #[cfg(feature = "test-policy-overrides")]
    AuthenticatedSchema = 16,
    #[cfg(feature = "test-policy-overrides")]
    OperationalPreWal = 17,
    #[cfg(feature = "test-policy-overrides")]
    Wal = 18,
    #[cfg(feature = "test-policy-overrides")]
    Migrations = 19,
}

#[cfg(feature = "test-policy-overrides")]
const MAX_PHASE: u64 = 19;
#[cfg(not(feature = "test-policy-overrides"))]
const MAX_PHASE: u64 = 11;

struct Slot {
    operation: AtomicU64,
    tid: AtomicU64,
    phase: AtomicU64,
    kind: AtomicU64,
    elapsed_us: AtomicU64,
    sequence: AtomicU64,
}

impl Slot {
    const fn new() -> Self {
        Self {
            operation: AtomicU64::new(0),
            tid: AtomicU64::new(0),
            phase: AtomicU64::new(0),
            kind: AtomicU64::new(0),
            elapsed_us: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
        }
    }
}

#[cfg(feature = "test-policy-overrides")]
struct MigrationCursor {
    publication: AtomicU64,
    operation: AtomicU64,
    tid: AtomicU64,
    value: AtomicU64,
    elapsed_us: AtomicU64,
}

#[cfg(feature = "test-policy-overrides")]
impl MigrationCursor {
    const fn new() -> Self {
        Self {
            publication: AtomicU64::new(0),
            operation: AtomicU64::new(0),
            tid: AtomicU64::new(0),
            value: AtomicU64::new(0),
            elapsed_us: AtomicU64::new(0),
        }
    }
}

#[cfg(feature = "test-policy-overrides")]
#[derive(Clone, Copy)]
struct MigrationRow {
    publication: u64,
    operation: u64,
    tid: u64,
    version: i64,
    step: u64,
    kind: u64,
    elapsed_us: u64,
}

#[cfg(feature = "test-policy-overrides")]
fn valid_migration_cursor(version: i64, step: u64, kind: u64) -> bool {
    (1..=4).contains(&kind) && version == 1 && (6..=9).contains(&step)
}

pub(crate) struct Witness {
    started: OnceLock<Instant>,
    claims: AtomicU64,
    active: AtomicBool,
    faults: AtomicU64,
    pid: AtomicU64,
    tid: AtomicU64,
    published: AtomicUsize,
    publication: AtomicU64,
    slots: [Slot; CAPACITY],
    #[cfg(feature = "test-policy-overrides")]
    migration: MigrationCursor,
}

#[derive(Clone)]
struct Binding {
    witness: Arc<Witness>,
    tid: u64,
    #[cfg(feature = "test-policy-overrides")]
    owner_thread: std::thread::ThreadId,
}

thread_local! {
    static CURRENT: RefCell<Option<Binding>> = const { RefCell::new(None) };
}

impl Witness {
    pub(crate) fn new() -> Self {
        Self {
            started: OnceLock::new(),
            claims: AtomicU64::new(0),
            active: AtomicBool::new(false),
            faults: AtomicU64::new(0),
            pid: AtomicU64::new(0),
            tid: AtomicU64::new(0),
            published: AtomicUsize::new(0),
            publication: AtomicU64::new(0),
            slots: [const { Slot::new() }; CAPACITY],
            #[cfg(feature = "test-policy-overrides")]
            migration: MigrationCursor::new(),
        }
    }

    pub(crate) fn arm(&self, started: Instant) {
        if self.started.set(started).is_err() {
            self.faults.fetch_or(INCONSISTENT, Ordering::Release);
        }
    }

    pub(crate) fn begin(self: &Arc<Self>) -> Option<Operation> {
        self.begin_with_identity(native_identity())
    }

    fn begin_with_identity(self: &Arc<Self>, identity: Option<(u32, u32)>) -> Option<Operation> {
        if self
            .claims
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.claims.store(2, Ordering::Release);
            let fault = MULTIPLE
                | if self.active.load(Ordering::Acquire) {
                    SECOND_WRITER
                } else {
                    0
                };
            self.faults.fetch_or(fault, Ordering::Release);
            return None;
        }
        let Some((pid, tid)) = identity.filter(|(pid, tid)| *pid != 0 && *tid != 0) else {
            self.faults.fetch_or(IDENTITY, Ordering::Release);
            return None;
        };
        if self.started.get().is_none() {
            self.faults.fetch_or(IDENTITY, Ordering::Release);
            return None;
        }
        self.pid.store(u64::from(pid), Ordering::Release);
        self.tid.store(u64::from(tid), Ordering::Release);
        self.active.store(true, Ordering::Release);
        let binding = Binding {
            witness: self.clone(),
            tid: u64::from(tid),
            #[cfg(feature = "test-policy-overrides")]
            owner_thread: std::thread::current().id(),
        };
        let previous = CURRENT.with(|current| current.replace(Some(binding.clone())));
        binding.record(Phase::AccountState, 1);
        Some(Operation {
            binding,
            previous,
            returned: false,
            _same_thread: PhantomData,
        })
    }

    fn offset_us(&self) -> u64 {
        match self
            .started
            .get()
            .and_then(|started| started.elapsed().as_micros().try_into().ok())
        {
            Some(offset) => offset,
            None => {
                self.faults.fetch_or(IDENTITY, Ordering::Release);
                0
            }
        }
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        let start_us = self.offset_us();
        let before = self.state();
        let len = before.published.min(CAPACITY);
        let mut rows = [Row::default(); CAPACITY];
        let mut faults = before.faults;
        let mut depth = 0;
        let mut stack = [0; CAPACITY];
        let mut last_us = 0;
        for (index, row) in rows.iter_mut().enumerate().take(len) {
            let slot = &self.slots[index];
            let sequence = slot.sequence.load(Ordering::Acquire);
            *row = Row {
                operation: slot.operation.load(Ordering::Relaxed),
                tid: slot.tid.load(Ordering::Relaxed),
                phase: slot.phase.load(Ordering::Relaxed),
                kind: slot.kind.load(Ordering::Relaxed),
                sequence,
                elapsed_us: slot.elapsed_us.load(Ordering::Relaxed),
            };
            if sequence != (index + 1) as u64
                || row.operation != 1
                || row.tid != before.tid
                || !(1..=MAX_PHASE).contains(&row.phase)
                || row.phase == 6
                || row.elapsed_us < last_us
            {
                faults |= INCONSISTENT;
            }
            last_us = row.elapsed_us;
            match row.kind {
                1 if depth < CAPACITY => {
                    stack[depth] = row.phase;
                    depth += 1;
                }
                2..=4 if depth > 0 && stack[depth - 1] == row.phase => {
                    depth -= 1;
                }
                _ => {
                    faults |= INCONSISTENT;
                }
            }
        }
        #[cfg(feature = "test-policy-overrides")]
        let (mut migration, migration_fault) = self.migration_snapshot(&before);
        #[cfg(feature = "test-policy-overrides")]
        {
            faults |= migration_fault;
        }
        let after = self.state();
        let end_us = self.offset_us();
        if before != after {
            faults |= CHANGED;
            #[cfg(feature = "test-policy-overrides")]
            {
                migration = None;
            }
        }
        if before.published > CAPACITY
            || after.published > CAPACITY
            || before.publication != (before.published * 2) as u64
            || after.publication != (after.published * 2) as u64
        {
            faults |= INCONSISTENT;
        }
        if before.claims == 0 {
            faults |= MISSING;
        }
        if before.claims > 1 {
            faults |= MULTIPLE;
        }
        let pid = before.pid;
        if pid == 0 || before.tid == 0 {
            faults |= IDENTITY;
        }
        if before.published > CAPACITY
            || last_us > end_us
            || len == 0
            || rows[0].phase != Phase::AccountState as u64
            || rows[0].kind != 1
            || (before.active && (depth == 0 || stack[0] != Phase::AccountState as u64))
            || (!before.active && depth != 0)
        {
            faults |= INCONSISTENT;
        }
        #[cfg(feature = "test-policy-overrides")]
        if let Some(cursor) = migration {
            let parent_entered = rows[..len]
                .iter()
                .find(|row| row.phase == 19 && row.kind == 1)
                .map(|row| row.elapsed_us);
            if cursor.elapsed_us > end_us
                || parent_entered.is_none_or(|entered| cursor.elapsed_us < entered)
                || (cursor.kind == 1 && (!before.active || !stack[..depth].contains(&19)))
            {
                faults |= MIGRATION_CURSOR_FAULT;
                migration = None;
            }
        }
        Snapshot {
            pid,
            tid: before.tid,
            claims: before.claims,
            active: before.active,
            faults,
            start_us,
            end_us,
            len,
            rows,
            #[cfg(feature = "test-policy-overrides")]
            migration,
            #[cfg(feature = "test-policy-overrides")]
            migration_publication: before.migration_publication,
        }
    }

    #[cfg(feature = "test-policy-overrides")]
    fn migration_snapshot(&self, state: &State) -> (Option<MigrationRow>, u64) {
        let cursor = &self.migration;
        let before = cursor.publication.load(Ordering::SeqCst);
        let operation = cursor.operation.load(Ordering::SeqCst);
        let tid = cursor.tid.load(Ordering::SeqCst);
        let value = cursor.value.load(Ordering::SeqCst);
        let elapsed_us = cursor.elapsed_us.load(Ordering::SeqCst);
        let after = cursor.publication.load(Ordering::SeqCst);
        if before != after || before != state.migration_publication || before % 2 != 0 {
            return (None, MIGRATION_CURSOR_FAULT);
        }
        if before == 0 {
            return (
                None,
                if operation == 0 && tid == 0 && value == 0 && elapsed_us == 0 {
                    0
                } else {
                    MIGRATION_CURSOR_FAULT
                },
            );
        }
        let version = (value & 127) as i64;
        let step = (value >> 7) & 15;
        let kind = (value >> 11) & 7;
        if state.claims != 1
            || state.pid == 0
            || operation != 1
            || tid == 0
            || tid != state.tid
            || value >> 14 != 0
            || !valid_migration_cursor(version, step, kind)
        {
            return (None, MIGRATION_CURSOR_FAULT);
        }
        (
            Some(MigrationRow {
                publication: before,
                operation,
                tid,
                version,
                step,
                kind,
                elapsed_us,
            }),
            0,
        )
    }

    fn state(&self) -> State {
        State {
            claims: self.claims.load(Ordering::Acquire),
            active: self.active.load(Ordering::Acquire),
            faults: self.faults.load(Ordering::Acquire),
            pid: self.pid.load(Ordering::Acquire),
            tid: self.tid.load(Ordering::Acquire),
            published: self.published.load(Ordering::Acquire),
            publication: self.publication.load(Ordering::Acquire),
            #[cfg(feature = "test-policy-overrides")]
            migration_publication: self.migration.publication.load(Ordering::SeqCst),
        }
    }
}

#[derive(PartialEq, Eq)]
struct State {
    claims: u64,
    active: bool,
    faults: u64,
    pid: u64,
    tid: u64,
    published: usize,
    publication: u64,
    #[cfg(feature = "test-policy-overrides")]
    migration_publication: u64,
}

impl Binding {
    #[cfg(feature = "test-policy-overrides")]
    fn storage_context_matches(&self) -> bool {
        self.owner_thread == std::thread::current().id()
            && self.witness.active.load(Ordering::Acquire)
            && self.witness.claims.load(Ordering::Acquire) == 1
            && self.witness.tid.load(Ordering::Acquire) == self.tid
            && CURRENT.with(|current| {
                current.borrow().as_ref().is_some_and(|current| {
                    current.tid == self.tid && Arc::ptr_eq(&current.witness, &self.witness)
                })
            })
    }

    #[cfg(feature = "test-policy-overrides")]
    fn record_event(&self, event: storage_sqlite::DiagnosticOpenEvent, kind: u64) {
        use storage_sqlite::{DiagnosticOpenEvent, DiagnosticOpenPhase as StoragePhase};
        let DiagnosticOpenEvent::Storage(phase) = event else {
            if let DiagnosticOpenEvent::Migration { version, step } = event {
                self.record_migration(version, step as u64, kind);
            }
            return;
        };
        if !self.storage_context_matches() {
            self.witness
                .faults
                .fetch_or(FOREIGN_STORAGE_CALLBACK, Ordering::Release);
            return;
        }
        if !(1..=4).contains(&kind) {
            self.witness
                .faults
                .fetch_or(INCONSISTENT, Ordering::Release);
            return;
        }
        let phase = match phase {
            StoragePhase::PrivateFiles => Phase::PrivateFiles,
            StoragePhase::RusqliteOpen => Phase::RusqliteOpen,
            StoragePhase::CipherPragmas => Phase::CipherPragmas,
            StoragePhase::KeyPragma => Phase::KeyPragma,
            StoragePhase::AuthenticatedSchema => Phase::AuthenticatedSchema,
            StoragePhase::OperationalPreWal => Phase::OperationalPreWal,
            StoragePhase::Wal => Phase::Wal,
            StoragePhase::Migrations => Phase::Migrations,
        };
        self.record(phase, kind);
    }

    #[cfg(feature = "test-policy-overrides")]
    fn record_migration(&self, version: i64, step: u64, kind: u64) {
        if !self.storage_context_matches() {
            self.witness
                .faults
                .fetch_or(FOREIGN_STORAGE_CALLBACK, Ordering::Release);
            return;
        }
        if !valid_migration_cursor(version, step, kind) {
            self.witness
                .faults
                .fetch_or(MIGRATION_CURSOR_FAULT, Ordering::Release);
            return;
        }
        let cursor = &self.witness.migration;
        let publication = cursor.publication.load(Ordering::SeqCst);
        let Some(done) = publication.checked_add(2).filter(|_| publication % 2 == 0) else {
            self.witness
                .faults
                .fetch_or(MIGRATION_CURSOR_FAULT, Ordering::Release);
            return;
        };
        // All cursor fields and both markers are SeqCst. A reader samples once,
        // never waits, and refuses odd/changing epochs or inconsistent ownership.
        cursor.publication.store(publication + 1, Ordering::SeqCst);
        cursor.operation.store(1, Ordering::SeqCst);
        cursor.tid.store(self.tid, Ordering::SeqCst);
        cursor.value.store(
            version as u64 | (step << 7) | (kind << 11),
            Ordering::SeqCst,
        );
        cursor
            .elapsed_us
            .store(self.witness.offset_us(), Ordering::SeqCst);
        cursor.publication.store(done, Ordering::SeqCst);
    }

    fn record(&self, phase: Phase, kind: u64) {
        let witness = &self.witness;
        let index = witness.published.load(Ordering::Relaxed);
        if index >= CAPACITY {
            witness.faults.fetch_or(OVERFLOW, Ordering::Release);
            return;
        }
        witness
            .publication
            .store((index * 2 + 1) as u64, Ordering::Release);
        let slot = &witness.slots[index];
        slot.operation.store(1, Ordering::Relaxed);
        slot.tid.store(self.tid, Ordering::Relaxed);
        slot.phase.store(phase as u64, Ordering::Relaxed);
        slot.kind.store(kind, Ordering::Relaxed);
        slot.elapsed_us
            .store(witness.offset_us(), Ordering::Relaxed);
        // A slot is immutable after this release; readers never wait for an unfinished slot.
        slot.sequence.store((index + 1) as u64, Ordering::Release);
        witness.published.store(index + 1, Ordering::Release);
        witness
            .publication
            .store((index * 2 + 2) as u64, Ordering::Release);
    }
}

/// The borrowed inline callback is supplied only by this exact App/operation.
/// Other fixture Apps and all normal library opens use the original None path.
#[cfg(feature = "test-policy-overrides")]
pub(crate) fn open_encrypted(
    witness: Option<&Arc<Witness>>,
    path: &Path,
    key: &storage_sqlite::SqlCipherKey,
) -> cgka_traits::storage::StorageResult<storage_sqlite::SqliteAccountStorage> {
    let binding = witness.and_then(|witness| {
        CURRENT.with(|current| {
            current
                .borrow()
                .as_ref()
                .filter(|binding| Arc::ptr_eq(witness, &binding.witness))
                .cloned()
        })
    });
    let Some(binding) = binding else {
        return storage_sqlite::SqliteAccountStorage::open_encrypted(path, key);
    };
    let observer = |event, kind| binding.record_event(event, kind);
    storage_sqlite::SqliteAccountStorage::open_encrypted_with_diagnostic_observer(
        path,
        key,
        Some(&observer),
    )
}

pub(crate) struct Operation {
    binding: Binding,
    previous: Option<Binding>,
    returned: bool,
    _same_thread: PhantomData<Rc<()>>,
}

impl Operation {
    pub(crate) fn returned(mut self) {
        self.binding.record(Phase::AccountState, 2);
        self.returned = true;
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        if !self.returned {
            self.binding.record(
                Phase::AccountState,
                if std::thread::panicking() { 4 } else { 3 },
            );
        }
        self.binding.witness.active.store(false, Ordering::Release);
        CURRENT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}

pub(crate) struct Boundary {
    binding: Option<Binding>,
    phase: Phase,
    returned: bool,
    _same_thread: PhantomData<Rc<()>>,
}

impl Boundary {
    pub(crate) fn enter(witness: Option<&Arc<Witness>>, phase: Phase) -> Self {
        let binding = witness.and_then(|witness| {
            CURRENT.with(|current| {
                current
                    .borrow()
                    .as_ref()
                    .filter(|binding| Arc::ptr_eq(witness, &binding.witness))
                    .cloned()
            })
        });
        if let Some(binding) = &binding {
            binding.record(phase, 1);
        }
        Self {
            binding,
            phase,
            returned: false,
            _same_thread: PhantomData,
        }
    }

    pub(crate) fn returned(mut self) {
        if let Some(binding) = &self.binding {
            binding.record(self.phase, 2);
        }
        self.returned = true;
    }
}

impl Drop for Boundary {
    fn drop(&mut self) {
        if !self.returned
            && let Some(binding) = &self.binding
        {
            binding.record(self.phase, if std::thread::panicking() { 4 } else { 3 });
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Row {
    operation: u64,
    tid: u64,
    phase: u64,
    kind: u64,
    sequence: u64,
    elapsed_us: u64,
}

pub(crate) struct Snapshot {
    pid: u64,
    tid: u64,
    claims: u64,
    active: bool,
    faults: u64,
    start_us: u64,
    end_us: u64,
    len: usize,
    rows: [Row; CAPACITY],
    #[cfg(feature = "test-policy-overrides")]
    migration: Option<MigrationRow>,
    #[cfg(feature = "test-policy-overrides")]
    migration_publication: u64,
}

impl Snapshot {
    pub(crate) fn print(&self) {
        eprintln!(
            "account_state_witness fixture=1 pid={} tid={} claims={} active={} faults={} attributed={} snapshot_start_us={} snapshot_end_us={} rows={}",
            self.pid,
            self.tid,
            self.claims,
            u8::from(self.active),
            self.faults,
            u8::from(self.faults == 0 && self.active),
            self.start_us,
            self.end_us,
            self.len
        );
        #[cfg(feature = "test-policy-overrides")]
        if let Some(cursor) = self.migration {
            eprintln!(
                "account_state_witness migration_cursor present=1 publication={} op={} tid={} version={} step={} kind={} offset_us={}",
                cursor.publication,
                cursor.operation,
                cursor.tid,
                cursor.version,
                cursor.step,
                cursor.kind,
                cursor.elapsed_us
            );
        } else {
            eprintln!(
                "account_state_witness migration_cursor present=0 publication={}",
                self.migration_publication
            );
        }
        for row in &self.rows[..self.len] {
            eprintln!(
                "account_state_witness row op={} tid={} phase={} kind={} seq={} offset_us={}",
                row.operation, row.tid, row.phase, row.kind, row.sequence, row.elapsed_us
            );
        }
    }
}

fn native_identity() -> Option<(u32, u32)> {
    #[cfg(target_os = "linux")]
    {
        let path = std::fs::read_link("/proc/thread-self").ok()?;
        parse_native_identity(&path, std::process::id())
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

fn parse_native_identity(path: &Path, expected_pid: u32) -> Option<(u32, u32)> {
    let text = path.to_str()?;
    if text.len() > 26 {
        return None;
    }
    let mut parts = text.split('/');
    let pid = parts.next()?;
    if parts.next()? != "task" {
        return None;
    }
    let tid = parts.next()?;
    if parts.next().is_some()
        || pid.is_empty()
        || tid.is_empty()
        || !pid.bytes().all(|byte| byte.is_ascii_digit())
        || !tid.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let pid: u32 = pid.parse().ok()?;
    let tid: u32 = tid.parse().ok()?;
    (pid == expected_pid && pid != 0 && tid != 0).then_some((pid, tid))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn armed() -> Arc<Witness> {
        let witness = Arc::new(Witness::new());
        witness.arm(Instant::now());
        witness
    }

    #[test]
    fn active_chain_and_completed_operation_are_distinguished() {
        let witness = armed();
        let worker_witness = witness.clone();
        let (reached, observe) = std::sync::mpsc::channel();
        let (release, proceed) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let operation = worker_witness.begin_with_identity(Some((1, 2))).unwrap();
            let boundary = Boundary::enter(Some(&worker_witness), Phase::ReadyLock);
            reached.send(()).unwrap();
            proceed
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            boundary.returned();
            operation.returned();
        });
        observe
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let active = witness.snapshot();
        release.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!((active.faults, active.active, active.len), (0, true, 2));
        assert_eq!(
            (
                active.rows[1].operation,
                active.rows[1].tid,
                active.rows[1].phase
            ),
            (1, 2, 3)
        );
        let completed = witness.snapshot();
        assert_eq!(
            (completed.faults, completed.active, completed.len),
            (0, false, 4)
        );
    }

    #[test]
    fn missing_identity_and_second_operation_refuse_attribution() {
        assert_ne!(armed().snapshot().faults & MISSING, 0);
        let missing = armed();
        assert!(missing.begin_with_identity(None).is_none());
        assert_ne!(missing.snapshot().faults & IDENTITY, 0);
        let witness = armed();
        let operation = witness.begin_with_identity(Some((1, 2))).unwrap();
        assert!(witness.begin_with_identity(Some((1, 3))).is_none());
        assert_eq!(
            witness.snapshot().faults & (MULTIPLE | SECOND_WRITER),
            MULTIPLE | SECOND_WRITER
        );
        drop(operation);
    }

    #[test]
    fn partial_slot_is_not_exposed_and_bad_committed_sequence_refuses() {
        let witness = armed();
        let operation = witness.begin_with_identity(Some((1, 2))).unwrap();
        witness.publication.store(3, Ordering::Release);
        witness.slots[1].phase.store(3, Ordering::Relaxed);
        let partial = witness.snapshot();
        assert_eq!(partial.len, 1);
        assert_ne!(partial.faults & INCONSISTENT, 0);
        let boundary = Boundary::enter(Some(&witness), Phase::ReadyLock);
        witness.slots[1].sequence.store(99, Ordering::Release);
        assert_ne!(witness.snapshot().faults & INCONSISTENT, 0);
        drop(boundary);
        drop(operation);
    }

    #[test]
    fn overflow_keeps_the_fixed_buffer_and_refuses_attribution() {
        let witness = armed();
        let operation = witness.begin_with_identity(Some((1, 2))).unwrap();
        for _ in 0..CAPACITY {
            Boundary::enter(Some(&witness), Phase::AccountLookup).returned();
        }
        let snapshot = witness.snapshot();
        assert_eq!(snapshot.len, CAPACITY);
        assert_ne!(snapshot.faults & OVERFLOW, 0);
        drop(operation);
    }

    #[test]
    fn unwind_restores_prior_context_and_does_not_record_other_app() {
        let outer = armed();
        let inner = armed();
        let operation = outer.begin_with_identity(Some((1, 2))).unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _inner_operation = inner.begin_with_identity(Some((1, 2))).unwrap();
                Boundary::enter(Some(&outer), Phase::AccountLookup).returned();
                let _boundary = Boundary::enter(Some(&inner), Phase::ReadyLock);
                panic!("recorder unwind fixture");
            }))
            .is_err()
        );
        Boundary::enter(Some(&outer), Phase::AccountLookup).returned();
        operation.returned();
        assert_eq!((outer.snapshot().faults, outer.snapshot().len), (0, 4));
        let snapshot = inner.snapshot();
        assert_eq!(
            (snapshot.faults, snapshot.active, snapshot.len),
            (0, false, 4)
        );
        assert_eq!((snapshot.rows[2].kind, snapshot.rows[3].kind), (4, 4));
    }

    #[test]
    fn native_identity_requires_exact_numeric_process_and_thread() {
        assert_eq!(
            parse_native_identity(Path::new("123/task/456"), 123),
            Some((123, 456))
        );
        assert_eq!(
            parse_native_identity(Path::new("4294967295/task/4294967295"), u32::MAX),
            Some((u32::MAX, u32::MAX))
        );
        for path in [
            "123/task/0",
            "124/task/456",
            "123/task/456/7",
            "123/task/key",
            "/123/task/456",
        ] {
            assert_eq!(parse_native_identity(Path::new(path), 123), None);
        }
    }

    #[cfg(feature = "test-policy-overrides")]
    #[test]
    fn storage_subphases_real_open_error_unwind_and_foreign_thread() {
        use storage_sqlite::{
            DiagnosticOpenEvent, DiagnosticOpenPhase, SqlCipherKey, SqliteAccountStorage,
        };
        let directory = tempfile::tempdir().unwrap();
        let key = SqlCipherKey::new("bounded storage witness test key").unwrap();

        // The real shared implementation completes all eight numeric stages.
        let success = armed();
        let operation = success.begin_with_identity(Some((1, 2))).unwrap();
        let storage =
            open_encrypted(Some(&success), &directory.path().join("success.db"), &key).unwrap();
        operation.returned();
        let snapshot = success.snapshot();
        assert_eq!(
            (snapshot.faults, snapshot.active, snapshot.len),
            (0, false, 18)
        );
        for index in 0..8 {
            assert_eq!(
                (
                    snapshot.rows[1 + index * 2].phase,
                    snapshot.rows[1 + index * 2].kind
                ),
                (12 + index as u64, 1)
            );
            assert_eq!(
                (
                    snapshot.rows[2 + index * 2].phase,
                    snapshot.rows[2 + index * 2].kind
                ),
                (12 + index as u64, 2)
            );
        }
        drop(storage);

        // A real filesystem failure exits PrivateFiles with kind 3.
        let blocked_parent = directory.path().join("file-not-directory");
        std::fs::write(&blocked_parent, b"occupied").unwrap();
        let failure = armed();
        let operation = failure.begin_with_identity(Some((1, 2))).unwrap();
        assert!(open_encrypted(Some(&failure), &blocked_parent.join("account.db"), &key).is_err());
        operation.returned();
        let snapshot = failure.snapshot();
        assert_eq!(
            (snapshot.faults, snapshot.active, snapshot.len),
            (0, false, 4)
        );
        assert_eq!(
            (
                snapshot.rows[1].phase,
                snapshot.rows[1].kind,
                snapshot.rows[2].kind
            ),
            (12, 1, 3)
        );

        // Panic after actual stage entry exercises storage guard/operation unwind.
        let unwind = armed();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let operation = unwind.begin_with_identity(Some((1, 2))).unwrap();
                let binding = operation.binding.clone();
                let observer = |event, kind| {
                    binding.record_event(event, kind);
                    if matches!(
                        event,
                        DiagnosticOpenEvent::Storage(DiagnosticOpenPhase::PrivateFiles)
                    ) && kind == 1
                    {
                        panic!("storage stage unwind fixture");
                    }
                };
                let _ = SqliteAccountStorage::open_encrypted_with_diagnostic_observer(
                    directory.path().join("unwind.db"),
                    &key,
                    Some(&observer),
                );
                operation.returned();
            }))
            .is_err()
        );
        let snapshot = unwind.snapshot();
        assert_eq!(
            (snapshot.faults, snapshot.active, snapshot.len),
            (0, false, 4)
        );
        assert_eq!(
            (
                snapshot.rows[1].phase,
                snapshot.rows[2].kind,
                snapshot.rows[3].kind
            ),
            (12, 4, 4)
        );
        assert!(CURRENT.with(|current| current.borrow().is_none()));

        // Deliberate hostile use of a cloned binding cannot publish foreign rows.
        // The public borrowed observer itself is not Send; this bypass tests its guard.
        let foreign = armed();
        let operation = foreign.begin_with_identity(Some((1, 2))).unwrap();
        let binding = operation.binding.clone();
        let foreign_binding = binding.clone();
        std::thread::spawn(move || {
            foreign_binding.record_event(DiagnosticOpenEvent::Storage(DiagnosticOpenPhase::Wal), 1)
        })
        .join()
        .unwrap();
        let other = armed();
        let other_operation = other.begin_with_identity(Some((1, 2))).unwrap();
        binding.record_event(DiagnosticOpenEvent::Storage(DiagnosticOpenPhase::Wal), 1);
        other_operation.returned();
        assert_eq!((other.snapshot().faults, other.snapshot().len), (0, 2));
        operation.returned();
        let snapshot = foreign.snapshot();
        assert_eq!((snapshot.len, snapshot.active), (2, false));
        assert_ne!(snapshot.faults & FOREIGN_STORAGE_CALLBACK, 0);
    }
    #[cfg(feature = "test-policy-overrides")]
    #[test]
    fn migration_cursor_real_steps_error_unwind_and_foreign_binding() {
        use storage_sqlite::{
            DiagnosticMigrationStep as Step, DiagnosticOpenEvent as Event, SqlCipherKey,
            SqliteAccountStorage,
        };
        let directory = tempfile::tempdir().unwrap();
        let passphrase = "bounded migration cursor test key";
        let key = SqlCipherKey::new(passphrase).unwrap();
        let path = directory.path().join("migration.db");

        let success = armed();
        let operation = success.begin_with_identity(Some((1, 2))).unwrap();
        let binding = operation.binding.clone();
        let saw_current_apply = std::cell::Cell::new(false);
        let migration_callbacks = std::cell::Cell::new(0u64);
        let observer = |event, kind| {
            binding.record_event(event, kind);
            if matches!(event, Event::Migration { .. }) {
                migration_callbacks.set(migration_callbacks.get() + 1);
            }
            if matches!(
                event,
                Event::Migration {
                    version: 1,
                    step: Step::Apply
                }
            ) && kind == 1
            {
                let snapshot = success.snapshot();
                let cursor = snapshot.migration.unwrap();
                assert_eq!((snapshot.faults, snapshot.active), (0, true));
                assert_eq!(
                    (
                        cursor.operation,
                        cursor.tid,
                        cursor.version,
                        cursor.step,
                        cursor.kind
                    ),
                    (1, 2, 1, 7, 1)
                );
                saw_current_apply.set(true);
            }
        };
        let storage = SqliteAccountStorage::open_encrypted_with_diagnostic_observer(
            &path,
            &key,
            Some(&observer),
        )
        .unwrap();
        assert_eq!(storage.migration_summary().0, 1);
        assert!(saw_current_apply.get());
        operation.returned();
        let snapshot = success.snapshot();
        let cursor = snapshot.migration.unwrap();
        assert_eq!(
            (snapshot.faults, snapshot.active, snapshot.len),
            (0, false, 18)
        );
        assert_eq!((cursor.version, cursor.step, cursor.kind), (1, 9, 2));
        assert_eq!(migration_callbacks.get(), 8);
        assert_eq!(cursor.publication, migration_callbacks.get() * 2);
        drop(storage);

        // A genuine future ledger row reaches the actual admission refusal.
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.pragma_update(None, "key", passphrase).unwrap();
        connection.execute("INSERT INTO cgka_schema_migrations(version,name,applied_at_unix_seconds) VALUES (2,'diagnostic_future_fixture',0)", []).unwrap();
        drop(connection);
        let failed = armed();
        let operation = failed.begin_with_identity(Some((1, 2))).unwrap();
        assert!(open_encrypted(Some(&failed), &path, &key).is_err());
        operation.returned();
        let snapshot = failed.snapshot();
        let cursor = snapshot.migration.unwrap();
        assert_eq!(
            (snapshot.faults, snapshot.active, snapshot.len),
            (0, false, 18)
        );
        assert_eq!((cursor.version, cursor.step, cursor.kind), (1, 7, 3));

        // Panic after actual begin/entry publishes unwind, never success.
        let unwind = armed();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let operation = unwind.begin_with_identity(Some((1, 2))).unwrap();
                let binding = operation.binding.clone();
                let observer = |event, kind| {
                    binding.record_event(event, kind);
                    if matches!(
                        event,
                        Event::Migration {
                            version: 1,
                            step: Step::Apply
                        }
                    ) && kind == 1
                    {
                        panic!("migration cursor unwind fixture");
                    }
                };
                let _ = SqliteAccountStorage::open_encrypted_with_diagnostic_observer(
                    directory.path().join("migration-unwind.db"),
                    &key,
                    Some(&observer),
                );
                operation.returned();
            }))
            .is_err()
        );
        let snapshot = unwind.snapshot();
        let cursor = snapshot.migration.unwrap();
        assert_eq!(
            (snapshot.faults, snapshot.active, snapshot.len),
            (0, false, 18)
        );
        assert_eq!((cursor.version, cursor.step, cursor.kind), (1, 7, 4));
        assert!(CURRENT.with(|current| current.borrow().is_none()));

        let foreign = armed();
        let operation = foreign.begin_with_identity(Some((1, 2))).unwrap();
        let binding = operation.binding.clone();
        let foreign_binding = binding.clone();
        std::thread::spawn(move || {
            foreign_binding.record_event(
                Event::Migration {
                    version: 1,
                    step: Step::Begin,
                },
                1,
            )
        })
        .join()
        .unwrap();
        let other = armed();
        let other_operation = other.begin_with_identity(Some((1, 2))).unwrap();
        binding.record_event(
            Event::Migration {
                version: 1,
                step: Step::Begin,
            },
            1,
        );
        other_operation.returned();
        assert_eq!(
            (
                other.snapshot().faults,
                other.migration.publication.load(Ordering::SeqCst)
            ),
            (0, 0)
        );
        operation.returned();
        assert_ne!(foreign.snapshot().faults & FOREIGN_STORAGE_CALLBACK, 0);
        assert_eq!(foreign.migration.publication.load(Ordering::SeqCst), 0);

        let invalid = armed();
        let operation = invalid.begin_with_identity(Some((1, 2))).unwrap();
        operation.binding.record_event(
            Event::Migration {
                version: 102,
                step: Step::Begin,
            },
            1,
        );
        operation.binding.record_migration(1, 10, 1);
        operation.binding.record_migration(1, 1, 1);
        operation.binding.record_migration(0, 5, 1);
        operation.binding.record_migration(1, 6, 0);
        assert_eq!(invalid.migration.publication.load(Ordering::SeqCst), 0);
        assert_ne!(invalid.snapshot().faults & MIGRATION_CURSOR_FAULT, 0);
        operation.returned();

        let partial = armed();
        let operation = partial.begin_with_identity(Some((1, 2))).unwrap();
        partial.migration.publication.store(1, Ordering::SeqCst);
        partial
            .migration
            .value
            .store(1 | (6 << 7) | (1 << 11), Ordering::SeqCst);
        let snapshot = partial.snapshot();
        assert!(snapshot.migration.is_none());
        assert_ne!(snapshot.faults & MIGRATION_CURSOR_FAULT, 0);
        operation.returned();

        let malformed = armed();
        let operation = malformed.begin_with_identity(Some((1, 2))).unwrap();
        malformed.migration.operation.store(1, Ordering::SeqCst);
        malformed.migration.tid.store(2, Ordering::SeqCst);
        malformed
            .migration
            .value
            .store(1 | (6 << 7) | (5 << 11), Ordering::SeqCst);
        malformed.migration.publication.store(2, Ordering::SeqCst);
        let snapshot = malformed.snapshot();
        assert!(snapshot.migration.is_none());
        assert_ne!(snapshot.faults & MIGRATION_CURSOR_FAULT, 0);
        operation.returned();

        let overflow = armed();
        let operation = overflow.begin_with_identity(Some((1, 2))).unwrap();
        overflow
            .migration
            .publication
            .store(u64::MAX - 1, Ordering::SeqCst);
        operation.binding.record_event(
            Event::Migration {
                version: 1,
                step: Step::Begin,
            },
            1,
        );
        assert_eq!(
            overflow.migration.publication.load(Ordering::SeqCst),
            u64::MAX - 1
        );
        assert_ne!(overflow.snapshot().faults & MIGRATION_CURSOR_FAULT, 0);
        operation.returned();
    }
}
