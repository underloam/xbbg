//! Consumer-local dirty-topic receipts over the shared feed image.
//!
//! The SDK path only updates an existing membership slot and its FIFO bit. Image
//! copies and Arrow construction happen on the reader, never on an incoming tick.
//! `valid` tracks source continuity, not field completeness: a valid image may
//! still contain unknown or explicitly cleared (null) fields.

use std::collections::VecDeque;
use std::sync::OnceLock;

use super::*;

pub(super) struct StateDeliveryShared {
    state: Mutex<DirtyState>,
    ready: Notify,
    owner: OnceLock<Weak<Consumer>>,
    image_schema: Mutex<ImageSchema>,
    #[cfg(test)]
    before_snapshot: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_snapshot: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

struct DirtyState {
    members: HashMap<SlabKey, DirtyMember>,
    queue: VecDeque<SlabKey>,
    frozen_images: Vec<Arc<FrozenImage>>,
    frozen_queue: VecDeque<usize>,
    spare_values: Vec<Vec<UpdateField>>,
    live_batch_high_water: usize,
    terminal: Option<BlpError>,
    terminal_in_flight: bool,
    terminal_delivered: bool,
    source_closed: bool,
    receiver_closed: bool,
    freezing: bool,
    frozen: bool,
}

struct DirtyMember {
    feed: Weak<Feed>,
    label: Arc<str>,
    revision: u64,
    acknowledged: Option<u64>,
    invalidated: Option<u64>,
    queued: bool,
    in_flight: bool,
    retired: bool,
    final_image: Option<(u64, ImageRow)>,
    read_plan: Option<Arc<ImageReadPlan>>,
}

struct SelectedMember {
    key: SlabKey,
    feed: Weak<Feed>,
    invalidated: Option<u64>,
    final_image: Option<(u64, ImageRow)>,
}

struct ReceiptKey {
    key: SlabKey,
    acknowledged: u64,
    invalidated: Option<u64>,
}
struct ImageReadPlan {
    source: Arc<FieldLayout>,
    projection: Arc<FieldLayout>,
    mapping: Vec<Option<u16>>,
    capacity: usize,
}

impl ImageReadPlan {
    fn new(source: Arc<FieldLayout>, projection: Arc<FieldLayout>, all_fields: bool) -> Self {
        let requested: HashMap<&str, u16> = projection
            .fields
            .iter()
            .map(|field| (field.name.as_ref(), field.index))
            .collect();
        let mapping: Vec<Option<u16>> = source
            .fields
            .iter()
            .map(|field| requested.get(field.name.as_ref()).copied())
            .collect();
        let capacity = if all_fields {
            source.fields.len()
        } else {
            mapping.iter().filter(|index| index.is_some()).count()
        };
        drop(requested);
        Self {
            source,
            projection,
            mapping,
            capacity,
        }
    }
}

struct ImageReadBuffer<'a> {
    shared: &'a StateDeliveryShared,
    plan: Option<Arc<ImageReadPlan>>,
    values: SmallVec<[UpdateField; 8]>,
}

impl Drop for ImageReadBuffer<'_> {
    fn drop(&mut self) {
        self.shared.recycle_values(std::mem::take(&mut self.values));
    }
}

pub(crate) struct StateDeliveryReceiver {
    shared: Arc<StateDeliveryShared>,
}

pub(crate) struct StateDeliveryReceipt {
    shared: Arc<StateDeliveryShared>,
    result: Option<Result<SubscriptionDelivery, BlpError>>,
    keys: Vec<ReceiptKey>,
    frozen_ids: Vec<usize>,
    terminal: bool,
    finished: bool,
}
struct FrozenImage {
    key: SlabKey,
    revision: u64,
    invalidated: Option<u64>,
    recovery_epoch: bool,
    row: ImageRow,
}

impl FrozenImage {
    fn covered_by(&self, member: &DirtyMember) -> bool {
        member
            .acknowledged
            .is_some_and(|seen| seen >= self.revision)
            && match self.invalidated {
                Some(marker) => member.invalidated != Some(marker),
                None => member
                    .invalidated
                    .is_none_or(|marker| marker > self.revision),
            }
    }
}

enum SelectedDelivery {
    Live(Vec<SelectedMember>),
    Frozen(Vec<(usize, Arc<FrozenImage>)>),
}

fn materialize_delivery<'a>(
    shared: &StateDeliveryShared,
    owner: &Consumer,
    rows: impl ExactSizeIterator<Item = &'a ImageRow> + Clone,
) -> Result<SubscriptionDelivery, BlpError> {
    // Ingestion never touches this consumer-local schema. Snapshot discoveries
    // and promotions persist even when only a narrower topic subset is dirty.
    let mut schema = shared.image_schema.lock();
    schema.observe_rows(rows.clone());
    materialize_images(rows, &mut schema, &owner.zero_as_null, true)
        .map(|data| SubscriptionDelivery {
            kind: SubscriptionDeliveryMode::State,
            data,
            gap_before: None,
        })
        .map_err(|error| match error {
            BlpAsyncError::ConfigError { detail } => BlpError::InvalidArgument {
                detail: format!("Configuration error: {detail}"),
            },
            error => BlpError::Internal {
                detail: error.to_string(),
            },
        })
}

fn image_is_valid(state: &FeedState) -> bool {
    state.last_update.is_some()
        && state.recovery.is_none()
        && !matches!(state.lifecycle, "failed" | "closed")
}

fn image_row(state: &FeedState, topic: &ConsumerTopic) -> ImageRow {
    let valid = image_is_valid(state);
    ImageRow {
        label: topic.projection.topic.clone(),
        last_update: state.last_update,
        live: valid && state.live && state.streams_active != Some(false),
        delayed: state.delayed,
        valid,
        layout: topic.projection.layout(),
        projected: None,
    }
}

/// Prepare reader-owned storage outside the feed lock. A layout change between
/// preparation and capture retries before any value is copied; the locked path
/// only clones selected scalar values into already-reserved slots.
pub(super) fn snapshot_image(
    owner: &Consumer,
    feed: &Feed,
    key: SlabKey,
) -> Option<(u64, ImageRow)> {
    let shared = owner.state_delivery.as_ref()?;
    let mut buffer = shared.read_buffer(key)?;
    loop {
        let (source_layout, projection_layout) = {
            let state = feed.state.lock();
            let topic = state.consumers.iter().find(|topic| {
                topic.active && topic.key == key && std::ptr::eq(topic.owner.as_ptr(), owner)
            })?;
            if !image_is_valid(&state) {
                return Some((state.revision, image_row(&state, topic)));
            }
            (state.layout.as_ref()?.clone(), topic.projection.layout())
        };
        let new_plan = !buffer.plan.as_ref().is_some_and(|plan| {
            Arc::ptr_eq(&plan.source, &source_layout)
                && Arc::ptr_eq(&plan.projection, &projection_layout)
        });
        if new_plan {
            buffer.plan = Some(Arc::new(ImageReadPlan::new(
                source_layout,
                projection_layout,
                owner.request.all_fields,
            )));
        }
        let plan = buffer.plan.as_ref().expect("prepared snapshot layout");
        buffer.values.reserve(plan.capacity);
        #[cfg(test)]
        if let Some(shared) = &owner.state_delivery {
            let hook = shared.before_snapshot.lock().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let (revision, mut row) = {
            let state = feed.state.lock();
            let topic = state.consumers.iter().find(|topic| {
                topic.active && topic.key == key && std::ptr::eq(topic.owner.as_ptr(), owner)
            })?;
            if !image_is_valid(&state) {
                return Some((state.revision, image_row(&state, topic)));
            }
            let same_source = state
                .layout
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &plan.source));
            if !same_source
                || !Arc::ptr_eq(&topic.projection.layout(), &plan.projection)
                || buffer.values.capacity() < plan.capacity
            {
                drop(state);
                continue;
            }
            for field in plan.source.fields.iter() {
                if !owner.request.all_fields && plan.mapping[field.index as usize].is_none() {
                    continue;
                }
                if let Some(value) = state.image[field.index as usize].as_ref() {
                    debug_assert!(buffer.values.len() < buffer.values.capacity());
                    buffer.values.push(UpdateField {
                        index: field.index,
                        value: value.clone(),
                    });
                }
            }
            (state.revision, image_row(&state, topic))
        };
        if new_plan {
            shared.remember_plan(key, plan.clone());
        }
        let projected = project_captured_image(
            &plan.source,
            plan.projection.clone(),
            &plan.mapping,
            std::mem::take(&mut buffer.values),
            &row,
            key as u32,
            owner.request.all_fields,
        );
        if !Arc::ptr_eq(&row.layout, &projected.layout) {
            row.layout = projected.layout.clone();
        }
        row.projected = Some(projected);
        #[cfg(test)]
        {
            let hook = shared.after_snapshot.lock().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        return Some((revision, row));
    }
}

fn project_captured_image(
    source: &FieldLayout,
    projection: Arc<FieldLayout>,
    mapping: &[Option<u16>],
    mut values: SmallVec<[UpdateField; 8]>,
    row: &ImageRow,
    topic_id: u32,
    all_fields: bool,
) -> SubscriptionUpdate {
    let mut fields: Option<Vec<FieldMeta>> = None;
    for value in &mut values {
        let source_index = value.index as usize;
        let source_meta = &source.fields[source_index];
        let index = match mapping[source_index] {
            Some(index) => index,
            None => {
                let fields = fields.get_or_insert_with(|| projection.fields.to_vec());
                let index = fields.len() as u16;
                fields.push(FieldMeta::new(
                    source_meta.name.clone(),
                    index,
                    FieldKind::Unknown,
                ));
                index
            }
        };
        let meta = fields.as_ref().map_or_else(
            || &projection.fields[index as usize],
            |fields| &fields[index as usize],
        );
        let mut kind = meta.kind;
        let mut provisional = meta.provisional;
        if source_meta.provisional {
            kind.seed_hint(&mut provisional, source_meta.kind);
        } else {
            kind.observe(&mut provisional, source_meta.kind);
        }
        kind.observe(&mut provisional, FieldKind::from_value(&value.value));
        if kind != meta.kind || provisional != meta.provisional {
            let fields = fields.get_or_insert_with(|| projection.fields.to_vec());
            fields[index as usize].kind = kind;
            fields[index as usize].provisional = provisional;
        }
        value.index = index;
    }
    if !all_fields {
        values.sort_unstable_by_key(|field| field.index);
    }
    let layout = match fields {
        Some(fields) => Arc::new(FieldLayout::new(
            projection.version.wrapping_add(1).max(1),
            fields,
        )),
        None => projection,
    };
    SubscriptionUpdate {
        timestamp_us: row.last_update.unwrap_or(0),
        topic_id,
        topic: row.label.clone(),
        layout,
        values,
    }
}

pub(super) fn channel(
    on_closed: Arc<dyn Fn() + Send + Sync>,
) -> (
    SubscriptionSender,
    StateDeliveryReceiver,
    Arc<StateDeliveryShared>,
) {
    let shared = Arc::new(StateDeliveryShared {
        state: Mutex::new(DirtyState {
            members: HashMap::new(),
            queue: VecDeque::new(),
            frozen_images: Vec::new(),
            frozen_queue: VecDeque::new(),
            spare_values: Vec::new(),
            live_batch_high_water: 0,
            terminal: None,
            terminal_in_flight: false,
            terminal_delivered: false,
            source_closed: false,
            receiver_closed: false,
            freezing: false,
            frozen: false,
        }),
        ready: Notify::new(),
        owner: OnceLock::new(),
        image_schema: Mutex::new(ImageSchema::default()),
        #[cfg(test)]
        before_snapshot: Mutex::new(None),
        #[cfg(test)]
        after_snapshot: Mutex::new(None),
    });
    let target = shared.clone();
    let sender = SubscriptionSender::callback(Arc::new(move |item| {
        if let Err(error) = item {
            target.fail(error);
        }
    }));
    let target = shared.clone();
    sender.on_closed(Arc::new(move || {
        target.close_source();
        on_closed();
    }));
    (
        sender,
        StateDeliveryReceiver {
            shared: shared.clone(),
        },
        shared,
    )
}

impl StateDeliveryShared {
    pub(super) fn bind(&self, owner: &Arc<Consumer>) {
        *self.image_schema.lock() = ImageSchema::new(owner.image_fields());
        let _ = self.owner.set(Arc::downgrade(owner));
    }
    fn read_buffer(&self, key: SlabKey) -> Option<ImageReadBuffer<'_>> {
        let mut state = self.state.lock();
        let member = state.members.get(&key)?;
        let plan = member.read_plan.clone();
        let values = state.spare_values.pop();
        drop(state);
        Some(ImageReadBuffer {
            shared: self,
            plan,
            values: values.map(SmallVec::from_vec).unwrap_or_default(),
        })
    }

    fn remember_plan(&self, key: SlabKey, plan: Arc<ImageReadPlan>) {
        let mut state = self.state.lock();
        let previous = state
            .members
            .get_mut(&key)
            .filter(|member| !member.retired)
            .and_then(|member| member.read_plan.replace(plan));
        drop(state);
        drop(previous);
    }

    fn recycle_values(&self, mut values: SmallVec<[UpdateField; 8]>) {
        values.clear();
        if !values.spilled() {
            return;
        }
        let values = values.into_vec();
        let mut state = self.state.lock();
        if !state.receiver_closed && state.spare_values.len() < state.live_batch_high_water {
            // Slot storage was reserved when an actual live batch raised the
            // high-water mark; returning a buffer cannot grow this LIFO pool.
            state.spare_values.push(values);
        } else {
            drop(state);
            drop(values);
        }
    }

    pub(super) fn seed_schema(&self, fields: Vec<(Arc<str>, FieldKind, bool)>) {
        self.image_schema.lock().seed(fields);
    }
    #[cfg(test)]
    pub(super) fn before_next_snapshot(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.before_snapshot.lock() = Some(hook);
    }
    #[cfg(test)]
    pub(super) fn after_next_snapshot(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.after_snapshot.lock() = Some(hook);
    }
    #[cfg(test)]
    pub(super) fn spare_buffer_stats(&self) -> (usize, usize) {
        let state = self.state.lock();
        (state.live_batch_high_water, state.spare_values.len())
    }

    pub(super) fn register(&self, key: SlabKey, feed: &Arc<Feed>, revision: u64, label: Arc<str>) {
        let mut state = self.state.lock();
        if state.source_closed || state.receiver_closed || state.freezing || state.frozen {
            return;
        }
        // Re-adding an alias replaces its retired generation, including a
        // cancelled invalidation receipt. Historical generations cannot grow.
        state
            .members
            .retain(|_, member| !member.retired || member.label != label);
        let DirtyState { members, queue, .. } = &mut *state;
        queue.retain(|queued| members.contains_key(queued));
        state.members.insert(
            key,
            DirtyMember {
                feed: Arc::downgrade(feed),
                label,
                revision,
                acknowledged: None,
                invalidated: None,
                queued: true,
                in_flight: false,
                retired: false,
                final_image: None,
                read_plan: None,
            },
        );
        // Reserve only during membership changes. A tick never grows this FIFO.
        let member_count = state.members.len();
        if state.queue.capacity() < member_count {
            let additional = member_count - state.queue.len();
            state.queue.reserve(additional);
        }
        state.queue.push_back(key);
        drop(state);
        self.ready.notify_one();
    }

    pub(super) fn mark(&self, key: SlabKey, revision: u64, invalidated: bool) {
        let mut state = self.state.lock();
        if state.source_closed || state.receiver_closed || state.frozen {
            return;
        }
        let freezing = state.freezing;
        let Some(member) = state.members.get_mut(&key) else {
            return;
        };
        member.revision = member.revision.max(revision);
        // Keep the first unpublished loss: a later event must not overwrite a
        // marker that belongs before an already captured snapshot revision.
        if invalidated && member.invalidated.is_none() {
            member.invalidated = Some(revision);
        }
        if freezing || member.queued || member.in_flight {
            return;
        }
        member.queued = true;
        state.queue.push_back(key);
        drop(state);
        self.ready.notify_one();
    }

    pub(super) fn retire(&self, key: SlabKey, row: ImageRow) {
        let mut state = self.state.lock();
        if state.source_closed || state.receiver_closed || state.frozen {
            return;
        }
        let freezing = state.freezing;
        let Some(member) = state.members.get_mut(&key) else {
            return;
        };
        member.revision = member.revision.saturating_add(1);
        member.invalidated = None;
        member.retired = true;
        member.final_image = Some((member.revision, row));
        if freezing || member.queued || member.in_flight {
            return;
        }
        member.queued = true;
        state.queue.push_back(key);
        drop(state);
        self.ready.notify_one();
    }

    pub(super) fn remove(&self, key: SlabKey) {
        let mut state = self.state.lock();
        state.members.remove(&key);
        // Removal is a slow path. Purging also bounds storage under remove/re-add.
        state.queue.retain(|queued| *queued != key);
    }
    pub(super) fn detach(&self, key: SlabKey) {
        let mut state = self.state.lock();
        if state.frozen || state.freezing {
            return;
        }
        state.members.remove(&key);
        state.queue.retain(|queued| *queued != key);
    }

    fn fail(&self, error: BlpError) {
        let mut state = self.state.lock();
        if state.receiver_closed || state.terminal_delivered || state.terminal_in_flight {
            return;
        }
        if state.terminal.is_none() {
            state.terminal = Some(error);
        }
        state.source_closed = true;
        state.queue.clear();
        state.frozen_queue.clear();
        for member in state.members.values_mut() {
            member.queued = false;
        }
        drop(state);
        self.ready.notify_waiters();
    }

    fn close_source(&self) {
        let mut state = self.state.lock();
        state.source_closed = true;
        let discarded = if !state.frozen && !state.freezing {
            state.queue.clear();
            state.live_batch_high_water = 0;
            Some((
                std::mem::take(&mut state.members),
                std::mem::take(&mut state.spare_values),
            ))
        } else {
            None
        };
        drop(state);
        drop(discarded);
        self.ready.notify_waiters();
    }

    fn finish(&self, keys: &[ReceiptKey], committed: bool) {
        let mut state = self.state.lock();
        let closing = state.frozen || state.freezing;
        let can_queue = !state.receiver_closed
            && !state.terminal_delivered
            && !state.terminal_in_flight
            && state.terminal.is_none()
            && !closing
            && !state.source_closed;
        for receipt in keys {
            let Some(member) = state.members.get_mut(&receipt.key) else {
                continue;
            };
            member.in_flight = false;
            if committed {
                member.acknowledged = Some(
                    member
                        .acknowledged
                        .map_or(receipt.acknowledged, |old| old.max(receipt.acknowledged)),
                );
                if member.invalidated == receipt.invalidated {
                    member.invalidated = None;
                }
            }
            let dirty = !committed
                || member.invalidated.is_some()
                || member
                    .acknowledged
                    .is_none_or(|seen| seen < member.revision);
            if !closing && member.retired && committed && !dirty {
                state.members.remove(&receipt.key);
                continue;
            }
            if can_queue && dirty && !member.queued {
                member.queued = true;
                state.queue.push_back(receipt.key);
            }
        }
        drop(state);
        self.ready.notify_waiters();
    }
    fn finish_frozen(&self, ids: &[usize], committed: bool) {
        let mut state = self.state.lock();
        if !committed
            && state.frozen
            && !state.receiver_closed
            && !state.terminal_delivered
            && !state.terminal_in_flight
            && state.terminal.is_none()
        {
            for &id in ids {
                if id >= state.frozen_images.len() || state.frozen_queue.contains(&id) {
                    continue;
                }
                let before = state
                    .frozen_queue
                    .iter()
                    .position(|queued| *queued > id)
                    .unwrap_or(state.frozen_queue.len());
                state.frozen_queue.insert(before, id);
            }
        }
        drop(state);
        self.ready.notify_waiters();
    }

    fn finish_terminal(&self, error: Option<BlpError>) {
        let mut state = self.state.lock();
        state.terminal_in_flight = false;
        if let Some(error) = error {
            if !state.receiver_closed {
                state.terminal = Some(error);
            }
        } else {
            state.terminal_delivered = true;
            state.queue.clear();
            state.frozen_queue.clear();
        }
        drop(state);
        self.ready.notify_waiters();
    }
}

impl StateDeliveryReceiver {
    pub(crate) async fn recv(&mut self, limit: usize) -> Option<StateDeliveryReceipt> {
        loop {
            let shared = self.shared.clone();
            let ready = shared.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if let Some(receipt) = self.try_recv(limit) {
                return Some(receipt);
            }
            {
                let state = shared.state.lock();
                if state.receiver_closed
                    || state.terminal_delivered
                    || (state.source_closed
                        && !state.freezing
                        && state.queue.is_empty()
                        && state.frozen_queue.is_empty()
                        && state.terminal.is_none()
                        && !state.terminal_in_flight)
                {
                    return None;
                }
            }
            ready.await;
        }
    }

    pub(crate) fn try_recv(&mut self, limit: usize) -> Option<StateDeliveryReceipt> {
        let selected = {
            let mut state = self.shared.state.lock();
            if state.receiver_closed
                || state.freezing
                || state.terminal_in_flight
                || state.terminal_delivered
            {
                return None;
            }
            if let Some(error) = state.terminal.take() {
                state.terminal_in_flight = true;
                return Some(StateDeliveryReceipt {
                    shared: self.shared.clone(),
                    result: Some(Err(error)),
                    keys: Vec::new(),
                    frozen_ids: Vec::new(),
                    terminal: true,
                    finished: false,
                });
            }
            if state.frozen {
                let mut selected = Vec::with_capacity(limit.max(1).min(state.frozen_queue.len()));
                let mut recovery_epoch = None;
                while selected.len() < limit.max(1) {
                    let Some(&id) = state.frozen_queue.front() else {
                        break;
                    };
                    let image = &state.frozen_images[id];
                    let member = state.members.get(&image.key);
                    if member.is_some_and(|member| image.covered_by(member)) {
                        state.frozen_queue.pop_front();
                        continue;
                    }
                    if member.is_some_and(|member| member.in_flight)
                        || recovery_epoch.is_some_and(|epoch| epoch != image.recovery_epoch)
                    {
                        break;
                    }
                    recovery_epoch = Some(image.recovery_epoch);
                    let image = image.clone();
                    state.frozen_queue.pop_front();
                    selected.push((id, image));
                }
                SelectedDelivery::Frozen(selected)
            } else {
                let mut selected = Vec::with_capacity(limit.max(1).min(state.queue.len()));
                while selected.len() < limit.max(1) {
                    let Some(key) = state.queue.pop_front() else {
                        break;
                    };
                    let Some(member) = state.members.get_mut(&key) else {
                        continue;
                    };
                    member.queued = false;
                    if member.in_flight {
                        continue;
                    }
                    member.in_flight = true;
                    selected.push(SelectedMember {
                        key,
                        feed: member.feed.clone(),
                        invalidated: member.invalidated,
                        final_image: member.final_image.clone(),
                    });
                }
                if selected.len() > state.live_batch_high_water {
                    let additional = selected.len() - state.spare_values.len();
                    state.spare_values.reserve(additional);
                    state.live_batch_high_water = selected.len();
                }
                SelectedDelivery::Live(selected)
            }
        };
        let selected = match selected {
            SelectedDelivery::Live(selected) => selected,
            SelectedDelivery::Frozen(selected) => return self.materialize_frozen(selected),
        };
        if selected.is_empty() {
            return None;
        }
        let Some(owner) = self.shared.owner.get().and_then(Weak::upgrade) else {
            self.close();
            return None;
        };
        let mut rows = Vec::with_capacity(selected.len());
        let mut keys = Vec::with_capacity(selected.len());
        for selected in selected {
            let image = selected
                .final_image
                .or_else(|| {
                    selected
                        .feed
                        .upgrade()
                        .and_then(|feed| owner.snapshot_image(&feed, selected.key, false))
                })
                .or_else(|| {
                    self.shared
                        .state
                        .lock()
                        .members
                        .get(&selected.key)
                        .and_then(|member| member.final_image.clone())
                });
            let Some((revision, mut row)) = image else {
                self.shared.remove(selected.key);
                continue;
            };
            let invalidated = self
                .shared
                .state
                .lock()
                .members
                .get(&selected.key)
                .and_then(|member| member.invalidated)
                .filter(|at| *at <= revision)
                .or(selected.invalidated.filter(|at| *at <= revision));
            let acknowledged = if row.valid
                && let Some(at) = invalidated
            {
                if let Some(update) = row.projected.take() {
                    self.shared.recycle_values(update.values);
                }
                row.invalidate();
                revision.min(at)
            } else {
                revision
            };
            keys.push(ReceiptKey {
                key: selected.key,
                acknowledged,
                invalidated,
            });
            rows.push(row);
        }
        if rows.is_empty() {
            return None;
        }
        let result = materialize_delivery(&self.shared, &owner, rows.iter());
        for row in rows {
            if let Some(update) = row.projected {
                self.shared.recycle_values(update.values);
            }
        }
        Some(StateDeliveryReceipt {
            shared: self.shared.clone(),
            result: Some(result),
            keys,
            frozen_ids: Vec::new(),
            terminal: false,
            finished: false,
        })
    }
    fn materialize_frozen(
        &mut self,
        selected: Vec<(usize, Arc<FrozenImage>)>,
    ) -> Option<StateDeliveryReceipt> {
        if selected.is_empty() {
            return None;
        }
        let Some(owner) = self.shared.owner.get().and_then(Weak::upgrade) else {
            self.close();
            return None;
        };
        let result = materialize_delivery(
            &self.shared,
            &owner,
            selected.iter().map(|(_, image)| &image.row),
        );
        Some(StateDeliveryReceipt {
            shared: self.shared.clone(),
            result: Some(result),
            keys: Vec::new(),
            frozen_ids: selected.into_iter().map(|(id, _)| id).collect(),
            terminal: false,
            finished: false,
        })
    }

    /// Freeze only pending memberships. Separate invalidation/current epochs
    /// remain independently leasable until the whole marked drain is published.
    pub(crate) fn prepare_close(&mut self) {
        let pending = {
            let mut state = self.shared.state.lock();
            if state.receiver_closed || state.frozen || state.freezing {
                return;
            }
            state.freezing = true;
            let mut keys: Vec<_> = state.queue.iter().copied().collect();
            let mut in_flight: Vec<_> = state
                .members
                .iter()
                .filter(|(_, member)| member.in_flight && !member.queued)
                .map(|(key, _)| *key)
                .collect();
            in_flight.sort_unstable();
            keys.extend(in_flight);
            keys.into_iter()
                .filter_map(|key| {
                    state
                        .members
                        .get(&key)
                        .map(|member| (key, member.feed.clone(), member.retired))
                })
                .collect::<Vec<_>>()
        };
        let owner = self.shared.owner.get().and_then(Weak::upgrade);
        for (key, feed, retired) in &pending {
            if *retired {
                continue;
            }
            let image = owner.as_ref().and_then(|owner| {
                feed.upgrade()
                    .and_then(|feed| owner.snapshot_image(&feed, *key, false))
            });
            let mut state = self.shared.state.lock();
            let remove = if let Some(member) = state.members.get_mut(key) {
                if member.retired {
                    false
                } else if let Some(image) = image {
                    member.revision = member.revision.max(image.0);
                    member.final_image = Some(image);
                    false
                } else {
                    member.final_image.is_none()
                }
            } else {
                false
            };
            drop(state);
            if remove {
                self.shared.remove(*key);
            }
        }
        let mut snapshots = Vec::with_capacity(pending.len());
        {
            let mut state = self.shared.state.lock();
            for (key, _, _) in &pending {
                if let Some(member) = state.members.get_mut(key) {
                    member.queued = false;
                    if let Some((revision, row)) = member.final_image.take() {
                        snapshots.push((*key, revision, row, member.invalidated));
                    }
                }
            }
            // Capture metadata through these snapshot revisions only. Readers
            // remain blocked by `freezing` while the immutable epoch FIFO builds.
            state.frozen = true;
            state.queue.clear();
        }
        let mut images = Vec::with_capacity(snapshots.len().saturating_mul(2));
        let mut recoveries = Vec::with_capacity(snapshots.len());
        for (key, revision, row, invalidated) in snapshots {
            let invalidated = invalidated.filter(|marker| *marker <= revision);
            if row.valid
                && let Some(marker) = invalidated
            {
                images.push(Arc::new(FrozenImage {
                    key,
                    revision: marker,
                    invalidated: Some(marker),
                    recovery_epoch: false,
                    row: ImageRow {
                        label: row.label.clone(),
                        last_update: row.last_update,
                        live: false,
                        delayed: row.delayed,
                        valid: false,
                        layout: row.layout.clone(),
                        projected: None,
                    },
                }));
                recoveries.push(Arc::new(FrozenImage {
                    key,
                    revision,
                    invalidated: None,
                    recovery_epoch: true,
                    row,
                }));
            } else {
                images.push(Arc::new(FrozenImage {
                    key,
                    revision,
                    invalidated,
                    recovery_epoch: false,
                    row,
                }));
            }
        }
        images.extend(recoveries);
        let queue = (0..images.len()).collect();
        let mut state = self.shared.state.lock();
        state.frozen_images = images;
        state.queue.clear();
        if !state.receiver_closed && !state.terminal_delivered && state.terminal.is_none() {
            state.frozen_queue = queue;
        }
        state.freezing = false;
        state.frozen = true;
        state.source_closed = true;
        drop(state);
        self.shared.ready.notify_waiters();
    }

    pub(crate) fn close(&mut self) {
        let mut state = self.shared.state.lock();
        state.receiver_closed = true;
        state.source_closed = true;
        state.queue.clear();
        state.frozen_queue.clear();
        let members = std::mem::take(&mut state.members);
        let frozen_images = std::mem::take(&mut state.frozen_images);
        let spare_values = std::mem::take(&mut state.spare_values);
        state.live_batch_high_water = 0;
        state.terminal = None;
        drop(state);
        drop(members);
        drop(frozen_images);
        drop(spare_values);
        self.shared.ready.notify_waiters();
        if let Some(owner) = self.shared.owner.get().and_then(Weak::upgrade) {
            owner.stream.close();
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        let state = self.shared.state.lock();
        state.source_closed || state.receiver_closed
    }

    pub(crate) fn len(&self) -> usize {
        let state = self.shared.state.lock();
        if state.frozen {
            state.frozen_queue.len()
        } else {
            state.queue.len()
        }
    }
}

impl Drop for StateDeliveryReceiver {
    fn drop(&mut self) {
        self.close();
    }
}

impl StateDeliveryReceipt {
    fn finish_updates(&self, committed: bool) {
        if self.frozen_ids.is_empty() {
            self.shared.finish(&self.keys, committed);
        } else {
            self.shared.finish_frozen(&self.frozen_ids, committed);
        }
    }

    pub(crate) fn as_ref(&self) -> Result<&SubscriptionDelivery, &BlpError> {
        self.result.as_ref().expect("uncommitted receipt").as_ref()
    }
    pub(crate) fn take_success(&mut self) -> Option<SubscriptionDelivery> {
        if matches!(self.result, Some(Ok(_))) {
            self.result.take().and_then(Result::ok)
        } else {
            None
        }
    }
    pub(crate) fn take_error(&mut self) -> Option<BlpError> {
        if matches!(self.result, Some(Err(_))) {
            self.result.take().and_then(Result::err)
        } else {
            None
        }
    }

    pub(crate) fn restore_error(&mut self, error: BlpError) {
        debug_assert!(self.result.is_none());
        self.result = Some(Err(error));
    }

    pub(crate) fn acknowledge(mut self) {
        if self.terminal {
            self.shared.finish_terminal(None);
        } else {
            self.finish_updates(true);
        }
        self.finished = true;
    }

    pub(crate) fn commit(mut self) -> Result<SubscriptionDelivery, BlpError> {
        let result = self.result.take().expect("uncommitted receipt");
        if self.terminal {
            self.shared.finish_terminal(None);
        } else {
            self.finish_updates(true);
        }
        self.finished = true;
        result
    }
}

impl Drop for StateDeliveryReceipt {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if self.terminal {
            self.shared
                .finish_terminal(self.result.take().and_then(Result::err));
        } else {
            self.finish_updates(false);
        }
    }
}
