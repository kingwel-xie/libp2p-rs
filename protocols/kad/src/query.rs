// Copyright 2019 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use std::collections::BTreeMap;
use std::{time::Instant, time::Duration, num::NonZeroUsize};
use futures::channel::mpsc;
use futures::{StreamExt, SinkExt};

use async_std::task;

use libp2prs_core::PeerId;

use crate::{ALPHA_VALUE, K_VALUE, BETA_VALUE, KadError, record};
use crate::kbucket::{Key, KeyBytes, Distance};

use crate::protocol::{ProtocolEvent, KadPeer, KadConnectionType};
use crate::kad::{MessengerManager, KadPoster};

type Result<T> = std::result::Result<T, KadError>;

/// A `QueryPool` provides an aggregate state machine for driving `Query`s to completion.
///
/// Internally, a `Query` is in turn driven by an underlying `QueryPeerIter`
/// that determines the peer selection strategy, i.e. the order in which the
/// peers involved in the query should be contacted.
pub struct QueryPool<TInner> {
    next_id: usize,
    config: QueryConfig,
    p: Vec<TInner>
}


impl<TInner> QueryPool<TInner> {
    /// Creates a new `QueryPool` with the given configuration.
    pub fn new(config: QueryConfig) -> Self {
        QueryPool {
            next_id: 0,
            config,
            p: vec!()
        }
    }

    /// Adds a query to the pool that iterates towards the closest peers to the target.
    pub fn add_iter_closest<T, I>(&mut self, _target: T, _peers: I, _inner: TInner) -> QueryId
    where
        T: Into<KeyBytes> + Clone,
        I: IntoIterator<Item = Key<PeerId>>
    {
        let id = self.next_query_id();
        id
    }

    fn next_query_id(&mut self) -> QueryId {
        let id = QueryId(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

}

/// Unique identifier for an active query.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct QueryId(usize);

/// The configuration for queries in a `QueryPool`.
#[derive(Debug, Clone)]
pub struct QueryConfig {
    /// Timeout of a single query.
    ///
    /// See [`crate::behaviour::KademliaConfig::set_query_timeout`] for details.
    pub timeout: Duration,

    /// The replication factor to use.
    ///
    /// See [`crate::behaviour::KademliaConfig::set_replication_factor`] for details.
    pub replication_factor: NonZeroUsize,

    /// Allowed level of parallelism for iterative queries.
    ///
    /// See [`crate::behaviour::KademliaConfig::set_parallelism`] for details.
    pub parallelism: NonZeroUsize,

    /// The number of peers closest to a target that must have responded for a query path to terminate.
    pub beta_value: NonZeroUsize,

    /// Whether to use disjoint paths on iterative lookups.
    ///
    /// See [`crate::behaviour::KademliaConfig::disjoint_query_paths`] for details.
    pub disjoint_query_paths: bool,
}

impl Default for QueryConfig {
    fn default() -> Self {
        QueryConfig {
            timeout: Duration::from_secs(60),
            replication_factor: K_VALUE,
            parallelism: ALPHA_VALUE,
            beta_value: BETA_VALUE,
            disjoint_query_paths: false,
        }
    }
}

struct QueryJob {
    key: record::Key,
    qt: QueryType,
    messengers: MessengerManager,
    peer: PeerId,
    tx: mpsc::Sender<QueryUpdate>,
}

impl QueryJob
{
    async fn execute(self) -> Result<()> {
        let mut me = self;
        let startup = Instant::now();

        let mut messenger = me.messengers.get_messenger(&me.peer).await?;
        let peer = me.peer.clone();
        match me.qt {
            QueryType::GetClosestPeers | QueryType::FindPeer => {
                let closer = messenger.send_find_node(me.key).await?;
                let duration = startup.elapsed();
                let _ = me.tx.send(QueryUpdate::Queried { source: peer, closer, provider: None, record: None, duration }).await;
            }
            QueryType::GetProviders {..} => {
                let (closer, provider) = messenger.send_get_providers(me.key).await?;
                let duration = startup.elapsed();
                let _ = me.tx.send(QueryUpdate::Queried { source: peer, closer, provider: Some(provider), record: None, duration }).await;
            }
            QueryType::GetRecord {..} => {
                let (closer, record) = messenger.send_get_value(me.key).await?;
                let duration = startup.elapsed();
                let _ = me.tx.send(QueryUpdate::Queried { source: peer, closer, provider: None, record, duration }).await;
            }
            _ => {
                panic!("shouldn't happen");
            }
        }

        // try to put messenger into cache
        me.messengers.put_messenger(messenger).await;

        Ok(())
    }
}

/// Representation of a peer in the context of a iterator.
#[derive(Debug, Clone)]
struct PeerWithState {
    peer: KadPeer,
    state: PeerState
}

/// The state of a single `PeerWithState`.
#[derive(Debug, Copy, Clone, PartialEq)]
enum PeerState {
    /// The peer has not yet been contacted.
    ///
    /// This is the starting state for every peer.
    NotContacted,

    /// The query has been started, waiting for a result from the peer.
    Waiting,

    /// A result was not delivered for the peer within the configured timeout.
    Unreachable,

    /// A successful result from the peer has been delivered.
    Succeeded,
}

impl PeerWithState {
    fn new(peer: KadPeer) -> Self {
        Self {
            peer,
            state: PeerState::NotContacted
        }
    }
}

pub(crate) struct IterativeQuery<'a> {
    /// The target to be queried.
    key: record::Key,
    /// The Messenger is used to send/receive Kad messages.
    messenger: MessengerManager,
    /// The query type to be executed.
    query_type: QueryType,
    /// The local peer Id of myself.
    local_id: PeerId,
    /// The kad configurations for queries.
    config: &'a QueryConfig,
    /// The seed peers used to start the query.
    seeds: Vec<Key<PeerId>>,
    /// The KadPoster used to post ProtocolEvent to Kad main loop.
    poster: KadPoster
}

pub(crate) enum QueryUpdate {
    Queried {
        source: PeerId,
        closer: Vec<KadPeer>,
        // For GetProvider.
        provider: Option<Vec<KadPeer>>,
        // For GetValue.
        record: Option<record::Record>,
        // How long this query job takes.
        duration: Duration
    },
    Unreachable(PeerId),
    //Timeout,
}

/// A record either received by the given peer or retrieved from the local
/// record store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRecord {
    /// The peer from whom the record was received. `None` if the record was
    /// retrieved from local storage.
    pub peer: Option<PeerId>,
    pub record: record::Record,
}


/// The query result returned by IterativeQuery.
pub(crate) struct QueryResult {
    pub(crate) closest_peers: Option<Vec<KadPeer>>,
    pub(crate) found_peer: Option<KadPeer>,
    pub(crate) providers: Option<Vec<KadPeer>>,
    pub(crate) records: Option<Vec<PeerRecord>>,
}

pub(crate) struct ClosestPeers {
    /// The target key.
    target: Key<record::Key>,
    /// All queried peers sorted by distance.
    closest_peers: BTreeMap<Distance, PeerWithState>,
}

impl ClosestPeers
{
    fn new(key: record::Key) -> Self {
        Self {
            target: Key::new(key),
            closest_peers: BTreeMap::new()
        }
    }

    // helper of calculating key and distance
    fn distance(&self, peer_id: PeerId) -> Distance {
        let key= Key::from(peer_id);
        let distance = key.distance(&self.target);

        distance
    }

    // extend the btree with an vector of peer_id
    fn add_peers(&mut self, peers: Vec<KadPeer>) {
        // Incorporate the reported peers into the btree map.
        for peer in peers {
            let distance = self.distance(peer.node_id.clone());
            self.closest_peers.entry(distance).or_insert(PeerWithState::new(peer));
        }
    }

    // check if the map contains the target already
    fn has_target(&self) -> Option<KadPeer> {
        // distance must be 0
        let distance = self.target.distance(&self.target);
        self.closest_peers.get(&distance).map(|p| p.peer.clone())
    }

    fn peers_in_state(&mut self, state: PeerState, num: usize) -> impl Iterator<Item = &mut PeerWithState> {
        self.closest_peers.values_mut().filter(move |peer| peer.state == state).take(num)
    }

    fn peers_in_states(&self, states: Vec<PeerState>, num: usize) -> impl Iterator<Item = &PeerWithState> {
        self.closest_peers.values().filter(move |peer| states.contains(&peer.state)).take(num)
    }

    fn get_peer_state(&self, peer_id: &PeerId) -> Option<PeerState> {
        let distance = self.distance(peer_id.clone());
        self.closest_peers.get(&distance).map(|peer| peer.state)
    }

    fn set_peer_state(&mut self, peer_id: &PeerId, state: PeerState) {
        let distance = self.distance(peer_id.clone());
        self.closest_peers.get_mut(&distance).map(|peer| peer.state = state);
    }

    fn num_of_state(&self, state: PeerState) -> usize {
        self.closest_peers.values().filter(|p| p.state == state).count()
    }

    fn num_of_states(&self, states: Vec<PeerState>) -> usize {
        self.closest_peers.values().filter(|p| states.contains(&p.state)).count()
    }

    fn is_starved(&self) -> bool {
        // starvation, if no peer to contact and no pending query
        self.num_of_states(vec!(PeerState::NotContacted, PeerState::Waiting)) == 0
    }

    fn can_terminate(&self, beta_value: usize) -> bool {
        let closest = self.closest_peers.values().filter(|peer| {
            peer.state == PeerState::NotContacted ||
                peer.state == PeerState::Waiting ||
                peer.state == PeerState::Succeeded
        }).take(beta_value);

        for peer in closest {
            if peer.state != PeerState::Succeeded {
                return false;
            }
        }
        true
    }
}

/// Information about a running query.
#[derive(Debug, Clone)]
pub enum QueryType {
    /// A query initiated by [`Kademlia::bootstrap`].
    Bootstrap,
    /// A query initiated by [`Kademlia::find_peer`].
    FindPeer,
    /// A query initiated by [`Kademlia::get_closest_peers`].
    GetClosestPeers,
    /// A query initiated by [`Kademlia::get_providers`].
    GetProviders {
        /// How many providers is needed before completing.
        count: usize,
        /// Providers found locally.
        local: Option<Vec<KadPeer>>,
    },
    /// A query initiated by [`Kademlia::get_record`].
    GetRecord {
        /// The quorum needed by get_record.
        quorum_needed: usize,
        /// Records found locally.
        local: Option<Vec<PeerRecord>>,
    }
}

impl<'a> IterativeQuery<'a>
{
    pub(crate) fn new(query_type: QueryType, key: record::Key, messenger: MessengerManager,
                      local_id: PeerId, config: &'a QueryConfig, seeds: Vec<Key<PeerId>>,
                      poster: KadPoster) -> Self {
        Self {
            query_type,
            key,
            messenger,
            local_id,
            config,
            seeds,
            poster,
        }
    }

    pub(crate) fn run<F>(&mut self, f: F)
        where
            F: FnOnce(Result<QueryResult>) + Send + 'static,
    {
        // check for empty seed peers
        if self.seeds.is_empty() {
            log::info!("no seeds, abort running");
            f(Err(KadError::NoKnownPeers));
            return;
        }

        // closest_peers is used to retrieve the closer peers. It is a sorted btree-map, which is
        // indexed by Distance of the peer. The queried 'key' is used to calculate the distance.
        let mut closest_peers = ClosestPeers::new(self.key.clone());
        // prepare the query result
        let mut query_results = QueryResult {
            closest_peers: None,
            found_peer: None,
            providers: None,
            records: None,
        };

        // extract local PeerRecord or Providers from QueryType
        // local items are parts of the final result
        match &mut self.query_type {
            QueryType::GetProviders { count:_, local } => {
                query_results.providers = local.take();
            }
            QueryType::GetRecord { quorum_needed:_, local } => {
                query_results.records = local.take();
            }
            _ => {}
        }

        let qt = self.query_type.clone();
        let key = self.key.clone();
        let messenger = self.messenger.clone();
        let local_id = self.local_id.clone();
        let seeds = self.seeds.clone();
        let alpha_value = self.config.parallelism.get();
        let beta_value = self.config.beta_value.get();
        let k_value = self.config.replication_factor.get();
        let mut poster = self.poster.clone();

        // the channel used to deliver the result of each jobs
        let (mut tx, mut rx) = mpsc::channel(alpha_value);

        // start a task for query
        task::spawn(async move {
            let seeds = seeds.into_iter().map(|k| KadPeer {
                node_id: k.into_preimage(),
                multiaddrs: vec![],
                connection_ty: KadConnectionType::CanConnect
            }).collect();

            // deliver the seeds to kick off the initial query
            let _ = tx.send(QueryUpdate::Queried { source: local_id.clone(), closer: seeds, provider: None, record: None, duration: Duration::from_secs(0) }).await;

            // starting iterative querying for all selected peers...
            loop {
                // note that the first update comes from the initial seeds
                let update = rx.next().await.expect("must");
                // handle update, update the closest_peers, and update the kbuckets for new peer
                // or dead peer detected, update the QueryResult
                // TODO: check the state before setting state??
                match update {
                    QueryUpdate::Queried { source, mut closer, provider, record, duration } => {
                        // incorporate 'closer' into 'clostest_peers', marked as PeerState::NotContacted
                        log::info!("successful query from {:}, closer {:?}, {:?}", source, closer, duration);
                        // note we don't add myself
                        closer.retain(|p| p.node_id != local_id);
                        closest_peers.add_peers(closer);
                        closest_peers.set_peer_state(&source, PeerState::Succeeded);

                        // TODO: signal the k-buckets for new peer found
                        let _ = poster.post(ProtocolEvent::KadPeerFound(source.clone(), true)).await;

                        // handle different query type, check if we are done querying
                        match qt {
                            QueryType::Bootstrap => {
                            }
                            QueryType::GetClosestPeers => {
                            }
                            QueryType::FindPeer => {
                                if let Some(peer) = closest_peers.has_target() {
                                    log::info!("FindPeer: successfully located, {:?}", peer);
                                    query_results.found_peer = Some(peer);
                                    break;
                                }
                            }
                            QueryType::GetProviders { count, .. } => {
                                // update providers
                                if let Some(provider) = provider {
                                    log::trace!("GetProviders: provider found {:?} key={:?}", provider, key);

                                    if !provider.is_empty() {
                                        // append or create the query_results.providers
                                        if let Some(mut old) = query_results.providers.take() {
                                            old.extend(provider);
                                            query_results.providers = Some(old);
                                        } else {
                                            query_results.providers = Some(provider);
                                        }
                                        // check if we have enough providers
                                        if query_results.providers.as_ref().map_or(0, |p|p.len()) >= count {
                                            log::info!("GetProviders: got enough provider for {:?}, limit={}", key, count);
                                            break;
                                        }
                                    }
                                }
                            }
                            QueryType::GetRecord { quorum_needed, .. } => {
                                if let Some(record) = record {
                                    log::trace!("GetRecord: record found {:?} key={:?}", record, key);

                                    let pr = PeerRecord { peer: Some(source), record };
                                    if let Some(mut old) = query_results.records.take() {
                                        old.push(pr);
                                        query_results.records = Some(old);
                                    } else {
                                        query_results.records = Some(vec![pr]);
                                    }

                                    // check if we have enough records
                                    if query_results.records.as_ref().map_or(0, |r|r.len()) > quorum_needed {
                                        log::info!("GetRecord: got enough records for key={:?}", key);
                                        break;
                                    }
                                }
                            }
                        }
                    },
                    QueryUpdate::Unreachable(peer) => {
                        // set to PeerState::Unreachable
                        log::info!("unreachable peer {:?} detected", peer);
                        closest_peers.set_peer_state(&peer, PeerState::Unreachable);
                        // TODO: signal for dead peer detected
                        let _ = poster.post(ProtocolEvent::KadPeerStopped(peer)).await;
                    }
                }

                // starvation, if no peer to contact and no pending query
                if closest_peers.is_starved() {
                    //return true, LookupStarvation, nil
                    log::trace!("query starvation, if no peer to contact and no pending query");
                    break;
                }
                // meet the k_value? meaning lookup completed
                if closest_peers.can_terminate(beta_value) {
                    //return true, LookupCompleted, nil
                    log::trace!("query got enough results, completed");
                    break;
                }

                // calculate the maximum number of queries we could be running
                // Note: NumWaiting will be updated before invoking job.execute()
                let num_jobs = alpha_value.checked_sub(closest_peers.num_of_state(PeerState::Waiting)).unwrap();

                log::info!("iteratively querying, starting {} jobs", num_jobs);

                let peer_iter = closest_peers.peers_in_state(PeerState::NotContacted, num_jobs);
                for peer in peer_iter {
                    //closest_peers.set_peer_state(&peer, PeerState::Waiting);
                    peer.state = PeerState::Waiting;
                    let peer_id = peer.peer.node_id.clone();

                    let job = QueryJob {
                        key: key.clone(),
                        qt: qt.clone(),
                        messengers: messenger.clone(),
                        peer: peer_id.clone(),
                        tx: tx.clone()
                    };

                    let mut tx = tx.clone();
                    let _ = task::spawn(async move {
                        let r = job.execute().await;
                        if r.is_err() {
                            let _ = tx.send(QueryUpdate::Unreachable(peer_id)).await;
                        }
                    });
                }
            }

            // collect the query result
            let wanted_states = vec!(PeerState::NotContacted, PeerState::Waiting, PeerState::Succeeded);
            let peers = closest_peers.peers_in_states(wanted_states, k_value).map(|p|p.peer.clone()).collect::<Vec<_>>();
            if !peers.is_empty() {
                query_results.closest_peers = Some(peers);
            }

            f(Ok(query_results));
        });
    }
}


/// Execution statistics of a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryStats {
    requests: u32,
    success: u32,
    failure: u32,
    start: Option<Instant>,
    end: Option<Instant>
}

impl QueryStats {
    pub fn empty() -> Self {
        QueryStats {
            requests: 0,
            success: 0,
            failure: 0,
            start: None,
            end: None,
        }
    }

    /// Gets the total number of requests initiated by the query.
    pub fn num_requests(&self) -> u32 {
        self.requests
    }

    /// Gets the number of successful requests.
    pub fn num_successes(&self) -> u32 {
        self.success
    }

    /// Gets the number of failed requests.
    pub fn num_failures(&self) -> u32 {
        self.failure
    }

    /// Gets the number of pending requests.
    ///
    /// > **Note**: A query can finish while still having pending
    /// > requests, if the termination conditions are already met.
    pub fn num_pending(&self) -> u32 {
        self.requests - (self.success + self.failure)
    }

    /// Gets the duration of the query.
    ///
    /// If the query has not yet finished, the duration is measured from the
    /// start of the query to the current instant.
    ///
    /// If the query did not yet start (i.e. yield the first peer to contact),
    /// `None` is returned.
    pub fn duration(&self) -> Option<Duration> {
        if let Some(s) = self.start {
            if let Some(e) = self.end {
                Some(e - s)
            } else {
                Some(Instant::now() - s)
            }
        } else {
            None
        }
    }

    /// Merges these stats with the given stats of another query,
    /// e.g. to accumulate statistics from a multi-phase query.
    ///
    /// Counters are merged cumulatively while the instants for
    /// start and end of the queries are taken as the minimum and
    /// maximum, respectively.
    pub fn merge(self, other: QueryStats) -> Self {
        QueryStats {
            requests: self.requests + other.requests,
            success: self.success + other.success,
            failure: self.failure + other.failure,
            start: match (self.start, other.start) {
                (Some(a), Some(b)) => Some(std::cmp::min(a, b)),
                (a, b) => a.or(b)
            },
            end: std::cmp::max(self.end, other.end)
        }
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_closest_peers_distance() {
        let peer = PeerId::random();
        let peer_cloned = peer.clone();

        let mut closest_peers = ClosestPeers::new(peer.into_bytes().into());

        let kad_peer = KadPeer {
            node_id: peer_cloned,
            multiaddrs: vec![],
            connection_ty: KadConnectionType::NotConnected
        };

        assert!(closest_peers.has_target().is_none());
        closest_peers.add_peers(vec!(kad_peer));
        assert!(closest_peers.has_target().is_some());
    }
}