// Copyright 2020 Netwarps Ltd.
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

use futures::{channel::mpsc, prelude::*, select};

use futures::channel::mpsc::UnboundedReceiver;
use libp2prs_core::{PeerId, WriteEx};
use libp2prs_runtime::task;
use libp2prs_swarm::protocol_handler::{IProtocolHandler, ProtocolImpl};
use libp2prs_swarm::substream::Substream;
use libp2prs_swarm::Control as SwarmControl;

use std::{
    cmp::{max, Ordering},
    collections::HashSet,
    collections::VecDeque,
    collections::{BTreeSet, HashMap},
    fmt,
    iter::FromIterator,
    net::IpAddr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::StreamExt;
use log::{debug, error, info, trace, warn};
use prost::Message;
use rand::{seq::SliceRandom, thread_rng};
use wasm_timer::{Instant, Interval};

type Result<T> = std::result::Result<T, GossipsubError>;
use crate::backoff::BackoffStorage;
use crate::config::{GossipsubConfig, ValidationMode};
use crate::error::{PublishError, SubscriptionError, ValidationError};
use crate::gossip_promises::GossipPromises;
use crate::mcache::MessageCache;
use crate::peer_score::{PeerScore, PeerScoreParams, PeerScoreThresholds, RejectReason};
use crate::protocol::SIGNING_PREFIX;
use crate::subscription_filter::{AllowAllSubscriptionFilter, TopicSubscriptionFilter};
use crate::time_cache::{DuplicateCache, TimeCache};
use crate::topic::{Hasher, Topic, TopicHash};
use crate::transform::{DataTransform, IdentityTransform};
use crate::types::{
    FastMessageId, GossipsubControlAction, GossipsubMessage, GossipsubSubscription,
    GossipsubSubscriptionAction, MessageAcceptance, MessageId, PeerInfo, RawGossipsubMessage,
};
use crate::types::{GossipsubRpc, PeerKind};
use crate::{rpc_proto, TopicScoreParams};
use std::{cmp::Ordering::Equal, fmt::Debug};


pub struct Gossipsub{
    // Used to open stream
    swarm: Option<SwarmControl>,

    // New peer is connected or peer is dead
    peer_tx: mpsc::UnboundedSender<PeerEvent>,
    peer_rx: mpsc::UnboundedReceiver<PeerEvent>,

    // Used to recv incoming rpc message.
    incoming_tx: mpsc::UnboundedSender<RPC>,
    incoming_rx: mpsc::UnboundedReceiver<RPC>,

    // Used to pub/sub/ls/peers.
    // control_tx: mpsc::UnboundedSender<ControlCommand>,
    // control_rx: mpsc::UnboundedReceiver<ControlCommand>,
    
    //refresh peer scores 
    peer_score_tx: mpsc::UnboundedSender<())>,
    peer_score_rx: mpsc::UnboundedReceiver<())>,

    //process heartbeat
    heartbeat_tx: mpsc::UnboundedSender<())>,
    heartbeat_rx: mpsc::UnboundedReceiver<())>,

    // Connected peer
    connected_peers: HashMap<PeerId, mpsc::UnboundedSender<Arc<Vec<u8>>>>,


    /// Configuration providing gossipsub performance parameters.
    config: GossipsubConfig,

    /// Pools non-urgent control messages between heartbeats.
    control_pool: HashMap<PeerId, Vec<GossipsubControlAction>>,

    /// Information used for publishing messages.
    publish_config: PublishConfig,

    /// An LRU Time cache for storing seen messages (based on their ID). This cache prevents
    /// duplicates from being propagated to the application and on the network.
    duplicate_cache: DuplicateCache<MessageId>,

    /// A map of peers to their protocol kind. This is to identify different kinds of gossipsub
    /// peers.
    peer_protocols: HashMap<PeerId, PeerKind>,

    /// A map of all connected peers - A map of topic hash to a list of gossipsub peer Ids.
    topic_peers: HashMap<TopicHash, BTreeSet<PeerId>>,

    /// A map of all connected peers to their subscribed topics.
    peer_topics: HashMap<PeerId, BTreeSet<TopicHash>>,

    /// A set of all explicit peers. These are peers that remain connected and we unconditionally
    /// forward messages to, outside of the scoring system.
    explicit_peers: HashSet<PeerId>,

    /// A list of peers that have been blacklisted by the user.
    /// Messages are not sent to and are rejected from these peers.
    blacklisted_peers: HashSet<PeerId>,

    /// Overlay network of connected peers - Maps topics to connected gossipsub peers.
    mesh: HashMap<TopicHash, BTreeSet<PeerId>>,

    /// Map of topics to list of peers that we publish to, but don't subscribe to.
    fanout: HashMap<TopicHash, BTreeSet<PeerId>>,

    /// The last publish time for fanout topics.
    fanout_last_pub: HashMap<TopicHash, Instant>,

    ///Storage for backoffs
    backoffs: BackoffStorage,

    /// Message cache for the last few heartbeats.
    mcache: MessageCache,

    /// Heartbeat interval stream.
    heartbeat: Interval,

    /// Number of heartbeats since the beginning of time; this allows us to amortize some resource
    /// clean up -- eg backoff clean up.
    heartbeat_ticks: u64,

    /// We remember all peers we found through peer exchange, since those peers are not considered
    /// as safe as randomly discovered outbound peers. This behaviour diverges from the go
    /// implementation to avoid possible love bombing attacks in PX. When disconnecting peers will
    /// be removed from this list which may result in a true outbound rediscovery.
    px_peers: HashSet<PeerId>,

    /// Set of connected outbound peers (we only consider true outbound peers found through
    /// discovery and not by PX).
    outbound_peers: HashSet<PeerId>,

    /// Stores optional peer score data together with thresholds, decay interval and gossip
    /// promises.
    peer_score: Option<(PeerScore, PeerScoreThresholds, Interval, GossipPromises)>,

    /// Counts the number of `IHAVE` received from each peer since the last heartbeat.
    count_received_ihave: HashMap<PeerId, usize>,

    /// Counts the number of `IWANT` that we sent the each peer since the last heartbeat.
    count_sent_iwant: HashMap<PeerId, usize>,

    /// Short term cache for published messsage ids. This is used for penalizing peers sending
    /// our own messages back if the messages are anonymous or use a random author.
    published_message_ids: DuplicateCache<MessageId>,

    /// Short term cache for fast message ids mapping them to the real message ids
    fast_messsage_id_cache: TimeCache<FastMessageId, MessageId>,

    /// The filter used to handle message subscriptions.
    subscription_filter: F,

    /// A general transformation function that can be applied to data received from the wire before
    /// calculating the message-id and sending to the application. This is designed to allow the
    /// user to implement arbitrary topic-based compression algorithms.
    data_transform: D,

}

impl ProtocolImpl for Gossipsub {
    fn handler(&self) -> IProtocolHandler {
        Box::new(Handler::new(self.incoming_tx.clone(), self.peer_tx.clone(),self.config.protocol_id_prefix(),self.config.max_transmit_size(),self.config.validation_mode(),self.config.support_floodsub()))
    }

    fn start(mut self, swarm: SwarmControl) -> Option<task::TaskHandle<()>> {
        self.swarm = Some(swarm);

        // well, self 'move' explicitly,
        let mut gossipsub = self;
        let task = task::spawn(async move {
            let _ = gossipsub.main_loop().await;
        });

        Some(task)
    }

}

async fn handle_send_message(mut rx: UnboundedReceiver<Arc<Vec<u8>>>, mut writer: Substream) -> Result<()> {
    loop {
        match rx.next().await {
            Some(rpc) => {
                log::trace!("send rpc msg: {:?}", rpc);
                writer.write_one(rpc.as_slice()).await?
            }
            None => {
                log::trace!("peer had been removed from floodsub");
                return Ok(());
            }
        }
    }
}

impl<D, F> Gossipsub<D, F> 
where
    D: DataTransform + Default,
    F: TopicSubscriptionFilter + Default,
{
    /// Creates a [`Gossipsub`] struct given a set of parameters specified via a
    /// [`GossipsubConfig`]. This has no subscription filter and uses no compression.
    pub fn new(
        privacy: MessageAuthenticity,
        config: GossipsubConfig,
    ) -> Result<Self, &'static str> {
        Self::new_with_subscription_filter_and_transform(
            privacy,
            config,
            F::default(),
            D::default(),
        )
    }
}

impl<D, F> Gossipsub<D, F>
where
    D: DataTransform,
    F: TopicSubscriptionFilter,
{
    /// Creates a [`Gossipsub`] struct given a set of parameters specified via a
    /// [`GossipsubConfig`] and a custom subscription filter and data transform.
    pub fn new_with_subscription_filter_and_transform(
        privacy: MessageAuthenticity,
        config: GossipsubConfig,
        subscription_filter: F,
        data_transform: D,
    ) -> Result<Self, &'static str> {
        // Set up the router given the configuration settings.

        // We do not allow configurations where a published message would also be rejected if it
        // were received locally.
        validate_config(&privacy, &config.validation_mode())?;

        // Set up message publishing parameters.
        let (peer_tx, peer_rx) = mpsc::unbounded();
        let (incoming_tx, incoming_rx) = mpsc::unbounded();
        //let (control_tx, control_rx) = mpsc::unbounded();
        //let (cancel_tx, cancel_rx) = mpsc::unbounded();
        let (peer_score_tx, peer_score_rx) = mpsc::unbounded();
        let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded();
        Ok(Gossipsub {
            swarm: None,
            peer_tx,
            peer_rx,
            incoming_tx,
            incoming_rx,
            // control_tx,
            // control_rx,
            peer_score_tx,
            peer_score_rx,
            heartbeat_tx,
            heartbeat_rx,
            connected_peers: HashMap::default(),
            control_pool: HashMap::new(),
            publish_config: privacy.into(),
            duplicate_cache: DuplicateCache::new(config.duplicate_cache_time()),
            fast_messsage_id_cache: TimeCache::new(config.duplicate_cache_time()),
            topic_peers: HashMap::new(),
            peer_topics: HashMap::new(),
            explicit_peers: HashSet::new(),
            blacklisted_peers: HashSet::new(),
            mesh: HashMap::new(),
            fanout: HashMap::new(),
            fanout_last_pub: HashMap::new(),
            backoffs: BackoffStorage::new(
                &config.prune_backoff(),
                config.heartbeat_interval(),
                config.backoff_slack(),
            ),
            mcache: MessageCache::new(config.history_gossip(), config.history_length()),
            heartbeat: Interval::new_at(
                Instant::now() + config.heartbeat_initial_delay(),
                config.heartbeat_interval(),
            ),
            heartbeat_ticks: 0,
            px_peers: HashSet::new(),
            outbound_peers: HashSet::new(),
            peer_score: None,
            count_received_ihave: HashMap::new(),
            count_sent_iwant: HashMap::new(),
            peer_protocols: HashMap::new(),
            published_message_ids: DuplicateCache::new(config.published_message_ids_cache_time()),
            config,
            subscription_filter,
            data_transform,
        })
    }
}

impl<D, F> Gossipsub<D, F>
where
    D: DataTransform,
    F: TopicSubscriptionFilter,
{
    /// Message Loop.
    async fn main_loop(&mut self) -> Result<()> {
        let result = self.next().await;

        // self.drop_all_peers();
        // self.drop_all_topics();

        result
    }
    
    async fn next(&mut self) -> Result<()> {
        loop {
            select! {
                cmd = self.peer_rx.next() => {
                    self.handle_peer_event(cmd);
                }
                rpc = self.incoming_rx.next() => {
                    self.handle_incoming_rpc(rpc);
                }
                // cmd = self.control_rx.next() => {
                //     self.on_control_command(cmd)?;
                // }
                // sub = self.cancel_rx.next() => {
                //     self.un_subscribe(sub);
                // }
                _ = self.peer_score_rx.next() => {
                    self.handle_refresh_scores();
                }
                _ = self.heartbeat_rx.next() => {
                    self.handle_heartbeat();
                }
            }
        }
    }

    /// Handle peer event, include new peer event and peer dead event
    fn handle_peer_event(&mut self, cmd: Option<PeerEvent>) {
        match cmd {
            Some(PeerEvent::NewPeer(rpid)) => {
                log::trace!("new peer {} has connected", rpid);
                self.handle_new_peer(rpid);
            }
            Some(PeerEvent::DeadPeer(rpid)) => {
                log::trace!("peer {} has disconnected", rpid);
                self.handle_remove_peer(rpid);
            }
            None => {
                unreachable!()
            }
        }
    }

    /// Always wait to send message.
    fn handle_new_peer(&mut self, rpid: PeerId) {
        let mut swarm = self.swarm.clone().expect("swarm??");
        let peer_dead_tx = self.peer_tx.clone();
        let (tx, rx) = mpsc::unbounded();

        //let _ = tx.unbounded_send(self.get_hello_packet());

        self.connected_peers.insert(rpid, tx);

        task::spawn(async move {
            let stream = swarm.new_stream(rpid, vec![FLOOD_SUB_ID.into()]).await;

            match stream {
                Ok(stream) => {
                    if handle_send_message(rx, stream).await.is_err() {
                        // write failed
                        let _ = peer_dead_tx.unbounded_send(PeerEvent::DeadPeer(rpid));
                    }
                }
                Err(_) => {
                    // new stream failed
                    let _ = peer_dead_tx.unbounded_send(PeerEvent::DeadPeer(rpid));
                }
            }
        });
    }

    /// If remote peer is dead, remove it from peers and topics.
    fn handle_remove_peer(&mut self, rpid: PeerId) {
        self.connected_peers.remove(&rpid);
        for ps in self.topics.values_mut() {
            ps.remove(&rpid);
        }
    }

    // Handle incoming rpc message received by Handler.
    fn handle_incoming_rpc(&mut self, rpc: Option<RPC>) {

    }

    /// update scores
    fn handle_refresh_scores(&mut self){
        if let Some((peer_score, _, interval, _)) = &mut self.peer_score {
            peer_score.refresh_scores();
        }
    }

    fn handle_heartbeat(&mut self){
        self.heartbeat();
    }

}

impl<D, F> Gossipsub<D, F>
where
    D: DataTransform,
    F: TopicSubscriptionFilter
{
    /// Lists the hashes of the topics we are currently subscribed to.
    pub fn topics(&self) -> impl Iterator<Item = &TopicHash> {
        self.mesh.keys()
    }

    /// Lists all mesh peers for a certain topic hash.
    pub fn mesh_peers(&self, topic_hash: &TopicHash) -> impl Iterator<Item = &PeerId> {
        self.mesh
            .get(topic_hash)
            .into_iter()
            .map(|x| x.iter())
            .flatten()
    }

    /// Lists all mesh peers for all topics.
    pub fn all_mesh_peers(&self) -> impl Iterator<Item = &PeerId> {
        let mut res = BTreeSet::new();
        for peers in self.mesh.values() {
            res.extend(peers);
        }
        res.into_iter()
    }

    /// Lists all known peers and their associated subscribed topics.
    pub fn all_peers(&self) -> impl Iterator<Item = (&PeerId, Vec<&TopicHash>)> {
        self.peer_topics
            .iter()
            .map(|(peer_id, topic_set)| (peer_id, topic_set.iter().collect()))
    }

    /// Lists all known peers and their associated protocol.
    pub fn peer_protocol(&self) -> impl Iterator<Item = (&PeerId, &PeerKind)> {
        self.peer_protocols.iter()
    }

    /// Returns the gossipsub score for a given peer, if one exists.
    pub fn peer_score(&self, peer_id: &PeerId) -> Option<f64> {
        self.peer_score
            .as_ref()
            .map(|(score, ..)| score.score(peer_id))
    }    


    /// Heartbeat function which shifts the memcache and updates the mesh.
    fn heartbeat(&mut self) {
        debug!("Starting heartbeat");

        self.heartbeat_ticks += 1;

        let mut to_graft = HashMap::new();
        let mut to_prune = HashMap::new();
        let mut no_px = HashSet::new();

        // clean up expired backoffs
        self.backoffs.heartbeat();

        // clean up ihave counters
        self.count_sent_iwant.clear();
        self.count_received_ihave.clear();

        // apply iwant penalties
        self.apply_iwant_penalties();

        // check connections to explicit peers
        if self.heartbeat_ticks % self.config.check_explicit_peers_ticks() == 0 {
            for p in self.explicit_peers.clone() {
                self.check_explicit_peer_connection(&p);
            }
        }

        // cache scores throughout the heartbeat
        let mut scores = HashMap::new();
        let peer_score = &self.peer_score;
        let mut score = |p: &PeerId| match peer_score {
            Some((peer_score, ..)) => *scores
                .entry(*p)
                .or_insert_with(|| peer_score.score(p)),
            _ => 0.0,
        };

        // maintain the mesh for each topic
        for (topic_hash, peers) in self.mesh.iter_mut() {
            let explicit_peers = &self.explicit_peers;
            let backoffs = &self.backoffs;
            let topic_peers = &self.topic_peers;
            let outbound_peers = &self.outbound_peers;

            // drop all peers with negative score, without PX
            // if there is at some point a stable retain method for BTreeSet the following can be
            // written more efficiently with retain.
            let to_remove: Vec<_> = peers
                .iter()
                .filter(|&p| {
                    if score(p) < 0.0 {
                        debug!(
                            "HEARTBEAT: Prune peer {:?} with negative score [score = {}, topic = \
                             {}]",
                            p,
                            score(p),
                            topic_hash
                        );

                        let current_topic = to_prune.entry(*p).or_insert_with(Vec::new);
                        current_topic.push(topic_hash.clone());
                        no_px.insert(*p);
                        true
                    } else {
                        false
                    }
                })
                .cloned()
                .collect();
            for peer in to_remove {
                peers.remove(&peer);
            }

            // too little peers - add some
            if peers.len() < self.config.mesh_n_low() {
                debug!(
                    "HEARTBEAT: Mesh low. Topic: {} Contains: {} needs: {}",
                    topic_hash,
                    peers.len(),
                    self.config.mesh_n_low()
                );
                // not enough peers - get mesh_n - current_length more
                let desired_peers = self.config.mesh_n() - peers.len();
                let peer_list = get_random_peers(
                    topic_peers,
                    &self.peer_protocols,
                    topic_hash,
                    desired_peers,
                    |peer| {
                        !peers.contains(peer)
                            && !explicit_peers.contains(peer)
                            && !backoffs.is_backoff_with_slack(topic_hash, peer)
                            && score(peer) >= 0.0
                    },
                );
                for peer in &peer_list {
                    let current_topic = to_graft.entry(*peer).or_insert_with(Vec::new);
                    current_topic.push(topic_hash.clone());
                }
                // update the mesh
                debug!("Updating mesh, new mesh: {:?}", peer_list);
                peers.extend(peer_list);
            }

            // too many peers - remove some
            if peers.len() > self.config.mesh_n_high() {
                debug!(
                    "HEARTBEAT: Mesh high. Topic: {} Contains: {} needs: {}",
                    topic_hash,
                    peers.len(),
                    self.config.mesh_n_high()
                );
                let excess_peer_no = peers.len() - self.config.mesh_n();

                // shuffle the peers and then sort by score ascending beginning with the worst
                let mut rng = thread_rng();
                let mut shuffled = peers.iter().cloned().collect::<Vec<_>>();
                shuffled.shuffle(&mut rng);
                shuffled
                    .sort_by(|p1, p2| score(p1).partial_cmp(&score(p2)).unwrap_or(Ordering::Equal));
                // shuffle everything except the last retain_scores many peers (the best ones)
                shuffled[..peers.len() - self.config.retain_scores()].shuffle(&mut rng);

                // count total number of outbound peers
                let mut outbound = {
                    let outbound_peers = &self.outbound_peers;
                    shuffled
                        .iter()
                        .filter(|p| outbound_peers.contains(*p))
                        .count()
                };

                // remove the first excess_peer_no allowed (by outbound restrictions) peers adding
                // them to to_prune
                let mut removed = 0;
                for peer in shuffled {
                    if removed == excess_peer_no {
                        break;
                    }
                    if self.outbound_peers.contains(&peer) {
                        if outbound <= self.config.mesh_outbound_min() {
                            //do not remove anymore outbound peers
                            continue;
                        } else {
                            //an outbound peer gets removed
                            outbound -= 1;
                        }
                    }

                    //remove the peer
                    peers.remove(&peer);
                    let current_topic = to_prune.entry(peer).or_insert_with(Vec::new);
                    current_topic.push(topic_hash.clone());
                    removed += 1;
                }
            }

            // do we have enough outbound peers?
            if peers.len() >= self.config.mesh_n_low() {
                // count number of outbound peers we have
                let outbound = { peers.iter().filter(|p| outbound_peers.contains(*p)).count() };

                // if we have not enough outbound peers, graft to some new outbound peers
                if outbound < self.config.mesh_outbound_min() {
                    let needed = self.config.mesh_outbound_min() - outbound;
                    let peer_list = get_random_peers(
                        topic_peers,
                        &self.peer_protocols,
                        topic_hash,
                        needed,
                        |peer| {
                            !peers.contains(peer)
                                && !explicit_peers.contains(peer)
                                && !backoffs.is_backoff_with_slack(topic_hash, peer)
                                && score(peer) >= 0.0
                                && outbound_peers.contains(peer)
                        },
                    );
                    for peer in &peer_list {
                        let current_topic = to_graft.entry(*peer).or_insert_with(Vec::new);
                        current_topic.push(topic_hash.clone());
                    }
                    // update the mesh
                    debug!("Updating mesh, new mesh: {:?}", peer_list);
                    peers.extend(peer_list);
                }
            }

            // should we try to improve the mesh with opportunistic grafting?
            if self.heartbeat_ticks % self.config.opportunistic_graft_ticks() == 0
                && peers.len() > 1
                && self.peer_score.is_some()
            {
                if let Some((_, thresholds, _, _)) = &self.peer_score {
                    // Opportunistic grafting works as follows: we check the median score of peers
                    // in the mesh; if this score is below the opportunisticGraftThreshold, we
                    // select a few peers at random with score over the median.
                    // The intention is to (slowly) improve an underperforming mesh by introducing
                    // good scoring peers that may have been gossiping at us. This allows us to
                    // get out of sticky situations where we are stuck with poor peers and also
                    // recover from churn of good peers.

                    // now compute the median peer score in the mesh
                    let mut peers_by_score: Vec<_> = peers.iter().collect();
                    peers_by_score
                        .sort_by(|p1, p2| score(p1).partial_cmp(&score(p2)).unwrap_or(Equal));

                    let middle = peers_by_score.len() / 2;
                    let median = if peers_by_score.len() % 2 == 0 {
                        (score(
                            *peers_by_score.get(middle - 1).expect(
                                "middle < vector length and middle > 0 since peers.len() > 0",
                            ),
                        ) + score(*peers_by_score.get(middle).expect("middle < vector length")))
                            * 0.5
                    } else {
                        score(*peers_by_score.get(middle).expect("middle < vector length"))
                    };

                    // if the median score is below the threshold, select a better peer (if any) and
                    // GRAFT
                    if median < thresholds.opportunistic_graft_threshold {
                        let peer_list = get_random_peers(
                            topic_peers,
                            &self.peer_protocols,
                            topic_hash,
                            self.config.opportunistic_graft_peers(),
                            |peer| {
                                !peers.contains(peer)
                                    && !explicit_peers.contains(peer)
                                    && !backoffs.is_backoff_with_slack(topic_hash, peer)
                                    && score(peer) > median
                            },
                        );
                        for peer in &peer_list {
                            let current_topic = to_graft.entry(*peer).or_insert_with(Vec::new);
                            current_topic.push(topic_hash.clone());
                        }
                        // update the mesh
                        debug!(
                            "Opportunistically graft in topic {} with peers {:?}",
                            topic_hash, peer_list
                        );
                        peers.extend(peer_list);
                    }
                }
            }
        }

        // remove expired fanout topics
        {
            let fanout = &mut self.fanout; // help the borrow checker
            let fanout_ttl = self.config.fanout_ttl();
            self.fanout_last_pub.retain(|topic_hash, last_pub_time| {
                if *last_pub_time + fanout_ttl < Instant::now() {
                    debug!(
                        "HEARTBEAT: Fanout topic removed due to timeout. Topic: {:?}",
                        topic_hash
                    );
                    fanout.remove(&topic_hash);
                    return false;
                }
                true
            });
        }

        // maintain fanout
        // check if our peers are still a part of the topic
        for (topic_hash, peers) in self.fanout.iter_mut() {
            let mut to_remove_peers = Vec::new();
            let publish_threshold = match &self.peer_score {
                Some((_, thresholds, _, _)) => thresholds.publish_threshold,
                _ => 0.0,
            };
            for peer in peers.iter() {
                // is the peer still subscribed to the topic?
                match self.peer_topics.get(peer) {
                    Some(topics) => {
                        if !topics.contains(&topic_hash) || score(peer) < publish_threshold {
                            debug!(
                                "HEARTBEAT: Peer removed from fanout for topic: {:?}",
                                topic_hash
                            );
                            to_remove_peers.push(*peer);
                        }
                    }
                    None => {
                        // remove if the peer has disconnected
                        to_remove_peers.push(*peer);
                    }
                }
            }
            for to_remove in to_remove_peers {
                peers.remove(&to_remove);
            }

            // not enough peers
            if peers.len() < self.config.mesh_n() {
                debug!(
                    "HEARTBEAT: Fanout low. Contains: {:?} needs: {:?}",
                    peers.len(),
                    self.config.mesh_n()
                );
                let needed_peers = self.config.mesh_n() - peers.len();
                let explicit_peers = &self.explicit_peers;
                let new_peers = get_random_peers(
                    &self.topic_peers,
                    &self.peer_protocols,
                    topic_hash,
                    needed_peers,
                    |peer| {
                        !peers.contains(peer)
                            && !explicit_peers.contains(peer)
                            && score(peer) < publish_threshold
                    },
                );
                peers.extend(new_peers);
            }
        }

        if self.peer_score.is_some() {
            trace!("Peer_scores: {:?}", {
                for peer in self.peer_topics.keys() {
                    score(peer);
                }
                scores
            });
            trace!("Mesh message deliveries: {:?}", {
                self.mesh
                    .iter()
                    .map(|(t, peers)| {
                        (
                            t.clone(),
                            peers
                                .iter()
                                .map(|p| {
                                    (
                                        *p,
                                        peer_score
                                            .as_ref()
                                            .expect("peer_score.is_some()")
                                            .0
                                            .mesh_message_deliveries(p, t)
                                            .unwrap_or(0.0),
                                    )
                                })
                                .collect::<HashMap<PeerId, f64>>(),
                        )
                    })
                    .collect::<HashMap<TopicHash, HashMap<PeerId, f64>>>()
            })
        }

        self.emit_gossip();

        // send graft/prunes
        if !to_graft.is_empty() | !to_prune.is_empty() {
            self.send_graft_prune(to_graft, to_prune, no_px);
        }

        // piggyback pooled control messages
        self.flush_control_pool();

        // shift the memcache
        self.mcache.shift();

        debug!("Completed Heartbeat");
    }

    /// Emits gossip - Send IHAVE messages to a random set of gossip peers. This is applied to mesh
    /// and fanout peers
    fn emit_gossip(&mut self) {
        let mut rng = thread_rng();
        for (topic_hash, peers) in self.mesh.iter().chain(self.fanout.iter()) {
            let mut message_ids = self.mcache.get_gossip_message_ids(&topic_hash);
            if message_ids.is_empty() {
                return;
            }

            // if we are emitting more than GossipSubMaxIHaveLength message_ids, truncate the list
            if message_ids.len() > self.config.max_ihave_length() {
                // we do the truncation (with shuffling) per peer below
                debug!(
                    "too many messages for gossip; will truncate IHAVE list ({} messages)",
                    message_ids.len()
                );
            } else {
                // shuffle to emit in random order
                message_ids.shuffle(&mut rng);
            }

            // dynamic number of peers to gossip based on `gossip_factor` with minimum `gossip_lazy`
            let n_map = |m| {
                max(
                    self.config.gossip_lazy(),
                    (self.config.gossip_factor() * m as f64) as usize,
                )
            };
            // get gossip_lazy random peers
            let to_msg_peers = get_random_peers_dynamic(
                &self.topic_peers,
                &self.peer_protocols,
                &topic_hash,
                n_map,
                |peer| {
                    !peers.contains(peer)
                        && !self.explicit_peers.contains(peer)
                        && !self.score_below_threshold(peer, |ts| ts.gossip_threshold).0
                },
            );

            debug!("Gossiping IHAVE to {} peers.", to_msg_peers.len());

            for peer in to_msg_peers {
                let mut peer_message_ids = message_ids.clone();

                if peer_message_ids.len() > self.config.max_ihave_length() {
                    // We do this per peer so that we emit a different set for each peer.
                    // we have enough redundancy in the system that this will significantly increase
                    // the message coverage when we do truncate.
                    peer_message_ids.partial_shuffle(&mut rng, self.config.max_ihave_length());
                    peer_message_ids.truncate(self.config.max_ihave_length());
                }

                // send an IHAVE message
                Self::control_pool_add(
                    &mut self.control_pool,
                    peer,
                    GossipsubControlAction::IHave {
                        topic_hash: topic_hash.clone(),
                        message_ids: peer_message_ids,
                    },
                );
            }
        }
    }




}


/// Helper function to get a subset of random gossipsub peers for a `topic_hash`
/// filtered by the function `f`. The number of peers to get equals the output of `n_map`
/// that gets as input the number of filtered peers.
fn get_random_peers_dynamic(
    topic_peers: &HashMap<TopicHash, BTreeSet<PeerId>>,
    peer_protocols: &HashMap<PeerId, PeerKind>,
    topic_hash: &TopicHash,
    // maps the number of total peers to the number of selected peers
    n_map: impl Fn(usize) -> usize,
    mut f: impl FnMut(&PeerId) -> bool,
) -> BTreeSet<PeerId> {
    let mut gossip_peers = match topic_peers.get(topic_hash) {
        // if they exist, filter the peers by `f`
        Some(peer_list) => peer_list
            .iter()
            .cloned()
            .filter(|p| {
                f(p) && match peer_protocols.get(p) {
                    Some(PeerKind::Gossipsub) => true,
                    Some(PeerKind::Gossipsubv1_1) => true,
                    _ => false,
                }
            })
            .collect(),
        None => Vec::new(),
    };

    // if we have less than needed, return them
    let n = n_map(gossip_peers.len());
    if gossip_peers.len() <= n {
        debug!("RANDOM PEERS: Got {:?} peers", gossip_peers.len());
        return gossip_peers.into_iter().collect();
    }

    // we have more peers than needed, shuffle them and return n of them
    let mut rng = thread_rng();
    gossip_peers.partial_shuffle(&mut rng, n);

    debug!("RANDOM PEERS: Got {:?} peers", n);

    gossip_peers.into_iter().take(n).collect()
}

/// Helper function to get a set of `n` random gossipsub peers for a `topic_hash`
/// filtered by the function `f`.
fn get_random_peers(
    topic_peers: &HashMap<TopicHash, BTreeSet<PeerId>>,
    peer_protocols: &HashMap<PeerId, PeerKind>,
    topic_hash: &TopicHash,
    n: usize,
    f: impl FnMut(&PeerId) -> bool,
) -> BTreeSet<PeerId> {
    get_random_peers_dynamic(topic_peers, peer_protocols, topic_hash, |_| n, f)
}

/// Validates the combination of signing, privacy and message validation to ensure the
/// configuration will not reject published messages.
fn validate_config(authenticity: &MessageAuthenticity, validation_mode: &ValidationMode) -> Result<(), &'static str> {
    match validation_mode {
        ValidationMode::Anonymous => {
            if authenticity.is_signing() {
                return Err("Cannot enable message signing with an Anonymous validation mode. Consider changing either the ValidationMode or MessageAuthenticity");
            }

            if !authenticity.is_anonymous() {
                return Err("Published messages contain an author but incoming messages with an author will be rejected. Consider adjusting the validation or privacy settings in the config");
            }
        }
        ValidationMode::Strict => {
            if !authenticity.is_signing() {
                return Err(
                    "Messages will be
                published unsigned and incoming unsigned messages will be rejected. Consider adjusting
                the validation or privacy settings in the config"
                );
            }
        }
        _ => {}
    }
    Ok(())
}

impl<C: DataTransform, F: TopicSubscriptionFilter> fmt::Debug for Gossipsub<C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gossipsub")
            .field("config", &self.config)
            .field("events", &self.events)
            .field("control_pool", &self.control_pool)
            .field("publish_config", &self.publish_config)
            .field("topic_peers", &self.topic_peers)
            .field("peer_topics", &self.peer_topics)
            .field("mesh", &self.mesh)
            .field("fanout", &self.fanout)
            .field("fanout_last_pub", &self.fanout_last_pub)
            .field("mcache", &self.mcache)
            .field("heartbeat", &self.heartbeat)
            .finish()
    }
}

impl fmt::Debug for PublishConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PublishConfig::Signing { author, .. } => {
                f.write_fmt(format_args!("PublishConfig::Signing({})", author))
            }
            PublishConfig::Author(author) => {
                f.write_fmt(format_args!("PublishConfig::Author({})", author))
            }
            PublishConfig::RandomAuthor => f.write_fmt(format_args!("PublishConfig::RandomAuthor")),
            PublishConfig::Anonymous => f.write_fmt(format_args!("PublishConfig::Anonymous")),
        }
    }
}

/// Determines if published messages should be signed or not.
///
/// Without signing, a number of privacy preserving modes can be selected.
///
/// NOTE: The default validation settings are to require signatures. The [`ValidationMode`]
/// should be updated in the [`GossipsubConfig`] to allow for unsigned messages.
#[derive(Clone)]
pub enum MessageAuthenticity {
    /// Message signing is enabled. The author will be the owner of the key and the sequence number
    /// will be a random number.
    Signed(Keypair),
    /// Message signing is disabled.
    ///
    /// The specified [`PeerId`] will be used as the author of all published messages. The sequence
    /// number will be randomized.
    Author(PeerId),
    /// Message signing is disabled.
    ///
    /// A random [`PeerId`] will be used when publishing each message. The sequence number will be
    /// randomized.
    RandomAuthor,
    /// Message signing is disabled.
    ///
    /// The author of the message and the sequence numbers are excluded from the message.
    ///
    /// NOTE: Excluding these fields may make these messages invalid by other nodes who
    /// enforce validation of these fields. See [`ValidationMode`] in the [`GossipsubConfig`]
    /// for how to customise this for rust-libp2p gossipsub.  A custom `message_id`
    /// function will need to be set to prevent all messages from a peer being filtered
    /// as duplicates.
    Anonymous,
}

impl MessageAuthenticity {
    /// Returns true if signing is enabled.
    pub fn is_signing(&self) -> bool {
        matches!(self, MessageAuthenticity::Signed(_))
    }

    pub fn is_anonymous(&self) -> bool {
        matches!(self, MessageAuthenticity::Anonymous)
    }
}

/// Event that can be emitted by the gossipsub behaviour.
#[derive(Debug)]
pub enum GossipsubEvent {
    /// A message has been received.
    Message {
        /// The peer that forwarded us this message.
        propagation_source: PeerId,
        /// The [`MessageId`] of the message. This should be referenced by the application when
        /// validating a message (if required).
        message_id: MessageId,
        /// The decompressed message itself.
        message: GossipsubMessage,
    },
    /// A remote subscribed to a topic.
    Subscribed {
        /// Remote that has subscribed.
        peer_id: PeerId,
        /// The topic it has subscribed to.
        topic: TopicHash,
    },
    /// A remote unsubscribed from a topic.
    Unsubscribed {
        /// Remote that has unsubscribed.
        peer_id: PeerId,
        /// The topic it has subscribed from.
        topic: TopicHash,
    },
}

/// A data structure for storing configuration for publishing messages. See [`MessageAuthenticity`]
/// for further details.
enum PublishConfig {
    Signing {
        keypair: Keypair,
        author: PeerId,
        inline_key: Option<Vec<u8>>,
    },
    Author(PeerId),
    RandomAuthor,
    Anonymous,
}

impl PublishConfig {
    pub fn get_own_id(&self) -> Option<&PeerId> {
        match self {
            Self::Signing { author, .. } => Some(&author),
            Self::Author(author) => Some(&author),
            _ => None,
        }
    }
}

impl From<MessageAuthenticity> for PublishConfig {
    fn from(authenticity: MessageAuthenticity) -> Self {
        match authenticity {
            MessageAuthenticity::Signed(keypair) => {
                let public_key = keypair.public();
                let key_enc = public_key.clone().into_protobuf_encoding();
                let key = if key_enc.len() <= 42 {
                    // The public key can be inlined in [`rpc_proto::Message::from`], so we don't include it
                    // specifically in the [`rpc_proto::Message::key`] field.
                    None
                } else {
                    // Include the protobuf encoding of the public key in the message.
                    Some(key_enc)
                };

                PublishConfig::Signing {
                    keypair,
                    author: public_key.into_peer_id(),
                    inline_key: key,
                }
            }
            MessageAuthenticity::Author(peer_id) => PublishConfig::Author(peer_id),
            MessageAuthenticity::RandomAuthor => PublishConfig::RandomAuthor,
            MessageAuthenticity::Anonymous => PublishConfig::Anonymous,
        }
    }
}

#[derive(Debug)]
pub enum GossipsubError {
    Io(io::Error),
    Closed,
}

impl error::Error for GossipsubError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            GossipsubError::Io(err) => Some(err),
            GossipsubError::Closed => None,
        }
    }
}

impl Display for GossipsubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result {
        match self {
            GossipsubError::Io(e) => write!(f, "i/o error: {}", e),
            GossipsubError::Closed => f.write_str("gossipsub protocol is closed"),
        }
    }
}

impl From<io::Error> for GossipsubError {
    fn from(e: io::Error) -> Self {
        GossipsubError::Io(e)
    }
}

impl From<mpsc::SendError> for GossipsubError {
    fn from(_: mpsc::SendError) -> Self {
        GossipsubError::Closed
    }
}

impl From<oneshot::Canceled> for GossipsubError {
    fn from(_: oneshot::Canceled) -> Self {
        GossipsubError::Closed
    }
}


