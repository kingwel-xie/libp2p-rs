
use futures::AsyncWriteExt;
use std::{error::Error, fmt, io};

use crate::rpc_proto;
use async_trait::async_trait;
use futures::{channel::mpsc, SinkExt};
use libp2prs_core::upgrade::UpgradeInfo;
use libp2prs_core::{identity::PublicKey, PeerId, ProtocolId, ReadEx};
use libp2prs_swarm::protocol_handler::Notifiee;
use libp2prs_swarm::{
    connection::Connection,
    protocol_handler::{IProtocolHandler, ProtocolHandler},
    substream::Substream,
};

use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
use bytes::BytesMut;
use futures::future;
use futures::prelude::*;
use log::{debug, warn};
use prost::Message as ProtobufMessage;
use std::{borrow::Cow, pin::Pin};
use unsigned_varint::codec;
use crate::topic::TopicHash;

pub(crate) const SIGNING_PREFIX: &[u8] = b"libp2p-pubsub:";

#[derive(Clone)]
pub struct Handler {
    incoming_tx: mpsc::UnboundedSender<RPC>,
    peer_tx: mpsc::UnboundedSender<PeerEvent>,

    /// The Gossipsub protocol id to listen on.
    protocol_ids: Vec<ProtocolId>,
    /// The maximum transmit size for a packet.
    max_transmit_size: usize,
    /// Determines the level of validation to be done on incoming messages.
    validation_mode: ValidationMode,
}

impl Handler {
    pub(crate) fn new(
        incoming_tx: mpsc::UnboundedSender<RPC>, 
        peer_tx: mpsc::UnboundedSender<PeerEvent>,
        id_prefix: Cow<'static, str>,
        max_transmit_size: usize,
        validation_mode: ValidationMode,
        support_floodsub: bool,
    ) -> Self {
        // support version 1.1.0 and 1.0.0 with user-customized prefix
        let mut protocol_ids = vec![
            ProtocolId(format!("/{}/{}", id_prefix, "1.1.0").into_bytes()),
            ProtocolId(format!("/{}/{}", id_prefix, "1.0.0").into_bytes()),
        ];

        // add floodsub support if enabled.
        if support_floodsub {
            protocol_ids.push(ProtocolId(format!("/{}/{}", "floodsub", "1.0.0").into_bytes());
        }

        Handler { 
            incoming_tx, 
            peer_tx,
            protocol_ids,
            max_transmit_size,
            validation_mode,
        }
    }

    /// Verifies a gossipsub message. This returns either a success or failure. All errors
    /// are logged, which prevents error handling in the codec and handler. We simply drop invalid
    /// messages and log warnings, rather than propagating errors through the codec.
    fn verify_signature(message: &rpc_proto::Message) -> bool {
        let from = match message.from.as_ref() {
            Some(v) => v,
            None => {
                debug!("Signature verification failed: No source id given");
                return false;
            }
        };

        let source = match PeerId::from_bytes(&from) {
            Ok(v) => v,
            Err(_) => {
                debug!("Signature verification failed: Invalid Peer Id");
                return false;
            }
        };

        let signature = match message.signature.as_ref() {
            Some(v) => v,
            None => {
                debug!("Signature verification failed: No signature provided");
                return false;
            }
        };

        // If there is a key value in the protobuf, use that key otherwise the key must be
        // obtained from the inlined source peer_id.
        let public_key = match message
            .key
            .as_ref()
            .map(|key| PublicKey::from_protobuf_encoding(&key))
        {
            Some(Ok(key)) => key,
            _ => match PublicKey::from_protobuf_encoding(&source.to_bytes()[2..]) {
                Ok(v) => v,
                Err(_) => {
                    warn!("Signature verification failed: No valid public key supplied");
                    return false;
                }
            },
        };

        // The key must match the peer_id
        if source != public_key.clone().into_peer_id() {
            warn!("Signature verification failed: Public key doesn't match source peer id");
            return false;
        }

        // Construct the signature bytes
        let mut message_sig = message.clone();
        message_sig.signature = None;
        message_sig.key = None;
        let mut buf = Vec::with_capacity(message_sig.encoded_len());
        message_sig
            .encode(&mut buf)
            .expect("Buffer has sufficient capacity");
        let mut signature_bytes = SIGNING_PREFIX.to_vec();
        signature_bytes.extend_from_slice(&buf);
        public_key.verify(&signature_bytes, signature)
    }

}

impl UpgradeInfo for Handler {
    type Info = ProtocolId;

    fn protocol_info(&self) -> Vec<Self::Info> {
        self.protocol_ids.clone()
    }
}

impl Notifiee for Handler {
    fn connected(&mut self, conn: &mut Connection) {
        let peer_id = conn.remote_peer();
        let _ = self.peer_tx.unbounded_send(PeerEvent::NewPeer(peer_id));
    }

    fn disconnected(&mut self, conn: &mut Connection) {
        let peer_id = conn.remote_peer();
        let _ = self.peer_tx.unbounded_send(PeerEvent::DeadPeer(peer_id));
    }
}

#[async_trait]
impl ProtocolHandler for Handler {
    fn box_clone(&self) -> IProtocolHandler {
        Box::new(self.clone())
    }

    async fn handle(&mut self, mut stream: Substream, _info: <Self as UpgradeInfo>::Info) -> Result<(), Box<dyn Error>> {
        log::trace!("Handle stream from {}", stream.remote_peer());
        loop {
            let packet = match stream.read_one(self.max_transmit_size).await {
                Ok(p) => p,
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        stream.close().await?;
                    }
                    return Err(Box::new(e));
                }
            };
            let rpc = rpc_proto::Rpc::decode(&packet[..])?;
            log::trace!("recv rpc msg: {:?}", rpc);

            // Store valid messages.
            let mut messages = Vec::with_capacity(rpc.publish.len());
            // Store any invalid messages.
            let mut invalid_messages = Vec::new();

            for message in rpc.publish.into_iter() {
                // Keep track of the type of invalid message.
                let mut invalid_kind = None;
                let mut verify_signature = false;
                let mut verify_sequence_no = false;
                let mut verify_source = false;

                match self.validation_mode {
                    ValidationMode::Strict => {
                        // Validate everything
                        verify_signature = true;
                        verify_sequence_no = true;
                        verify_source = true;
                    }
                    ValidationMode::Permissive => {
                        // If the fields exist, validate them
                        if message.signature.is_some() {
                            verify_signature = true;
                        }
                        if message.seqno.is_some() {
                            verify_sequence_no = true;
                        }
                        if message.from.is_some() {
                            verify_source = true;
                        }
                    }
                    ValidationMode::Anonymous => {
                        if message.signature.is_some() {
                            warn!("Signature field was non-empty and anonymous validation mode is set");
                            invalid_kind = Some(ValidationError::SignaturePresent);
                        } else if message.seqno.is_some() {
                            warn!("Sequence number was non-empty and anonymous validation mode is set");
                            invalid_kind = Some(ValidationError::SequenceNumberPresent);
                        } else if message.from.is_some() {
                            warn!("Message dropped. Message source was non-empty and anonymous validation mode is set");
                            invalid_kind = Some(ValidationError::MessageSourcePresent);
                        }
                    }
                    ValidationMode::None => {}
                }

                // If the initial validation logic failed, add the message to invalid messages and
                // continue processing the others.
                if let Some(validation_error) = invalid_kind.take() {
                    let message = RawGossipsubMessage {
                        source: None, // don't bother inform the application
                        data: message.data.unwrap_or_default(),
                        sequence_number: None, // don't inform the application
                        topic: TopicHash::from_raw(message.topic),
                        signature: None, // don't inform the application
                        key: message.key,
                        validated: false,
                    };
                    invalid_messages.push((message, validation_error));
                    // proceed to the next message
                    continue;
                }

                // verify message signatures if required
                if verify_signature && !Handler::verify_signature(&message) {
                    warn!("Invalid signature for received message");

                    // Build the invalid message (ignoring further validation of sequence number
                    // and source)
                    let message = RawGossipsubMessage {
                        source: None, // don't bother inform the application
                        data: message.data.unwrap_or_default(),
                        sequence_number: None, // don't inform the application
                        topic: TopicHash::from_raw(message.topic),
                        signature: None, // don't inform the application
                        key: message.key,
                        validated: false,
                    };
                    invalid_messages.push((message, ValidationError::InvalidSignature));
                    // proceed to the next message
                    continue;
                }

                // ensure the sequence number is a u64
                let sequence_number = if verify_sequence_no {
                    if let Some(seq_no) = message.seqno {
                        if seq_no.is_empty() {
                            None
                        } else if seq_no.len() != 8 {
                            debug!(
                                "Invalid sequence number length for received message. SeqNo: {:?} Size: {}",
                                seq_no,
                                seq_no.len()
                            );
                            let message = RawGossipsubMessage {
                                source: None, // don't bother inform the application
                                data: message.data.unwrap_or_default(),
                                sequence_number: None, // don't inform the application
                                topic: TopicHash::from_raw(message.topic),
                                signature: message.signature, // don't inform the application
                                key: message.key,
                                validated: false,
                            };
                            invalid_messages.push((message, ValidationError::InvalidSequenceNumber));
                            // proceed to the next message
                            continue;
                        } else {
                            // valid sequence number
                            Some(BigEndian::read_u64(&seq_no))
                        }
                    } else {
                        // sequence number was not present
                        debug!("Sequence number not present but expected");
                        let message = RawGossipsubMessage {
                            source: None, // don't bother inform the application
                            data: message.data.unwrap_or_default(),
                            sequence_number: None, // don't inform the application
                            topic: TopicHash::from_raw(message.topic),
                            signature: message.signature, // don't inform the application
                            key: message.key,
                            validated: false,
                        };
                        invalid_messages.push((message, ValidationError::EmptySequenceNumber));
                        continue;
                    }
                } else {
                    // Do not verify the sequence number, consider it empty
                    None
                };

                // Verify the message source if required
                let source = if verify_source {
                    if let Some(bytes) = message.from {
                        if !bytes.is_empty() {
                            match PeerId::from_bytes(&bytes) {
                                Ok(peer_id) => Some(peer_id), // valid peer id
                                Err(_) => {
                                    // invalid peer id, add to invalid messages
                                    debug!("Message source has an invalid PeerId");
                                    let message = RawGossipsubMessage {
                                        source: None, // don't bother inform the application
                                        data: message.data.unwrap_or_default(),
                                        sequence_number,
                                        topic: TopicHash::from_raw(message.topic),
                                        signature: message.signature, // don't inform the application
                                        key: message.key,
                                        validated: false,
                                    };
                                    invalid_messages.push((message, ValidationError::InvalidPeerId));
                                    continue;
                                }
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // This message has passed all validation, add it to the validated messages.
                messages.push(RawGossipsubMessage {
                    source,
                    data: message.data.unwrap_or_default(),
                    sequence_number,
                    topic: TopicHash::from_raw(message.topic),
                    signature: message.signature,
                    key: message.key,
                    validated: false,
                });
            }

            let mut control_msgs = Vec::new();

            if let Some(rpc_control) = rpc.control {
                // Collect the gossipsub control messages
                let ihave_msgs: Vec<GossipsubControlAction> = rpc_control
                    .ihave
                    .into_iter()
                    .map(|ihave| GossipsubControlAction::IHave {
                        topic_hash: TopicHash::from_raw(ihave.topic_id.unwrap_or_default()),
                        message_ids: ihave
                            .message_ids
                            .into_iter()
                            .map(MessageId::from)
                            .collect::<Vec<_>>(),
                    })
                    .collect();

                let iwant_msgs: Vec<GossipsubControlAction> = rpc_control
                    .iwant
                    .into_iter()
                    .map(|iwant| GossipsubControlAction::IWant {
                        message_ids: iwant
                            .message_ids
                            .into_iter()
                            .map(MessageId::from)
                            .collect::<Vec<_>>(),
                    })
                    .collect();

                let graft_msgs: Vec<GossipsubControlAction> = rpc_control
                    .graft
                    .into_iter()
                    .map(|graft| GossipsubControlAction::Graft {
                        topic_hash: TopicHash::from_raw(graft.topic_id.unwrap_or_default()),
                    })
                    .collect();

                let mut prune_msgs = Vec::new();

                for prune in rpc_control.prune {
                    // filter out invalid peers
                    let peers = prune
                        .peers
                        .into_iter()
                        .filter_map(|info| {
                            info.peer_id
                                .as_ref()
                                .and_then(|id| PeerId::from_bytes(id).ok())
                                .map(|peer_id|
                                        //TODO signedPeerRecord, see https://github.com/libp2p/specs/pull/217
                                        PeerInfo {
                                            peer_id: Some(peer_id),
                                        })
                        })
                        .collect::<Vec<PeerInfo>>();

                    let topic_hash = TopicHash::from_raw(prune.topic_id.unwrap_or_default());
                    prune_msgs.push(GossipsubControlAction::Prune {
                        topic_hash,
                        peers,
                        backoff: prune.backoff,
                    });
                }

                control_msgs.extend(ihave_msgs);
                control_msgs.extend(iwant_msgs);
                control_msgs.extend(graft_msgs);
                control_msgs.extend(prune_msgs);
            }

            let rpc = RPC {
                from : stream.remote_peer(),
                msg : RpcMessage {
                    rpc: GossipsubRpc {
                        messages,
                        subscriptions: rpc
                            .subscriptions
                            .into_iter()
                            .map(|sub| GossipsubSubscription {
                                action: if Some(true) == sub.subscribe {
                                    GossipsubSubscriptionAction::Subscribe
                                } else {
                                    GossipsubSubscriptionAction::Unsubscribe
                                },
                                topic_hash: TopicHash::from_raw(sub.topic_id.unwrap_or_default()),
                            })
                            .collect(),
                        control_msgs,
                    },
                    invalid_messages,
                },
            };

            self.incoming_tx.send(rpc).await.map_err(|_| GossipsubDecodeError::ProtocolExit)?;
        }
    }

}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RPC {
    pub msg: RpcMessage,
    // unexported on purpose, not sending this over the wire
    pub from: PeerId,
}

/// A GossipsubRPC message has been received. This also contains a list of invalid messages (if
/// any) that were received.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RpcMessage {
    /// The GossipsubRPC message excluding any invalid messages.
    pub rpc: GossipsubRpc,
    /// Any invalid messages that were received in the RPC, along with the associated
    /// validation error.
    pub invalid_messages: Vec<(RawGossipsubMessage, ValidationError)>,
}

/// Reach attempt interrupt errors.
#[derive(Debug)]
pub enum GossipsubDecodeError {
    /// Error when reading the packet from the socket.
    ReadError(io::Error),
    /// Error when decoding the raw buffer into a protobuf.
    ProtobufError(prost::DecodeError),
    /// Error when parsing the `PeerId` in the message.
    InvalidPeerId,
    /// Protocol message process mainloop exit
    ProtocolExit,
}

impl From<prost::DecodeError> for GossipsubDecodeError {
    fn from(err: prost::DecodeError) -> Self {
        GossipsubDecodeError::ProtobufError(err)
    }
}

impl fmt::Display for GossipsubDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            GossipsubDecodeError::ReadError(ref err) => write!(f, "Error while reading from socket: {}", err),
            GossipsubDecodeError::ProtobufError(ref err) => write!(f, "Error while decoding protobuf: {}", err),
            GossipsubDecodeError::InvalidPeerId => write!(f, "Error while decoding PeerId from message"),
            GossipsubDecodeError::ProtocolExit => write!(f, "Error while send message to message process mainloop"),
        }
    }
}

impl Error for GossipsubDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            GossipsubDecodeError::ReadError(ref err) => Some(err),
            GossipsubDecodeError::ProtobufError(ref err) => Some(err),
            GossipsubDecodeError::InvalidPeerId => None,
            GossipsubDecodeError::ProtocolExit => None,
        }
    }
}
