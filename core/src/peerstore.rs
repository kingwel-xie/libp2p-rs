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

use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use libp2prs_multiaddr::Multiaddr;
use crate::{PublicKey, PeerId};
use std::collections::{HashSet, HashMap};
use serde::{Deserialize, Serialize};
use std::io;
use std::fs::File;
use std::io::{Write, Read};
use std::str::FromStr;
use crate::peerstore::AddrType::{KAD, OTHER};

pub const ADDRESS_TTL: Duration = Duration::from_secs(60 * 60);
pub const TEMP_ADDR_TTL: Duration = Duration::from_secs(2 * 60);
pub const PROVIDER_ADDR_TTL: Duration = Duration::from_secs(10 * 60);
pub const RECENTLY_CONNECTED_ADDR_TTL: Duration = Duration::from_secs(10 * 60);
pub const OWN_OBSERVED_ADDR_TTL: Duration = Duration::from_secs(10 * 60);

pub const PERMANENT_ADDR_TTL: Duration = Duration::from_secs(u64::MAX - 1);
pub const CONNECTED_ADDR_TTL: Duration = Duration::from_secs(u64::MAX - 2);

pub const GC_PURGE_INTERVAL: Duration = Duration::from_secs(10 * 60);

#[derive(Default, Clone)]
pub struct PeerStore {
    inner: Arc<Mutex<HashMap<PeerId, PeerInfo>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerSaved {
    addr: Multiaddr,
    addr_type: AddrType,
    ttl: Duration,
}

#[derive(Clone)]
pub struct PeerInfo {
    addr: Vec<AddrBookRecord>,
    key: Option<PublicKey>,
    proto: HashSet<String>,
}

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug, Serialize, Deserialize)]
pub enum AddrType {
    // KAD means that address is comes from kad protocol.
    // We can't delete kad address in gc, because it may used later.
    KAD,

    // Normal address, it will be deleted if timeout.
    OTHER,
}

#[derive(Clone, Debug)]
pub struct AddrBookRecord {
    addr: Multiaddr,
    addr_type: AddrType,
    ttl: Duration,
    expiry: Option<Instant>,
}

impl PeerStore {
    pub fn insert_peer_info(&self, peer_id: &PeerId, key: PublicKey, addr: Vec<Multiaddr>,
                            ttl: Duration, is_kad: bool, proto: Vec<String>) -> Option<PeerInfo> {
        let mut guard = self.inner.lock().unwrap();

        let mut proto_list = HashSet::new();
        for item in proto {
            proto_list.insert(item);
        }

        let mut addr_list = vec![];

        for a in addr {
            addr_list.push(AddrBookRecord {
                addr: a,
                addr_type: if is_kad { KAD } else { OTHER },
                ttl,
                expiry: if is_kad { None } else { Instant::now().checked_add(ttl) },
            });
        };

        let info = PeerInfo {
            addr: addr_list,
            key: Some(key),
            proto: proto_list,
        };

        guard.insert(peer_id.clone(), info)
    }

    /// Save addr_book when closing swarm
    pub fn save_data(&self) -> io::Result<()> {
        let mut ds_addr_book = HashMap::new();

        {
            let guard = self.inner.lock().unwrap();
            // Transfer peer_id to String and insert into a new HashMap
            for (peer_id, value) in guard.iter() {
                let key = peer_id.to_string();
                let mut v = Vec::new();
                // save address info
                for item in value.addr.to_vec() {
                    v.push(PeerSaved {
                        addr: item.addr,
                        addr_type: item.addr_type,
                        ttl: item.ttl,
                    })
                }
                ds_addr_book.insert(key, v);
            }
        }
        let json_addrbook = serde_json::to_string(&ds_addr_book)?;

        let mut file = File::create("./ds_addr_book.txt")?;
        file.write_all(json_addrbook.as_bytes())
    }

    /// Load addr_book when initializing swarm
    pub fn load_data(&self) -> io::Result<()> {
        let mut file = match File::open("./ds_addr_book.txt") {
            Ok(file) => file,
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    File::create("./ds_addr_book.txt")?
                } else {
                    return Err(e);
                }
            }
        };
        let metadata = file.metadata()?;
        let length = metadata.len() as usize;
        if length == 0 {
            return Ok(());
        }
        let mut buf = vec![0u8; length];

        // Read data from file and deserialize
        let _ = file.read_exact(buf.as_mut())?;
        let json_data: HashMap<String, Vec<PeerSaved>> =
            serde_json::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        // Iter and insert into hashmap
        let mut guard = self.inner.lock().unwrap();
        for (key, value) in json_data {
            let peer_id = PeerId::from_str(&key).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut v = Vec::new();
            for item in value {
                v.push(AddrBookRecord {
                    addr: item.addr,
                    addr_type: item.addr_type,
                    ttl: item.ttl,
                    expiry: Instant::now().checked_add(item.ttl),
                })
            }
            guard.insert(peer_id, PeerInfo {
                addr: v,
                key: None,
                proto: Default::default(),
            });
            // guard.addrs.addr_book.insert(peer_id, );
        }

        Ok(())
    }

    // /// Insert a public key, indexed by peer_id.
    // pub fn add_key(&self, peer_id: &PeerId, key: PublicKey) {
    //     let mut guard = self.inner.lock().unwrap();
    //     // Replace a new key if exists.
    //     // Otherwise, insert while peer_id cannot be found.
    //     match guard.get_mut(peer_id) {
    //         Some(pr) => {
    //             pr.key = Some(key);
    //         }
    //         None => {
    //             guard.insert(peer_id.clone(), PeerInfo {
    //                 addr: vec![],
    //                 key: Some(key),
    //                 proto: Default::default(),
    //             });
    //         }
    //     }
    // }

    /// Delete public key by peer_id.
    pub fn del_key(&self, peer_id: &PeerId) {
        let mut guard = self.inner.lock().unwrap();
        let pr = guard.get_mut(peer_id);
        if pr.is_some() {
            pr.unwrap().key = None
        }
    }

    /// Get public key by peer_id.
    pub fn get_key(&self, peer_id: &PeerId) -> Option<PublicKey> {
        let guard = self.inner.lock().unwrap();
        match guard.get(peer_id) {
            Some(pr) => {
                return pr.clone().key;
            }
            None => {
                None
            }
        }
    }

    /// Get all peer Ids in peer store.
    pub fn get_all_peers(&self) -> Vec<PeerId> {
        let guard = self.inner.lock().unwrap();
        guard.keys().cloned().collect()
    }

    /// Add address to address_book by peer_id, if exists, update rtt.
    pub fn add_addr(&self, peer_id: &PeerId, addr: Multiaddr, ttl: Duration, is_kad: bool) {
        let mut guard = self.inner.lock().unwrap();
        let addr_type = if is_kad { KAD } else { OTHER };
        let expiry = if is_kad { None } else { Instant::now().checked_add(ttl) };
        // if hash contains peer_id
        match guard.get_mut(peer_id) {
            Some(peer_info) => {
                let mut exist = false;
                for (index, item) in peer_info.addr.iter().enumerate() {
                    if item.addr == addr {
                        // addr is exist, update ttl & expiry(if necessary)
                        let record: &mut AddrBookRecord = peer_info.addr.get_mut(index).unwrap();
                        if is_kad {
                            // Update addr to KAD addr.
                            record.set_type(KAD);
                            record.set_expiry(None);
                        } else {
                            record.set_expiry(expiry);
                        }
                        exist = true;
                        break;
                    }
                }
                // If not exists, insert an address into vector.
                if !exist {
                    peer_info.addr.push(AddrBookRecord {
                        addr,
                        addr_type,
                        ttl,
                        expiry,
                    });
                }
            }
            None => {
                // Peer_id non-exists, create a new PeerInfo and fill with a new AddrBookRecord.
                let vec = vec![AddrBookRecord {
                    addr,
                    addr_type,
                    ttl,
                    expiry,
                }];
                guard.insert(peer_id.clone(), PeerInfo {
                    addr: vec,
                    key: None,
                    proto: Default::default(),
                });
            }
        }
    }

    /// Add many new addresses if they're not already in the Address Book.
    pub fn add_addrs(&self, peer_id: &PeerId, addrs: Vec<Multiaddr>, ttl: Duration, is_kad: bool) {
        for addr in addrs {
            self.add_addr(peer_id, addr, ttl, is_kad)
        }
    }

    /// Delete all multiaddr of a peer from address book.
    pub fn clear_addrs(&self, peer_id: &PeerId) {
        let mut guard = self.inner.lock().unwrap();
        match guard.get_mut(peer_id) {
            Some(pr) => {
                pr.addr = vec![];
            }
            _ => {}
        }
    }

    /// Retrieve the record from the address book.
    pub fn get_addrs(&self, peer_id: &PeerId) -> Option<Vec<AddrBookRecord>> {
        let guard = self.inner.lock().unwrap();
        match guard.get(peer_id) {
            Some(pi) => {
                return Some(pi.addr.clone());
            }
            None => {
                None
            }
        }
    }

    /// Update ttl if current_ttl equals old_ttl.
    pub fn update_addr(&self, peer_id: &PeerId, new_ttl: Duration) {
        let mut guard = self.inner.lock().unwrap();

        match guard.get_mut(peer_id) {
            Some(pi) => {
                let time = Instant::now().checked_add(new_ttl);
                for record in pi.addr.iter_mut() {
                    if record.addr_type == KAD {
                        continue;
                    }
                    record.expiry = time;
                }
            }
            None => {}
        }
    }

    /// Get smallvec by peer_id and remove expired address
    pub fn remove_expired_addr(&self, peer_id: &PeerId) {
        let mut guard = self.inner.lock().unwrap();
        match guard.get_mut(peer_id) {
            Some(pi) => {
                let iter_vec = pi.addr.clone();
                let mut remove_count = 0;
                for (index, value) in iter_vec.iter().enumerate() {
                    if value.addr_type == KAD {
                        continue;
                    }
                    if value.expiry.map_or(PERMANENT_ADDR_TTL, |d| d.elapsed()) < GC_PURGE_INTERVAL {
                        continue;
                    } else {
                        pi.addr.remove(index - remove_count);
                        remove_count += 1;
                    }
                }
            }
            None => {}
        }
    }

    /// Insert supported protocol by peer_id
    pub fn add_protocol(&self, peer_id: &PeerId, proto: Vec<String>) {
        let mut guard = self.inner.lock().unwrap();
        if guard.contains_key(peer_id) {
            let record = guard.get_mut(peer_id).unwrap();
            for item in proto {
                record.proto.insert(item);
            }
        } else {
            let mut s = HashSet::new();
            for item in proto {
                s.insert(item);
            }
            guard.insert(peer_id.clone(), PeerInfo {
                addr: vec![],
                key: None,
                proto: s,
            });
        }
    }

    /// Remove support protocol by peer_id
    pub fn remove_protocol(&self, peer_id: &PeerId) {
        let mut guard = self.inner.lock().unwrap();
        match guard.get_mut(peer_id) {
            Some(pi) => {
                pi.proto = HashSet::new();
            }
            None => {}
        }
    }

    /// Get supported protocol by peer_id.
    pub fn get_protocol(&self, peer_id: &PeerId) -> Option<Vec<String>> {
        let guard = self.inner.lock().unwrap();
        match guard.get(peer_id) {
            Some(pi) => {
                let mut result = Vec::<String>::new();
                for s in pi.proto.iter() {
                    result.push(s.parse().unwrap())
                }
                return Some(result);
            }
            None => {
                None
            }
        }
    }

    /// Get the first protocol which is matched by the given protocols.
    pub fn first_supported_protocol(&self, peer_id: &PeerId, proto: Vec<String>) -> Option<String> {
        let guard = self.inner.lock().unwrap();
        match guard.get(peer_id) {
            Some(pi) => {
                for item in pi.proto.iter() {
                    if proto.contains(item) {
                        return Some(item.parse().unwrap());
                    }
                }
            }
            None => {}
        }
        None
    }

    /// Search all protocols and return an option that matches by given proto param.
    pub fn support_protocols(&self, peer_id: &PeerId, proto: Vec<String>) -> Option<Vec<String>> {
        let guard = self.inner.lock().unwrap();
        match guard.get(peer_id) {
            Some(pi) => {
                let mut proto_list = Vec::new();
                for item in proto {
                    if pi.proto.contains(&item) {
                        proto_list.push(item)
                    }
                }
                return Some(proto_list);
            }
            None => {
                None
            }
        }
    }

    /// Remove timeout address
    pub async fn addr_gc(self) {
        loop {
            log::info!("GC is looping...");
            async_std::task::sleep(GC_PURGE_INTERVAL).await;
            let pid_addr = self.get_all_peers();
            if !pid_addr.is_empty() {
                for id in pid_addr {
                    self.remove_expired_addr(&id);
                }
            }
            log::info!("GC finished");
        }
    }
}

impl AddrBookRecord {
    /// Set the route-trip-time
    pub fn get_addr(&self) -> &Multiaddr {
        &self.addr
    }

    /// Set the route-trip-time
    pub fn into_maddr(self) -> Multiaddr {
        self.addr
    }

    /// Set the route-trip-time
    pub fn set_ttl(&mut self, ttl: Duration) {
        self.ttl = ttl
    }

    /// Set the expiry time
    pub fn set_expiry(&mut self, expiry: Option<Instant>) {
        self.expiry = expiry
    }

    /// Get the route-trip-time
    pub fn get_type(&self) -> AddrType {
        self.addr_type
    }

    /// Set the type of address
    pub fn set_type(&mut self, addr_type: AddrType) {
        self.addr_type = addr_type
    }

    /// Get the expiry time
    pub fn get_expiry(&self) -> Option<Instant> {
        self.expiry
    }
}

#[cfg(test)]
mod tests {
    use crate::peerstore::{PeerInfo, AddrBookRecord, AddrType, ADDRESS_TTL, PeerStore};
    use crate::PeerId;
    use std::time::Duration;
    use crate::identity::Keypair;
    use wasm_timer::Instant;
    use std::collections::{HashSet, HashMap};
    use std::sync::{Arc, Mutex};

    #[test]
    fn addr_basic() {
        // let keypair = Keypair::generate_secp256k1();
        let peer_id = PeerId::random();

        let pr = PeerInfo {
            addr: vec![AddrBookRecord {
                addr: "/memory/123456".parse().unwrap(),
                addr_type: AddrType::KAD,
                ttl: ADDRESS_TTL,
                expiry: Instant::now().checked_add(ADDRESS_TTL),
            }],
            key: None,
            proto: HashSet::new(),
        };
        let mut hashmap = HashMap::new();
        hashmap.insert(peer_id.clone(), pr);
        let peerstore = PeerStore { inner: Arc::new(Mutex::new(hashmap)) };

        peerstore.add_addr(&peer_id, "/memory/123456".parse().unwrap(), Duration::from_secs(1), false);

        assert_eq!(
            &(peerstore.get_addrs(&peer_id).unwrap().first().unwrap().addr),
            &"/memory/123456".parse().unwrap()
        );

        peerstore.add_addr(&peer_id, "/memory/654321".parse().unwrap(), Duration::from_secs(1), false);
        let addrs = peerstore.get_addrs(&peer_id).unwrap();
        assert_eq!(addrs.len(), 2);

        peerstore.add_addr(&peer_id, "/memory/654321".parse().unwrap(), Duration::from_secs(1), false);
        let addrs = peerstore.get_addrs(&peer_id).unwrap();
        assert_eq!(addrs.len(), 2);

        peerstore.clear_addrs(&peer_id);
        assert_eq!(peerstore.get_addrs(&peer_id).unwrap().len(), 0);
    }

    #[test]
    fn proto_basic() {
        let keypair = Keypair::generate_secp256k1();
        let peer_id = PeerId::random();

        let mut proto_list = HashSet::new();
        for item in vec!["/libp2p/secio/1.0.0".to_string(), "/libp2p/yamux/1.0.0".to_string()] {
            proto_list.insert(item);
        }

        let pr = PeerInfo {
            addr: vec![],
            key: Some(keypair.public()),
            proto: proto_list,
        };
        let mut hashmap = HashMap::new();
        hashmap.insert(peer_id.clone(), pr);
        let peerstore = PeerStore { inner: Arc::new(Mutex::new(hashmap)) };

        let proto_list = vec!["/libp2p/secio/1.0.0".to_string(), "/libp2p/yamux/1.0.0".to_string()];
        peerstore.add_protocol(&peer_id, proto_list.clone());

        let p = peerstore.get_protocol(&peer_id).unwrap();

        for i in proto_list {
            if p.contains(&i) {
                continue;
            } else {
                unreachable!()
            }
        }

        let optional_list = vec!["/libp2p/noise/1.0.0".to_string(), "/libp2p/yamux/1.0.0".to_string()];
        let protocol = peerstore.first_supported_protocol(&peer_id, optional_list);
        assert_eq!(protocol.unwrap(), "/libp2p/yamux/1.0.0");

        let option_support_list = vec![
            "/libp2p/secio/1.0.0".to_string(),
            "/libp2p/noise/1.0.0".to_string(),
            "/libp2p/yamux/1.0.0".to_string(),
        ];
        let support_protocol = peerstore.support_protocols(&peer_id, option_support_list);
        assert_eq!(
            support_protocol.unwrap(),
            vec!["/libp2p/secio/1.0.0".to_string(), "/libp2p/yamux/1.0.0".to_string()]
        );
    }

    #[test]
    fn peerstore_basic() {
        let peer_id = PeerId::random();
        let keypair = Keypair::generate_secp256k1();

        let addr = vec!["/memory/123456".parse().unwrap(), "/memory/123456".parse().unwrap()];
        let proto = vec!["/libp2p/secio/1.0.0".to_string(), "/libp2p/yamux/1.0.0".to_string()];

        let ps = PeerStore::default();
        ps.insert_peer_info(&peer_id, keypair.public(), addr, ADDRESS_TTL, false, proto);

        let optional_list = vec!["/libp2p/noise/1.0.0".to_string(), "/libp2p/yamux/1.0.0".to_string()];
        let protocol = ps.first_supported_protocol(&peer_id, optional_list);
        assert_eq!(protocol.unwrap(), "/libp2p/yamux/1.0.0");

        let option_support_list = vec![
            "/libp2p/secio/1.0.0".to_string(),
            "/libp2p/noise/1.0.0".to_string(),
            "/libp2p/yamux/1.0.0".to_string(),
        ];
        let support_protocol = ps.support_protocols(&peer_id, option_support_list);
        assert_eq!(
            support_protocol.unwrap(),
            vec!["/libp2p/secio/1.0.0".to_string(), "/libp2p/yamux/1.0.0".to_string()]
        );
    }
}