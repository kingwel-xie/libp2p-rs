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

use std::time::Instant;

/// The information of a peer in Kad routing table.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    /// The time instant at which we talk to the remote peer.
    aliveness: Option<Instant>,

    /// The time this peer was added to the routing table.
    added_at: Instant,

    /// If a bucket is full, this peer can be replaced to make space for a new peer.
    replaceable: bool,
}

impl PeerInfo {
    pub(crate) fn new(aliveness: bool) -> Self {
        Self {
            aliveness: if aliveness { Some(Instant::now()) } else { None },
            added_at: Instant::now(),
            replaceable: true,
        }
    }

    pub(crate) fn is_replaceable(&self) -> bool {
        self.replaceable
    }

    pub(crate) fn set_aliveness(&mut self, aliveness: Instant) {
        self.aliveness = Some(aliveness);
    }

    pub(crate) fn get_aliveness(&self) -> Option<Instant> {
        self.aliveness
    }
}
