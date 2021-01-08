## 0.1.0 Initial release (2020.10.26)

### Features

- Transport: Tcp, Dns, Websocket
- Security IO: secio, plaintext, noise
- Stream Muxing: yamux, mplex
- Transport Upgrade: Multistream select, timeout, protector 
- Swarm, with Ping & Identify



## 0.2.0 release (2021.1.10)

### New features and changes

- Protocols
    + **Kad-DHT**: full functions comparable to `rust-libp2p` 
    + floodsub: experimental
    + mDns: experimental
- **Swarm: dailer added to support dialing in parallel**
- **Swarm: improved identify protocol** 
- **Swarm: metric support and many bug fixes**
- **Swarm: notification mechanism for protocol handlers**
- Tcp transport: interface address change event
- PeerStore improvement
- Prometheus exporter and Info web server
- An interactive shell integrated into Swarm and Kad
- Copyright notice updated to conform with MIT license

 