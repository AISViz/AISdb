# MPROXY: Multicast Network Dispatcher and Proxy

Streams data over the network. 


## About 
This repo includes four packages: Forward-proxy, reverse-proxy, UDP client, and UDP server. Proxies allow conversion between TCP and UDP, so these blocks can be combined together for complete interoperability with existing networks.  
A primary feature is compatability with [UDP Multicast](https://en.wikipedia.org/wiki/Multicast) for intermediate routing and reverse-proxy, enabling dead simple group communication across complex one-to-many or many-to-many data streams, and resulting in scalable reverse-proxy.
Packages can be run either from the command line or included as a library.

- [X] Simple to use full networking stack
  - Send, proxy, reverse-proxy, and receive to/from multiple endpoints simultaneously
- [X] Fast
  - Can be deployed in less than 5 minutes
  - 500+ Mbps read/transfer/write speed (UDP)
- [X] Minimal 
  - Zero configuration, logging, or caching
  - Tiny memory footprint, compiled binary sizes ~350KB
  - No shared resources between threads
- [X] Leverage benefits of UDP
  - Simple stream aggregation
  - Performant proxy and reverse proxy
  - UDP multicasting enables stateless, scalable reverse-proxy


## Quick Start
Get started with a simple client/server network. Install the command line tools with cargo, and start a UDP listen server on port 9920.
```bash
cargo install mproxy-client mproxy-server
mproxy-server --listen-addr "localhost:9920" --path "streamoutput.log" --tee
```
Then send some bytes from the client to the server. The path option "-" tells the client to read input from stdin. A filepath, descriptor, or handle may also be used.
```bash
mproxy-client --path "-" --server-addr "localhost:9920"
> Hello world!
```
You should now see your message appear in `streamoutput.log` (and also to stdout if `--tee` is used)


### Compatability

- [X] Windows/Linux/Mac
- [X] IPv4/IPv6
- [X] UDP
- [X] TCP/TLS 
  - via forward and reverse proxy 
  - Partial client-side TLS support provided by `rustls` (requires feature `tls` enabled in `mproxy-forward`)
- [X] Fully transparent routing



## Docs
See the documentation for installing and operation instructions
 - [mproxy-client](https://docs.rs/mproxy-client/)
 - [mproxy-server](https://docs.rs/mproxy-server/)
 - [mproxy-forward](https://docs.rs/mproxy-forward/)
 - [mproxy-reverse](https://docs.rs/mproxy-reverse/)

