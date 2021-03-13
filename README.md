# swim

An implementation of the SWIM gossip protocol in Rust using tokio-rs.

# Running

To run the code do `cargo build` then to see debug output use:
```
RUST_LOG=debug ./target/debug/swim -a 127.0.0.1:1234
RUST_LOG=debug ./target/debug/swim -a 127.0.0.1:1235 -b 127.0.0.1:1234
RUST_LOG=debug ./target/debug/swim -a 127.0.0.1:1236 -b 127.0.0.1:1234
RUST_LOG=debug ./target/debug/swim -a 127.0.0.1:1237 -b 127.0.0.1:1234
RUST_LOG=debug ./target/debug/swim -a 127.0.0.1:1238 -b 127.0.0.1:1234
```

Note: The code needs at least 5 instances to be running for Snowball consensus to start.

If you prefer to only see the output of consensus, use `RUST_LOG=info`.

