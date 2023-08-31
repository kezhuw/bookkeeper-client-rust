# Apache BookKeeper client
[![crates.io](https://img.shields.io/crates/v/bookkeeper-client)](https://crates.io/crates/bookkeeper-client)
[![docs.rs](https://img.shields.io/docsrs/bookkeeper-client)](https://docs.rs/bookkeeper-client)
[![github-ci](https://github.com/kezhuw/bookkeeper-client-rust/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/kezhuw/bookkeeper-client-rust/actions)
[![mit-license](https://img.shields.io/github/license/kezhuw/bookkeeper-client-rust)](LICENSE)

Apache BookKeeper client writes in async rust

## Disclaimer
**This project is far from production usage.**

## Examples
See [tests](tests/client.rs).

## History
[I tried to construct a `Send`, `!Sync` and `Clone`](https://github.com/kezhuw/bookkeeper-client-rust/blob/76f2fc88384966b1e367f3dc6f3538938214d214/README.md#send-sync-and-await) to batch simultaneous requests in single asynchronous task and serve parallel requests in multiple concurrent asynchronous tasks. But it failed due to `.await` requires `&self` to be `Send` which is not possible [by definition](https://github.com/rust-lang/rust/blob/008c21c9779fd1e3632d9fe908b8afc0c421b26c/library/core/src/marker.rs#L506) if `Self` is `!Sync`. See [What shall Sync mean across an .await](https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020) for thoughts from experts.

## License
[MIT](LICENSE).
