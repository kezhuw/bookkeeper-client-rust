# Apache BookKeeper client
Apache BookKeeper client writes in async rust

## Disclaimer
**This project is far from production usage.**

## Examples
See [tests](tests/client.rs).

## `Send`, `!Sync` and `.await`
I tried to build a `Send` and `!Sync` to serve multiple simultaneous writes in single asynchronous task.
* Wraps *owned* `std::rc::Rc` to implement `Send`.
* Wraps `std::cell::Cell` related structs to implement `Sync`.

Both make owned immutable reference `Send` to across `await` point. I think it is safe due to following reasons:
* They are owned by single task.
* `await` imposes strong ordering across scheduling threads. Let's assume task is transferred from thread-a to thread-b in polling. Thread-b must see all operations performed in thread-a, otherwise all async tasks will perform undefined.

But this failed due to limitation in current `async` rust. `!Send`(aka. reference to `!Sync`) across `await` make whole future `!Send`, this make generated future useless. To my knowledge, `await` should require `!ThreadBound`(e.g. `MutexGuard`) but not `Send` for it to be `Send`. A future should be `Send` unless it captures `!Send`.

It is counterintuitive, at least to me, that `Send` and `!Sync` are able to use in multi-core synchronous thread environment but not in multi-thread asynchronous task environment.

There are related discussions from [What shall Sync mean across an .await](https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020). I quoted some for highlight.

> I think @rkuhn is talking about the more general case where the reference is used after the await. In that case, accessing the object without it being Sync is unsound in general. But if we could somehow prove that the object in question uniquely belongs to this async 'thread' (task, chain of futures, whatever you want to call it) – in other words, that if the current async fn is sent to another thread, all other code accessing the object will also be sent to that same thread – then it would be sound.
>
> -- @comex https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020/14

> Yes, that is part of what I’m asking. The bigger question is whether this “we could detect this and it would be sound” is really the right approach — it makes the compiler response to any given code less predictable. I’d prefer a feature that always works but is less powerful over one that works most of the time.
>
> If someone can prove (or at least convince themself) that it is possible to make it always work, then that’s good; current behavior is then just qualified as a bug and needs to be fixed. But if after fixing all bugs there are still situations where a subtle local change to the structure of my code requires something to be Sync then the developer experience suffers.
>
> -- @rkuhn https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020/15

> In the longer term, I'm not very optimistic that making this analysis more precise will ever be implemented, but I'll make this note:
>
> This problem has a fundamental analogy to the lifetime problems that led us to the current async/await system. Previously, a future containing references of a lifetime 'a can also not exceed that lifetime: now the lifetimes are "closed over" to create futures that are 'static, unless the references came in externally. This problem stems from a desire to similarly "close over" internal non-Sync data. Of course the fact that lifetimes have a built-in escape analysis (fundamentally they are a tool for escape analysis) contributed to making the solution for lifetimes much more straightforward than it would be for the Sync trait.
>
> -- @withoutboats https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020/17

## License
[MIT](LICENSE).
