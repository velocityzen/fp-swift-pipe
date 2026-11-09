# Async and Parallel Work

Run async closures, run several of them at the same time, and stop when the reader stops.

Most stages have an async version, with `Async` at the start of the name: `AsyncMap`, `AsyncFilter`, `AsyncCompactMap`, `AsyncFlatMap`, `AsyncTap` and others. Use them when the closure must `await`.

```swift
let activeUsers = Pipe<User, APIError> {
    From(userIDs)
    AsyncFlatMap { (id: Int) in await api.user(id) }
    AsyncFilter { (user: User) in await api.isActive(user) }
}
```

`api.user(id)` returns a `Result<User, APIError>`, so a failed request becomes a failure in the pipe.

By default, an async stage handles one element at a time, in order. The closure runs in the task that reads the pipe, on the same actor.

fp-swift's `|>` operator also works with async functions:

```swift
AsyncMap { (url: URL) in await url |> fetchData |> decodeArticle }
```

## Running work in parallel

FPPipe runs work one element at a time unless you ask for more. Async stages take `concurrency:`. The default is 1. With a larger number, up to that many closures run at the same time:

```swift
AsyncMap(concurrency: 10) { (url: URL) in await fetchData(url) }
AsyncFilter(concurrency: 4) { (item: Item) in await isValid(item) }
```

The order of results depends on the stage:

- `AsyncMap`, `AsyncFlatMap` and the other async stages give results **in the order they finish**. A fast element can pass a slow one.
- `AsyncMapKeepOrder` and `AsyncFlatMapKeepOrder` give results **in source order**. A slow element holds back the faster elements behind it.

Use the `KeepOrder` stages only when the order matters, for example when you later zip the results with another sequence.

Rules:

- At most `concurrency` elements are being processed or waiting to be read. When the reader is slow, the stage stops reading the source, so memory does not grow.
- Elements that the stage does not change, like failures in `AsyncMap`, pass through without a new task.
- With `concurrency: 1`, the closure runs in the reader's task and on its actor. With more, closures run in child tasks, not on the reader's actor.
- `concurrency:` must be 1 or more. A smaller value stops the program with a precondition failure.
- `AsyncTap`, `AsyncTapError`, `AsyncTapMatch` and `AsyncTapMatchError` have no `concurrency:`. They always run one at a time, so side effects happen in order.

### Cancellation

When you `break` out of `for await`, or the reading task is cancelled, the pipe stops at once. Running closures get a cancellation signal, and the reader does not wait for them to finish.

A closure stops early only if it checks for cancellation. A closure that awaits `Task.sleep`, a `URLSession` call or `try Task.checkCancellation()` stops quickly. A long loop with no `await` runs to its end. This is how Swift tasks work. For long CPU work with `concurrency:` above 1, check `Task.isCancelled` inside the closure.

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
