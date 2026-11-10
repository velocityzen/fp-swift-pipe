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

By default, an async stage handles one element at a time, in order, in the task that reads the pipe.

fp-swift's `|>` operator also works with async functions:

```swift
AsyncMap { (url: URL) in await url |> fetchData |> decodeArticle }
```

## Running work in parallel

Async stages take `concurrency:`. The default is 1. With a larger number, up to that many closures run at the same time:

```swift
AsyncMap(concurrency: 10) { (url: URL) in await fetchData(url) }
AsyncFilter(concurrency: 4) { (item: Item) in await isValid(item) }
```

By default, results come out **in the order they finish**. A fast element can pass a slow one. With `ordered: true`, results come out **in source order**:

```swift
AsyncMap(concurrency: 10, ordered: true) { (url: URL) in
    await fetchData(url)
}
```

A slow element then holds back the results after it, but not the work: later elements are still processed, and their results wait. Use `ordered: true` only when the order matters.

`bufferLimit:` caps how many results may wait, for example when the source never ends:

```swift
AsyncMap(concurrency: 8, ordered: true, bufferLimit: 32) { (url: URL) in
    await fetchData(url)
}
```

A `bufferLimit` below `concurrency - 1` also lowers how many closures run at the same time. Without `ordered: true`, `bufferLimit:` does nothing.

Rules:

- At most `concurrency` elements are being processed or waiting to be read. When the reader is slow, the stage stops reading the source, so memory does not grow.
- With `ordered: true`, results that wait for an earlier, slower element do not count toward `concurrency`. So many results can wait behind one very slow element, unless you set `bufferLimit:`.
- With `concurrency: 1`, the closure runs in the reader's task. With more, closures run in child tasks.
- `concurrency:` must be 1 or more, and `bufferLimit:` must be 0 or more. A smaller value stops the program.
- `AsyncTap`, `AsyncTapError`, `AsyncTapMatch` and `AsyncTapMatchError` have no `concurrency:`. They run one at a time, so side effects happen in order.

### Cancellation

When you `break` out of `for await`, or the reading task is cancelled, the pipe stops at once. Running closures are cancelled, and the reader does not wait for them.

A closure stops early only if it checks for cancellation. `Task.sleep`, `URLSession` calls and `try Task.checkCancellation()` do. A long loop with no `await` runs to its end, so check `Task.isCancelled` inside it.

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
