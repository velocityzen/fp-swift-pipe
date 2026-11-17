# Handling Failures

Turn values into failures, change errors, and recover from them.

A failure enters the pipe from a source of `Result`s, like `FromResult`, or from a stage that returns a `Result`, like `FlatMap`. `FailOnCancellation` adds one when the reading task is cancelled.

| Stage                         | Closure                   | What it does                                                               |
| ----------------------------- | ------------------------- | -------------------------------------------------------------------------- |
| `FlatMap`                     | `(A) -> Result<B, F>`     | Changes a value. It can also turn the value into a failure.                |
| `MapError`                    | `(F1) -> F2`              | Changes an error, or its type.                                             |
| `TapError`                    | `(F) -> Void`             | Runs a side effect for each failure. The failure does not change.          |
| `Alt`                         | `() -> Result<A, F>`      | Replaces each failure with a new `Result`. It does not see the error.      |
| `FlatMapError`                | `(F1) -> Result<A, F2>`   | Sees the error and returns a `Result`: a value to recover, or a failure.   |
| `GetOrElse`                   | `(F) -> A`                | Replaces each failure with a value. After it, the pipe cannot fail.        |
| `Match(onSuccess:onFailure:)` | `(A) -> R` and `(F) -> R` | Turns values and errors into one type `R`. After it, the pipe cannot fail. |
| `FailOnCancellation(error)`   | none                      | Ends the pipe with one failure when the reading task is cancelled.         |

```swift
enum FetchError: Error, Equatable {
    case timeout
    case notFound
}

let items = Pipe<String, FetchError> {
    From([1, 2, 3])
    FlatMap { (id: Int) -> Result<String, FetchError> in
        id == 2 ? .failure(.timeout) : .success("item \(id)")
    }
    TapError { (error: FetchError) in print("failed:", error) }
    FlatMapError { (error: FetchError) -> Result<String, FetchError> in
        error == .timeout ? .success("cached item") : .failure(error)
    }
}

await items.toResult()
// .success(["item 1", "cached item", "item 3"])
```

`GetOrElse` and `Match(onSuccess:onFailure:)` give a pipe that cannot fail:

```swift
let names = Pipe<String, Never> {
    FromResult(items)
    GetOrElse { (error: FetchError) in "unknown" }
}

let lines = Pipe<String, Never> {
    FromResult(items)
    Match(
        onSuccess: { (item: String) in "ok: \(item)" },
        onFailure: { (error: FetchError) in "error: \(error)" }
    )
}
```

`AsyncFlatMap`, `AsyncTapError`, `AsyncAlt`, `AsyncFlatMapError`, `AsyncGetOrElse` and `AsyncMatch` do the same with an `async` closure.

To handle only some errors, use `MatchError` (see <doc:PatternMatching>).

## Calling throwing code

Stages do not throw, so the compiler always knows the failure type. To call a function that throws, turn its result into a `Result` inside the closure:

- For sync code, use `Result { try … }` from the standard library, in `FlatMap`.
- For async code, use `Result.fromAsync { try await … }` from fp-swift, in `AsyncFlatMap`.

Then change the error into your error type with `mapError`:

```swift
import FP   // for Result.fromAsync
import FPPipe
import Foundation

struct User: Decodable, Sendable {
    let name: String
}

enum LoadError: Error {
    case network(any Error)
    case decode(any Error)
}

let users = Pipe<User, LoadError> {
    From(urls)
    AsyncFlatMap { (url: URL) in
        await Result.fromAsync {
            try await URLSession.shared.data(from: url).0
        }
        .mapError(LoadError.network)
    }
    FlatMap { (data: Data) in
        Result { try JSONDecoder().decode(User.self, from: data) }
            .mapError(LoadError.decode)
    }
}
```

A throwing `AsyncSequence` works too. fp-swift's `asResults()` turns it into a sequence of `Result`s that does not throw: each element becomes a success, and the error it throws becomes one last failure, with its type. Read it with `FromResult`, or with `DeferResult` when it can be read only once, like a stream:

```swift
let messages = Pipe<String, LoadError> {
    DeferResult { socket.messages().asResults() }
    MapError(LoadError.network)
}
```

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
