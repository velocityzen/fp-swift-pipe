# PipelineKit

[![Documentation](https://img.shields.io/badge/documentation-DocC-purple)](https://swiftpackageindex.com/velocityzen/PipelineKit/documentation/pipelinekit)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A small, opinionated library for composing async, error-aware pipelines in Swift.

A `Pipe<Success, Failure>` is a re-iterable description of an async stream of `Result<Success, Failure>`. Stages compose in a `@resultBuilder` DSL. Errors live in the `Result.failure` channel — the library is `Result`-only by design, and Swift `throws` never crosses a stage boundary. Throwing code is bridged at the call site (see [Working with throwing code](#working-with-throwing-code) below). Built on top of [`fp-swift`](https://github.com/velocityzen/fp-swift).

## Example

A realistic article-fetching pipeline. Concurrent fetches preserve source order, throwing decode is bridged into a typed failure channel, rate-limit failures are recovered with a fallback, and other failures short-circuit:

```swift
import PipelineKit
import FP

enum AppError: Error, Sendable {
    case network(any Error)
    case decode(Data)
    case rateLimited
}

struct Article: Decodable, Sendable {
    let id: String
    let isInteresting: Bool
    static let placeholder = Article(id: "—", isInteresting: false)
}

func fetchArticles(_ urls: [URL]) async -> Result<[Article], AppError> {
    let pipe = Pipe<Article, AppError> {
        // 1. Lift URLs into the pipeline (Pipe<URL, Never>).
        From(urls)

        // 2. Concurrent fetches; emission order matches the input order.
        AsyncMapKeepOrder { url -> Result<Data, AppError> in
            await Result.fromAsync { try await URLSession.shared.data(from: url).0 }
                .mapError(AppError.network)
        }

        // 3. Lift the inner Result back into the failure channel.
        FlatMap { (r: Result<Data, AppError>) in r }

        // 4. Bridge throwing JSON decode into the failure channel.
        FlatMap { data in
            Result { try JSONDecoder().decode(Article.self, from: data) }
                .mapError { _ in AppError.decode(data) }
        }

        // 5. Drop boring articles.
        Filter { $0.isInteresting }

        // 6. Side-effect on failures only — count in metrics, log, etc.
        TapError { error in print("article failed:", error) }

        // 7. Recover only from rate-limit failures with a placeholder; others propagate.
        FlatMapError { (e: AppError) -> Result<Article, AppError> in
            switch e {
                case .rateLimited: return .success(.placeholder)
                default:           return .failure(e)
            }
        }
    }

    return await pipe.toResult()
}
```

Key shapes used:
- `From` / `FromResult` for source ingest.
- `AsyncMapKeepOrder` for concurrent-but-ordered work.
- `FlatMap` to bridge throwing decode into the failure channel.
- `Filter` for drop-on-predicate.
- `TapError` for failure-side observation without consumption.
- `FlatMapError` for selective recovery — pattern-match the error, return success or re-fail.
- `toResult` for all-or-nothing collection.

## Stage catalog

| Stage | Shape | Purpose |
|---|---|---|
| `Map` | `(A) → B` | Transform success values. |
| `FlatMap` | `(A) → Result<B, F>` | Transform with possible failure. |
| `AsyncMap` | `(A) async → B` | Async transform. |
| `AsyncFlatMap` | `(A) async → Result<B, F>` | Async transform with possible failure. |
| `AsyncMapKeepOrder` | `(A) async → B`, `concurrency:` | Bounded-parallel transform; **preserves source order**. |
| `Filter` / `AsyncFilter` | `(A) → Bool` / `(A) async → Bool` | Keep matching successes; failures pass through. |
| `CompactMap` / `AsyncCompactMap` | `(A) → B?` / `(A) async → B?` | Map and drop `nil`s. |
| `Take` / `Drop` | `Int` | Limit / skip leading elements. |
| `FlatMapSequence` | `(A) → Sequence<B>` | One-to-many fan-out, sync inner. |
| `FlatMapAsyncSequence` | `(A) → AsyncSequence<B>` | One-to-many fan-out, async inner (non-throwing). |
| `MapError` | `(F1) → F2` | Transform the failure channel. |
| `Alt` / `AsyncAlt` | `() → Result<A, F>` / `() async → Result<A, F>` | Replace failures with an alternative `Result` that doesn't see the error. |
| `FlatMapError` / `AsyncFlatMapError` | `(F1) → Result<A, F2>` / `(F1) async → Result<A, F2>` | Recover or re-fail with a possibly different error type. |
| `GetOrElse` / `AsyncGetOrElse` | `(F) → A` / `(F) async → A` | Collapse failures to a `Success`; output pipeline cannot fail (`Failure == Never`). |
| `Tap` / `AsyncTap` | `(A) → Void` / `(A) async → Void` | Observe successes. |
| `TapError` / `AsyncTapError` | `(F) → Void` / `(F) async → Void` | Observe failures. |
| `Match` / `AsyncMatch` | `onSuccess: (A) → R, onFailure: (F) → R` (and async) | Fold both channels into a single `R`; output `Failure == Never`. |

### Sources

| Source | Notes |
|---|---|
| `From(seq)` | Lift an `AsyncSequence` or `Sequence` of bare values (each → `.success`). |
| `FromResult(seq)` | Lift an `AsyncSequence` of `Result` elements directly. |
| `Defer { … }` / `DeferResult { … }` | Multi-statement, re-iteration-fresh source construction. |
| `FromAsync { await … }` / `FromAsyncResult { … }` | Source whose **producer is async** — useful when iteration needs async setup (cursors, authenticated streams). Closure is re-awaited per iteration. |
| `From(T.self)` / `FromAsync(T.self)` | **Open** marker — declares an `Input` to be supplied at call time. Builds an `OpenPipe<T, …, …>` instead of a `Pipe`. See [Open pipes](#open-pipes). |
| `Success(value)` / `Of(value)` | Single success (`Of` is an alias). |
| `Failure(error, valueType:)` | Single failure. |
| `Empty(valueType:failureType:)` | Empty source. |

### Sinks

| Sink | Returns | Stops at |
|---|---|---|
| `await pipe.toResult()` | `Result<[Success], Failure>` (`@discardableResult`) | first failure |
| `await pipe.toArray()` | `[Result<Success, Failure>]` | end of stream |
| `await pipe.reduce(init, combine)` | `Result<U, Failure>` | first failure |
| `await pipe.first()` | `Result<Success, Failure>?` | first element |
| `await pipe.firstSuccess()` | `Success?` | first success |
| `await pipe.firstError()` | `Failure?` | first failure |
| `for await x in pipe` | `Result<Success, Failure>` per element | iteration |

### Failure handling

Three stages mirror fp-swift's `Result+Failure` API at the streaming level — each per-element `.failure` is handled by the closure; successes pass through unchanged.

```swift
Pipe<Item, AppError> {
    From(urls)
    AsyncFlatMap { url in await fetch(url) }

    // Alt: replace failures without seeing the error. Same Failure type.
    Alt { Result<Item, AppError>.success(.placeholder) }

    // FlatMapError: receive the error, return a Result. May change the failure type.
    FlatMapError { (e: AppError) -> Result<Item, OtherError> in ... }

    // GetOrElse: collapse to a plain Success — output pipeline cannot fail.
    GetOrElse { (e: AppError) -> Item in fallback(for: e) }
}
```

Each comes with an `Async*` variant (`AsyncAlt`, `AsyncFlatMapError`, `AsyncGetOrElse`) for cache lookups, retries, or any other async fallback.

## Open pipes

A pipeline that starts with `From(T.self)` (or `FromAsync(T.self)`) leaves the source slot open and builds an `OpenPipe<Input, Success, Failure>` — a re-callable function from a `Sequence`/`AsyncSequence` to a regular `Pipe`. Construct an open pipe once, call it with different inputs:

```swift
let pipe = OpenPipe {
    From(Int.self)
    Filter { $0 > 0 }
    Map { (n: Int) in n * 2 }
}

for await x in pipe([1, -1, 2]) { … }            // closed Pipe<Int, Never>
for await x in pipe(asyncStreamOfInts) { … }     // also a closed Pipe
```

`pipe(source)` accepts any `Sequence` or `AsyncSequence` whose `Element == Input`. The returned `Pipe` is itself re-iterable in the usual way. Useful when the same pipeline shape needs to run over multiple inputs (per-request handlers, batch jobs over different cohorts, test setups), without rebuilding the stage chain.

Open pipes accept all the same stages as closed pipes — every builder overload has an open-pipe variant.

## Working with throwing code

PipelineKit is `Result`-only by design. There's no `TryMap` and no throwing-stage variant of any operator — stages take and return `Result`, period. The library nudges you to express your code in `Result`-land first, where success and failure are values, then compose. When you have to call a throwing API, bridge it at the closure boundary using stdlib's `Result(catching:)` for sync code and fp-swift's `Result.fromAsync { … }` for async:

```swift
// Sync throwing function → FlatMap
Pipe<Item, AppError> {
    From(payloads)
    FlatMap { data in
        Result { try decode(data) }.mapError(AppError.parse)
    }
}

// Async throwing function → AsyncFlatMap
Pipe<Item, AppError> {
    From(urls)
    AsyncFlatMap { url in
        await Result.fromAsync { try await fetch(url) }
            .mapFailureAsync(AppError.network)
    }
}
```

The same pattern applies to throwing `AsyncSequence`s: wrap them in a `Result`-bearing producer first, then feed via `FromResult`. The pipeline's failure channel stays typed and intentional, every `throws` is converted at exactly one place, and the typed-throws boundary is visible in the diff rather than hidden in a stage adapter.

## Concurrency

Every `Async*` stage takes a `concurrency: Int = 1` parameter. With the default of 1 the stage runs strictly sequentially — element N's closure waits for N−1 to finish. With `concurrency > 1`, up to N closures run in parallel:

```swift
AsyncMap(concurrency: 10) { url in await fetch(url) }
AsyncFilter(concurrency: 4) { item in await isInteresting(item) }
AsyncFlatMap(concurrency: 8) { url in await fetchOrFail(url) }
AsyncCompactMap(concurrency: 6) { id in await maybeLookUp(id) }
```

**Two ordering modes:**
- **`AsyncMap` (and most `Async*` stages)** emit results **as they complete** — the fastest closure wins, output order is unrelated to source order.
- **`AsyncMapKeepOrder`** preserves source order — slow elements hold back faster downstream ones, but you get back the original sequence with parallel processing in between.

Use `AsyncMap(concurrency: N)` when order doesn't matter (typical for fan-out fetch + decode). Use `AsyncMapKeepOrder(concurrency: N)` when downstream expects source order (e.g. zipping with another sequence).

`AsyncTap` / `AsyncTapError` deliberately have no `concurrency:` parameter — observation stages run sequentially to keep side-effect ordering predictable.

## Conditional composition

The result builder doesn't support `if` / `else` / `guard` inside the body. Each `buildPartialBlock` call changes the pipeline's type, so heterogeneous branches can't be reconciled into a single uniform type without `buildEither`/`buildOptional` overloads — which we deliberately omit (they constrain both branches to identical types, which the heterogeneous-stage zoo makes unergonomic).

Compose conditionally **outside** the builder instead. Three patterns:

```swift
// 1. Pick a stage at the call site (homogeneous return type).
let scaler = needsDoubling
    ? Map { (n: Int) in n * 2 }
    : Map { (n: Int) in n }
let pipe = Pipe<Int, Never> {
    From(0..<10)
    scaler
}

// 2. Branch on the whole pipeline.
let base = Pipe<Int, Never> { From(0..<10) }
let final = needsDoubling
    ? Pipe<Int, Never> { FromResult(base); Map { (n: Int) in n * 2 } }
    : base

// 3. Build incrementally — each Pipe is itself an AsyncSequence<Result<…>>,
//    so re-wrap with `FromResult` to extend it.
var p = Pipe<Int, Never> { From(0..<10) }
if needsDoubling {
    p = Pipe<Int, Never> {
        FromResult(p)
        Map { (n: Int) in n * 2 }
    }
}
```

Pattern 1 is preferred when the conditional is local to one stage's behavior. Patterns 2 and 3 are for branching whole pipelines — both rely on `FromResult` to lift one pipeline into another's source.

## Design

A `Pipe<S, F>` is itself an `AsyncSequence<Result<S, F>>` — sinks are just iteration. Each `makeAsyncIterator()` reconstructs the underlying chain via a stored `@Sendable` builder, so pipeline values are re-iterable and free of shared mutable state.

Stages are typed by what they touch:

| Protocol | Value | Failure | Examples |
|---|---|---|---|
| `PipeStage` | bound | bound | `FlatMap`, `AsyncFlatMap` |
| `PipePolyStage` | bound | poly | `Map`, `Tap`, `Filter` |
| `PipePolyValueStage` | poly | bound | `MapError`, `TapError` |
| `PipeForwardingStage` | poly | poly | `Take`, `Drop` |
| `PipeFlatErrorStage` | bound | bound (in/out) | `FlatMapError`, `AsyncFlatMapError`, `Alt`, `OrElse`, `GetOrElse` |
| `PipeFoldStage` | A → R | `F` → `Never` | `Match`, `AsyncMatch` (fold both channels) |

The result builder has one `buildPartialBlock` overload per shape, plus a `Never`-widening overload so non-failable sources compose naturally with failure-introducing stages without an explicit `MapError`.

## Requirements

- Swift 6.2+, Swift language mode 6
- macOS 15+ / iOS 18+ (inherited from `fp-swift`)
- Strict concurrency: clean

## License

Copyright © 2026 Alexey Novikov. Released under the [MIT License](LICENSE).
