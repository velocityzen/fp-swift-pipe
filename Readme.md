# FPPipe

[![Swift](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fvelocityzen%2Ffp-swift-pipe%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/velocityzen/fp-swift-pipe)
[![Platforms](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fvelocityzen%2Ffp-swift-pipe%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/velocityzen/fp-swift-pipe)
[![Documentation](https://img.shields.io/badge/documentation-DocC-purple)](https://swiftpackageindex.com/velocityzen/fp-swift-pipe/documentation/fppipe)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

FPPipe builds async data pipelines in Swift. A pipe reads elements from a source, passes them through stages, and gives you the results. Every element is a `Result`, so an error is a value that moves through the pipe like any other element. FPPipe is built on [fp-swift](https://github.com/velocityzen/fp-swift).

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Sources](#sources)
  - [Running a pipe more than once](#running-a-pipe-more-than-once)
- [Reading results](#reading-results)
- [Changing values](#changing-values)
- [Handling failures](#handling-failures)
- [Calling throwing code](#calling-throwing-code)
- [Pattern matching](#pattern-matching)
- [Async stages](#async-stages)
- [Running work in parallel](#running-work-in-parallel)
  - [Cancellation](#cancellation)
- [Turning one element into many](#turning-one-element-into-many)
- [Open pipes](#open-pipes)
- [Conditions inside a pipe](#conditions-inside-a-pipe)
- [A complete example](#a-complete-example)
- [Stage reference](#stage-reference)
- [Design](#design)
- [License](#license)

## Installation

Requirements:

- Swift 6.3 or later, in Swift 6 language mode
- macOS, iOS, tvOS, watchOS or visionOS 26 or later, or Linux

Add the package to `Package.swift`:

```swift
dependencies: [
    .package(
        url: "https://github.com/velocityzen/fp-swift-pipe",
        from: "2.1.0"
    ),
],
```

Then add the `FPPipe` product to your target:

```swift
.target(
    name: "MyApp",
    dependencies: [.product(name: "FPPipe", package: "fp-swift-pipe")]
),
```

## Quick start

```swift
import FPPipe

let pipe = Pipe<Int, Never> {
    From(1...6)                                 // 1, 2, 3, 4, 5, 6
    Filter { (n: Int) in n.isMultiple(of: 2) }  // 2, 4, 6
    Map { (n: Int) in n * 10 }                  // 20, 40, 60
}

let result = await pipe.toResult()  // .success([20, 40, 60])
```

`Pipe<Int, Never>` gives `Int` values and cannot fail (`Never`). Creating a pipe does no work. It runs each time you read it, here with `toResult()`.

## How it works

A pipe has three kinds of parts:

| Part   | Job                                                 | Examples                  |
| ------ | --------------------------------------------------- | ------------------------- |
| Source | Puts elements into the pipe. It always comes first. | `From`, `FromAsync`       |
| Stage  | Changes, removes or watches elements.               | `Map`, `Filter`, `Tap`    |
| Sink   | Reads the results out of the pipe.                  | `toResult()`, `for await` |

Each element is a `Result<Success, Failure>`: a value (a success) or an error (a failure). Most stages work on one side only:

- Value stages, like `Map` and `Filter`, change successes. Failures pass through.
- Error stages, like `MapError` and `FlatMapError`, change failures. Successes pass through.

So one failure does not stop the other elements:

```swift
enum ParseError: Error, Equatable {
    case notANumber(String)
}

let numbers = Pipe<Int, ParseError> {
    From(["1", "x", "3"])
    FlatMap { (text: String) -> Result<Int, ParseError> in
        guard let n = Int(text) else {
            return .failure(.notANumber(text))
        }
        return .success(n)
    }
    Map { (n: Int) in n * 2 }   // runs for 1 and 3 only
}

await numbers.toArray()
// [.success(2), .failure(.notANumber("x")), .success(6)]

await numbers.toResult()  // .failure(.notANumber("x"))
```

`toArray()` returns every element. `toResult()` stops at the first failure.

`From` cannot fail. `FlatMap` is the first stage that can, so it sets the failure type to `ParseError`. You do not need a stage to change `Never` into `ParseError`.

## Sources

A source is always the first line of a pipe.

| Source                                                   | Gives                                                                                           |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `From(sequence)`                                         | Each element of a `Sequence` or `AsyncSequence`, as a success.                                  |
| `FromResult(sequence)`                                   | Each element of an `AsyncSequence` of `Result`s, unchanged. It can read another pipe.           |
| `Success(value)` or `Of(value)`                          | One success.                                                                                    |
| `Failure(error, valueType:)`                             | One failure.                                                                                    |
| `Empty(valueType:failureType:)`                          | No elements.                                                                                    |
| `Defer { … }` or `DeferResult { … }`                     | The elements of a sequence that the closure creates. The closure runs on every run of the pipe. |
| `FromAsync { await … }` or `FromAsyncResult { await … }` | Like `Defer`, but the closure is `async`. It runs when the first element is needed.             |

```swift
From([1, 2, 3])                         // an array
From(1...3)                             // a range
From(stream)                            // any AsyncSequence
FromResult(numbers)                     // the elements of another pipe
Success(42)                             // one value
FromAsync { await database.userIDs() }  // made by async code
```

### Running a pipe more than once

A pipe does not save its results. Each time you read it, it reads its source again. An array or a range gives the same elements every time.

A sequence that can be read only once, like a stored `AsyncStream`, is empty the second time, with no error. Create it inside `Defer` instead, so every run gets a new one:

```swift
let pipe = Pipe<Int, Never> {
    Defer { makeStream() }   // a new stream on every run
    Map { (n: Int) in n + 1 }
}
```

## Reading results

A sink reads the pipe and returns the results.

| Sink                                  | Returns                                                      | Stops reading             |
| ------------------------------------- | ------------------------------------------------------------ | ------------------------- |
| `await pipe.toResult()`               | `Result<[Success], Failure>`: all values, or the first error | at the first failure      |
| `await pipe.toArray()`                | `[Result<Success, Failure>]`: every element                  | at the end                |
| `await pipe.reduce(initial, combine)` | `Result<U, Failure>`: all values combined into one           | at the first failure      |
| `await pipe.first()`                  | `Result<Success, Failure>?`: the first element               | after the first element   |
| `await pipe.firstSuccess()`           | `Success?`                                                   | at the first success      |
| `await pipe.firstError()`             | `Failure?`                                                   | at the first failure      |
| `for await element in pipe`           | one `Result` at a time                                       | at `break`, or at the end |

When a sink stops reading, the pipe stops reading the source too.

```swift
let counts = Pipe<Int, Never> { From(1...4) }

await counts.reduce(0) { sum, n in sum + n }   // .success(10)
await counts.firstSuccess()                    // 1
```

`for await` gives you each element as a `Result`. Here `numbers` is the pipe from [How it works](#how-it-works), which can fail:

```swift
for await element in numbers {
    switch element {
        case .success(let n): print("value:", n)
        case .failure(let error): print("error:", error)
    }
}
// value: 2
// error: notANumber("x")
// value: 6
```

## Changing values

These stages work on successes. Failures pass through them unchanged.

| Stage        | Closure       | What it does                                                 |
| ------------ | ------------- | ------------------------------------------------------------ |
| `Map`        | `(A) -> B`    | Changes each value.                                          |
| `Filter`     | `(A) -> Bool` | Keeps the values for which the closure returns `true`.       |
| `CompactMap` | `(A) -> B?`   | Changes each value, and removes the `nil` results.           |
| `Tap`        | `(A) -> Void` | Runs a side effect, like logging. The value does not change. |
| `Take(n)`    | none          | Keeps the first `n` elements, then stops reading.            |
| `Drop(n)`    | none          | Skips the first `n` elements.                                |

`Take` and `Drop` count failures too, like `prefix(_:)` and `dropFirst(_:)` on an `AsyncSequence`.

```swift
let labels = Pipe<String, Never> {
    From(["1", "two", "3", "4", "5"])
    CompactMap { (text: String) in Int(text) }  // 1, 3, 4, 5
    Drop(1)                                     // 3, 4, 5
    Take(2)                                     // 3, 4
    Tap { (n: Int) in print("got", n) }         // prints 3 and 4
    Map { (n: Int) in "#\(n)" }                 // "#3", "#4"
}

await labels.toResult()  // .success(["#3", "#4"])
```

`Int("two")` is `nil`, so `CompactMap` removes it.

## Handling failures

A failure enters the pipe in one of two ways: from a source of `Result`s, like `FromResult`, or from a stage that returns a `Result`, like `FlatMap`.

| Stage                         | Closure                   | What it does                                                               |
| ----------------------------- | ------------------------- | -------------------------------------------------------------------------- |
| `FlatMap`                     | `(A) -> Result<B, F>`     | Changes a value. It can also turn the value into a failure.                |
| `MapError`                    | `(F1) -> F2`              | Changes an error, or its type.                                             |
| `TapError`                    | `(F) -> Void`             | Runs a side effect for each failure. The failure does not change.          |
| `Alt`                         | `() -> Result<A, F>`      | Replaces each failure with a new `Result`. It does not see the error.      |
| `FlatMapError`                | `(F1) -> Result<A, F2>`   | Sees the error and returns a `Result`: a value to recover, or a failure.   |
| `GetOrElse`                   | `(F) -> A`                | Replaces each failure with a value. After it, the pipe cannot fail.        |
| `Match(onSuccess:onFailure:)` | `(A) -> R` and `(F) -> R` | Turns values and errors into one type `R`. After it, the pipe cannot fail. |

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

To handle only some errors, use [`MatchError`](#pattern-matching).

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

A throwing `AsyncSequence` works the same way: turn it into a sequence of `Result`s first, then read it with `FromResult`.

## Pattern matching

Pattern-matching stages act only on the elements that match a pattern, like one `case` of a `switch`. All other elements pass through unchanged, so the value and failure types stay the same.

| Stage           | Looks at  | The handler returns                                             |
| --------------- | --------- | --------------------------------------------------------------- |
| `Match`         | successes | `Result<A, F>`: `.success` for a new value, `.failure` to fail  |
| `MatchError`    | failures  | `Result<A, F>`: `.success` to recover, `.failure` to fail again |
| `TapMatch`      | successes | nothing: it runs a side effect, and the element passes through  |
| `TapMatchError` | failures  | nothing: it runs a side effect, and the element passes through  |

Each stage also has an async version: `AsyncMatch`, `AsyncMatchError`, `AsyncTapMatch` and `AsyncTapMatchError`.

The examples below use these types:

```swift
enum Event: Equatable, Sendable {
    case login(String)
    case logout

    // The user name, for a login only.
    var loggedInUser: String? {
        if case .login(let user) = self { user } else { nil }
    }
}

enum AppError: Error, Equatable, Sendable {
    case rateLimited
    case http(Int)

    var isRetryable: Bool { self == .rateLimited }

    // The code, for an HTTP error only.
    var statusCode: Int? {
        if case .http(let code) = self { code } else { nil }
    }
}
```

A pattern has one of four forms. The handler comes after it, usually as a trailing closure:

```swift
// 1. A value (Equatable). The handler gets no argument,
//    because you already know what matched.
TapMatch(Event.logout) { print("logged out") }
MatchError(AppError.rateLimited) { .success("busy, try later") }

// 2. A type. It matches with a cast (`as?`), and the handler gets
//    the cast value. Use it when the failure type is `any Error`.
TapMatchError(URLError.self) { error in print(error.code) }

// 3. A condition (`where:`): a closure or a key path to a Bool.
TapMatchError(where: \AppError.isRetryable) { error in print(error) }
TapMatch(where: { (n: Int) in n > 1_000 }, then: { n in print(n) })

// 4. A payload (`extract:`): a closure or key path that returns
//    a value for a match, or nil.
TapMatch(extract: \Event.loggedInUser) { user in print(user) }
TapMatchError(extract: \AppError.statusCode) { code in print(code) }
```

When the pattern is a closure, pass the handler as `then:`.

Put several stages in a row to write the cases of a `switch`:

```swift
let pages = Pipe<String, AppError> {
    From(urls)
    AsyncFlatMap { (url: URL) in await download(url) }
    Match("") { .failure(AppError.http(204)) }
    MatchError(AppError.rateLimited) { .success("busy, try later") }
    TapMatchError(extract: \AppError.statusCode) { code in print(code) }
}
```

`download(url)` returns a `Result<String, AppError>`. `Match("")` turns an empty page into a failure. `MatchError` recovers from rate limits, and `TapMatchError` prints the code of every other HTTP error.

A stage gets its types from its own arguments only. The pattern gives one type: the value type in `Match`, or the failure type in `MatchError`. The handler must give the other: in `.failure(AppError.http(204))` it is the failure type, and in `.success("busy, try later")` it is the value type. When the handler shows no type, write its return type: `MatchError(URLError.self) { e -> Result<Data, any Error> in … }`.

`Match(pattern) { … }` changes only the elements that match. `Match(onSuccess:onFailure:)` is a different stage: it changes every element into a new type.

`AsyncMatch` and `AsyncMatchError` take `concurrency:` (see [Running work in parallel](#running-work-in-parallel)). The pattern is always sync, and only matching elements go to the async handler. `AsyncTapMatch` and `AsyncTapMatchError` handle one element at a time.

## Async stages

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

## Turning one element into many

`FlatMapSequence` turns each value into a `Sequence` and passes on all of its elements. `FlatMapAsyncSequence` does the same with an `AsyncSequence` that does not throw.

```swift
let repeated = Pipe<Int, Never> {
    From([1, 2, 3])
    FlatMapSequence { (n: Int) in Array(repeating: n, count: n) }
}

await repeated.toResult()   // .success([1, 2, 2, 3, 3, 3])
```

Both stages take `bufferSize:`, which must be 1 or more. With the default of 1, each element is made only when the reader asks for it. With a larger number, a separate task makes up to `bufferSize` elements ahead of the reader. Use it when the reader takes elements in bursts.

The inner sequence is never stored all at once, even when it is very large. When the reader stops, the stage stops too.

## Open pipes

Start a pipe with `From(T.self)` to build it without its input. You get an `OpenPipe`, which works like a function: call it with a sequence, and it returns a normal `Pipe`. Build it once and use it with many inputs.

```swift
let double = OpenPipe {
    From(Int.self)
    Filter { (n: Int) in n > 0 }
    Map { (n: Int) in n * 2 }
}

await double([1, -1, 2]).toResult()   // .success([2, 4])
await double(1...3).toResult()        // .success([2, 4, 6])
```

The input can be any `Sequence` or `AsyncSequence` of the input type. The rules in [Running a pipe more than once](#running-a-pipe-more-than-once) apply to the input too. `FromAsync(T.self)` works the same as `From(T.self)`.

When the input is a sequence of `Result`s, start with `FromResult(Value.self, Error.self)`. Failures in the input go straight into the failure channel:

```swift
let tenTimes = OpenPipe {
    FromResult(Int.self, ParseError.self)
    Map { (n: Int) in n * 10 }
}

let input: [Result<Int, ParseError>] = [
    .success(1),
    .failure(.notANumber("x")),
]

await tenTimes(input).toArray()
// [.success(10), .failure(.notANumber("x"))]
```

All stages work in open pipes.

## Conditions inside a pipe

You can use `if`, `if`/`else` and `switch` inside a pipe.

With `if`/`else` and `switch`, every branch must have the same stage type. Below, each `if` branch is a `Map<Int, Int>`, and each `switch` case is a `Filter<Int>`. A `Map` in one branch and a `Filter` in another does not compile.

```swift
let pipe = Pipe<Int, Never> {
    From(0..<10)

    if needsDoubling {
        Map { (n: Int) in n * 2 }
    } else {
        Map { (n: Int) in n + 1 }
    }

    switch mode {
        case .even: Filter { (n: Int) in n.isMultiple(of: 2) }
        case .odd: Filter { (n: Int) in !n.isMultiple(of: 2) }
        case .all: Filter { (_: Int) in true }
    }
}
```

`if` without `else` works only for stages that keep the element types the same, because the pipe must have the same type when the condition is false. These stages work:

- `Take`, `Drop`, `Tap`, `TapError` and their async versions
- every [pattern-matching](#pattern-matching) stage
- `Map`, `Filter`, `FlatMap` and their async versions, when the value type does not change
- `Alt`, `FlatMapError` and their async versions, when the failure type does not change

```swift
let pipe = Pipe<Int, Never> {
    From(0..<10)
    if logging { Tap { (n: Int) in print(n) } }
    if skipFirst { Drop(2) }
    if evenOnly { Filter { (n: Int) in n.isMultiple(of: 2) } }
}

let parsed = Pipe<Int, ParseError> {
    From(lines)
    FlatMap(parse)  // String -> Result<Int, ParseError>
    if lenient {
        Alt { Result<Int, ParseError>.success(0) }  // keeps ParseError
    }
}
```

A stage that changes a type, like `Map { (n: Int) in String(n) }`, needs an `else` branch with a stage of the same type.

`for` loops do not work inside a pipe. Combine the loop's work into one closure outside the pipe:

```swift
// Instead of `for f in transforms { Map(f) }`:
let combined: @Sendable (Int) -> Int = { n in
    transforms.reduce(n) { value, transform in transform(value) }
}

let pipe = Pipe<Int, Never> {
    From(0..<10)
    Map(combined)
}
```

When branches need different stage types, choose outside the pipe. `FromResult` can read one pipe as the source of another:

```swift
let base = Pipe<Int, Never> { From(0..<10) }

let final = needsDoubling
    ? Pipe<Int, Never> {
        FromResult(base)
        Map { (n: Int) in n * 2 }
    }
    : base
```

## A complete example

This pipe downloads several articles at the same time and keeps their order. It decodes them, removes the ones that are not interesting, and recovers from rate limits:

```swift
import FP
import FPPipe
import Foundation

enum ArticleError: Error, Sendable {
    case network(any Error)
    case decode(Data)
    case rateLimited

    var isRateLimited: Bool {
        if case .rateLimited = self { true } else { false }
    }
}

struct Article: Decodable, Sendable {
    let id: String
    let isInteresting: Bool

    static let placeholder = Article(id: "-", isInteresting: false)
}

func fetchArticles(
    _ urls: [URL]
) async -> Result<[Article], ArticleError> {
    let pipe = Pipe<Article, ArticleError> {
        // 1. Read the URLs.
        From(urls)

        // 2. Download up to 4 at a time, and keep the source order.
        AsyncFlatMap(concurrency: 4, ordered: true) { (url: URL) in
            await Result.fromAsync {
                try await URLSession.shared.data(from: url).0
            }
            .mapError(ArticleError.network)
        }

        // 3. Decode. A decode error becomes a failure.
        FlatMap { (data: Data) in
            Result {
                try JSONDecoder().decode(Article.self, from: data)
            }
            .mapError { _ in ArticleError.decode(data) }
        }

        // 4. Remove articles that are not interesting.
        Filter { (article: Article) in article.isInteresting }

        // 5. Log every failure.
        TapError { (error: ArticleError) in print("failed:", error) }

        // 6. Replace rate-limit failures with a placeholder.
        //    Other failures stay.
        MatchError(where: \ArticleError.isRateLimited) { _ in
            .success(Article.placeholder)
        }
    }

    // 7. Return all articles, or the first failure.
    return await pipe.toResult()
}
```

## Stage reference

`A` and `B` are value types. `F` is a failure type.

| Stage                                  | Closure                                      | What it does                                                                                        |
| -------------------------------------- | -------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `Map` / `AsyncMap`                     | `(A) -> B`                                   | Changes each value.                                                                                 |
| `FlatMap` / `AsyncFlatMap`             | `(A) -> Result<B, F>`                        | Changes each value. It can turn a value into a failure.                                             |
| `Filter` / `AsyncFilter`               | `(A) -> Bool`                                | Keeps the values for which the closure returns `true`.                                              |
| `CompactMap` / `AsyncCompactMap`       | `(A) -> B?`                                  | Changes each value, and removes `nil` results.                                                      |
| `Take` / `Drop`                        | `Int`                                        | Keeps or skips the first `n` elements.                                                              |
| `FlatMapSequence`                      | `(A) -> some Sequence<B>`                    | Turns each value into many values. Takes `bufferSize:`.                                             |
| `FlatMapAsyncSequence`                 | `(A) -> some AsyncSequence<B>`               | Turns each value into many values, from an async sequence that does not throw. Takes `bufferSize:`. |
| `Tap` / `AsyncTap`                     | `(A) -> Void`                                | Runs a side effect for each value.                                                                  |
| `MapError`                             | `(F1) -> F2`                                 | Changes each error.                                                                                 |
| `TapError` / `AsyncTapError`           | `(F) -> Void`                                | Runs a side effect for each failure.                                                                |
| `Alt` / `AsyncAlt`                     | `() -> Result<A, F>`                         | Replaces each failure with a new `Result`. It does not see the error.                               |
| `FlatMapError` / `AsyncFlatMapError`   | `(F1) -> Result<A, F2>`                      | Recovers from a failure or fails again, possibly with another error type.                           |
| `GetOrElse` / `AsyncGetOrElse`         | `(F) -> A`                                   | Replaces each failure with a value. The pipe cannot fail after it.                                  |
| `Match` / `AsyncMatch` (fold)          | `onSuccess: (A) -> R`, `onFailure: (F) -> R` | Turns every element into one type `R`. The pipe cannot fail after it.                               |
| `Match` / `AsyncMatch` (pattern)       | `(pattern) { … } -> Result<A, F>`            | Replaces only the successes that match a [pattern](#pattern-matching).                              |
| `MatchError` / `AsyncMatchError`       | `(pattern) { … } -> Result<A, F>`            | Recovers from, or fails again, only the failures that match a pattern.                              |
| `TapMatch` / `AsyncTapMatch`           | `(pattern) { … } -> Void`                    | Runs a side effect only for the successes that match a pattern.                                     |
| `TapMatchError` / `AsyncTapMatchError` | `(pattern) { … } -> Void`                    | Runs a side effect only for the failures that match a pattern.                                      |

All `Async` stages except the tap stages take `concurrency:`, `ordered:` and `bufferLimit:`.

## Design

A `Pipe<Success, Failure>` is an `AsyncSequence` of `Result<Success, Failure>`. A pipe stores how to build its stages, not their state. Each run builds a new chain of stages, so pipes are `Sendable` and share no mutable state.

Each stage protocol says which types a stage fixes and which it leaves open ("any"):

| Protocol              | Value type | Failure type      | Examples                                                              |
| --------------------- | ---------- | ----------------- | --------------------------------------------------------------------- |
| `PipeStage`           | fixed      | fixed             | `FlatMap`, `AsyncFlatMap`, `Match(pattern)`                           |
| `PipePolyStage`       | fixed      | any               | `Map`, `Tap`, `Filter`, `TapMatch`                                    |
| `PipePolyValueStage`  | any        | fixed             | `MapError`, `TapError`, `TapMatchError`                               |
| `PipeForwardingStage` | any        | any               | `Take`, `Drop`, `TapMatch(T.self)`, `TapMatchError(E.self)`           |
| `PipeFlatErrorStage`  | fixed      | fixed, can change | `FlatMapError`, `AsyncFlatMapError`, `Alt`, `GetOrElse`, `MatchError` |
| `PipeFoldStage`       | `A` to `R` | `F` to `Never`    | `Match(onSuccess:onFailure:)`, `AsyncMatch(onSuccess:onFailure:)`     |

The result builder has a `buildPartialBlock` overload for each protocol. One more overload widens a `Never` failure type, so a source that cannot fail works with stages that can.

Sources, stages and sinks are `@inlinable` and use concrete types, so the compiler specializes the whole pipe. There is no allocation for each element, and each element costs one call for each stage. With `concurrency: 1` and `bufferSize: 1`, stages run in the reader's task, with no child tasks or buffers.

The benchmarks measure allocations, reference counting and time for every source, stage and sink. They are in their own package, in [`Benchmarks/`](Benchmarks/README.md):

```bash
cd Benchmarks && swift package benchmark
```

## License

Copyright © 2026 Alexey Novikov. Released under the [MIT License](LICENSE).
