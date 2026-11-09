# Pattern Matching

Act only on the elements that match a pattern, like the cases of a `switch`.

Pattern-matching stages act only on the elements that match a pattern. Each stage is like one `case` of a `switch`. All other elements pass through unchanged, so the value and failure types do not change.

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

A stage finds its types from its own arguments, not from the stages around it. The pattern gives one type: the value type in `Match`, or the failure type in `MatchError`. (A type pattern, like `URLError.self`, only says what to cast to.) The handler must give the other type:

- In `.failure(AppError.http(204))`, `AppError` is the failure type.
- In `.success("busy, try later")`, `String` is the value type.

When the handler shows no type, write its return type: `MatchError(URLError.self) { e -> Result<Data, any Error> in … }`.

`Match(pattern) { … }` and `Match(onSuccess:onFailure:)` have the same name but do different jobs. The pattern form changes only the elements it matches. `Match(onSuccess:onFailure:)` changes every element of both channels into a new type.

`AsyncMatch` and `AsyncMatchError` take `concurrency:` (see <doc:AsyncAndParallelWork#Running-work-in-parallel>). The pattern itself is always sync and runs once for each element. Only matching elements go to the async handler. `AsyncTapMatch` and `AsyncTapMatchError` handle one element at a time, like `AsyncTap`.

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
