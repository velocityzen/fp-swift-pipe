# Stage Reference

Every stage, the closure it takes, and what it does.

`A` and `B` are value types. `F` is a failure type.

| Stage                                  | Closure                                      | What it does                                                                                        |
| -------------------------------------- | -------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `Map` / `AsyncMap`                     | `(A) -> B`                                   | Changes each value.                                                                                 |
| `FlatMap` / `AsyncFlatMap`             | `(A) -> Result<B, F>`                        | Changes each value. It can turn a value into a failure.                                             |
| `AsyncMapKeepOrder`                    | `(A) async -> B`                             | Like `AsyncMap` with `concurrency:`, but keeps the source order.                                    |
| `AsyncFlatMapKeepOrder`                | `(A) async -> Result<B, F>`                  | Like `AsyncFlatMap` with `concurrency:`, but keeps the source order.                                |
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
| `Match` / `AsyncMatch` (pattern)       | `(pattern) { … } -> Result<A, F>`            | Replaces only the successes that match a pattern.                              |
| `MatchError` / `AsyncMatchError`       | `(pattern) { … } -> Result<A, F>`            | Recovers from, or fails again, only the failures that match a pattern.                              |
| `TapMatch` / `AsyncTapMatch`           | `(pattern) { … } -> Void`                    | Runs a side effect only for the successes that match a pattern.                                     |
| `TapMatchError` / `AsyncTapMatchError` | `(pattern) { … } -> Void`                    | Runs a side effect only for the failures that match a pattern.                                      |

All `Async` stages except the tap stages take `concurrency:`. <doc:PatternMatching> explains patterns.

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
