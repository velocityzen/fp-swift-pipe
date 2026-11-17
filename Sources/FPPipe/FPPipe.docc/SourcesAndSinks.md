# Sources and Sinks

Put elements into a pipe, and read the results out of it.

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

### Polling

fp-swift's `withRepeat(interval:)` calls a closure each time the reader asks for an element, at most once per `interval`. Like the sinks, it needs `import FP`. The first call runs at once, and each run of the pipe makes its own calls. It never ends by itself, so read it with `Take`, or with a sink that stops:

```swift
let statuses = Pipe<Status, Never> {
    From(withRepeat(interval: .seconds(5)) { await server.status() })
    Take(3)
}
```

When the closure returns a `Result`, read it with `FromResult`. Here the pipe polls until a check gives a ready status: `firstSuccess()` skips the failed checks, and `Filter` removes the statuses that are not ready:

```swift
let ready = await Pipe<Status, CheckError> {
    FromResult(withRepeat(interval: .seconds(1)) { await check() })
    Filter { (status: Status) in status.isReady }
}
.firstSuccess()
```

Cancelling the reading task stops the waits, and no new call starts.

## Reading results

A sink reads the pipe and returns the results. The sinks come from fp-swift and work on any `AsyncSequence` of `Result`s, so they need `import FP`.

| Sink                                       | Returns                                                                     | Stops reading             |
| ------------------------------------------ | --------------------------------------------------------------------------- | ------------------------- |
| `await pipe.toResult()`                    | `Result<[Success], Failure>`: all values, or the first error                | at the first failure      |
| `await pipe.toArray()`                     | `[Result<Success, Failure>]`: every element                                 | at the end                |
| `await pipe.separate()`                    | `(successes: [Success], failures: [Failure])`: every element, split in two  | at the end                |
| `await pipe.reduce(initial, combine)`      | `Result<U, Failure>`: all values combined into one, or the first error      | at the first failure      |
| `await pipe.reduce(into: initial, update)` | `Result<U, Failure>`: like `reduce`, but it changes one value in place      | at the first failure      |
| `await pipe.first()`                       | `Result<Success, Failure>?`: the first element                              | after the first element   |
| `await pipe.firstSuccess()`                | `Success?`                                                                  | at the first success      |
| `await pipe.firstError()`                  | `Failure?`                                                                  | at the first failure      |
| `await pipe.firstSuccessOrLastError()`     | `Result<Success, Failure>?`: the first success, or else the last failure    | at the first success      |
| `await pipe.last()`                        | `Result<Success, Failure>?`: the last element                               | at the end                |
| `await pipe.lastSuccess()`                 | `Success?`                                                                  | at the end                |
| `await pipe.lastError()`                   | `Failure?`                                                                  | at the end                |
| `for await element in pipe`                | one `Result` at a time                                                      | at `break`, or at the end |

When a sink stops reading, the pipe stops reading the source too. A sink that reads to the end never returns for a pipe that never ends.

```swift
let counts = Pipe<Int, Never> { From(1...4) }

await counts.reduce(0) { sum, n in sum + n }   // .success(10)
await counts.firstSuccess()                    // 1
await counts.lastSuccess()                     // 4
```

`reduce(into:)` builds a collection without copying it for each value:

```swift
let parity = await counts.reduce(into: [Bool: [Int]]()) { by, n in
    by[n.isMultiple(of: 2), default: []].append(n)
}
// .success([false: [1, 3], true: [2, 4]])
```

Here `numbers` is the pipe from <doc:GettingStarted#How-it-works>, which can fail. `separate()` reads past its failure, and `firstSuccessOrLastError()` stops at its first value:

```swift
await numbers.separate()
// (successes: [2, 6], failures: [.notANumber("x")])

await numbers.firstSuccessOrLastError()   // .success(2)
```

`for await` gives you each element as a `Result`:

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

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
