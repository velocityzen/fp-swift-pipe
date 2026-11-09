# Sources and Sinks

Put elements into a pipe, and read the results out of it.

## Sources

A source is always the first line of a pipe.

| Source                                                   | Gives                                                                                                                                                                       |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `From(sequence)`                                         | Each element of a `Sequence` or `AsyncSequence`, as a success.                                                                                                              |
| `FromResult(sequence)`                                   | Each element of an `AsyncSequence` of `Result`s, unchanged. A pipe is such a sequence, so a pipe can read another pipe.                                                     |
| `Success(value)` or `Of(value)`                          | One success.                                                                                                                                                                |
| `Failure(error, valueType:)`                             | One failure.                                                                                                                                                                |
| `Empty(valueType:failureType:)`                          | No elements.                                                                                                                                                                |
| `Defer { … }` or `DeferResult { … }`                     | The elements of a sequence that the closure creates. The closure runs again on every run of the pipe.                                                                       |
| `FromAsync { await … }` or `FromAsyncResult { await … }` | Like `Defer`, but the closure is `async`, for example to open a database cursor. It runs when the first element is needed. The sequence is then read one element at a time. |

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

A sequence that can be read only once, like a stored `AsyncStream`, is empty the second time, and there is no error. To avoid this, create the sequence inside `Defer`, so every run gets a new one:

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

When a sink stops reading, the pipe stops working too. No more elements are read from the source.

```swift
let counts = Pipe<Int, Never> { From(1...4) }

await counts.reduce(0) { sum, n in sum + n }   // .success(10)
await counts.firstSuccess()                    // 1
```

`for await` gives you each element as a `Result`. Here `numbers` is the pipe from <doc:GettingStarted#How-it-works>, which can fail:

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
