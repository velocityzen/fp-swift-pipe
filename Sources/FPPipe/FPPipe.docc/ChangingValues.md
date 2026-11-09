# Changing Values

Change, remove and watch the values that pass through a pipe.

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

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
