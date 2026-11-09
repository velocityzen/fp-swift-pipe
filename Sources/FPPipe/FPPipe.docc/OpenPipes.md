# Open Pipes

Build a pipe once, and run it with different inputs.

Start a pipe with `From(T.self)` to build it without its input. You get an `OpenPipe`, which works like a function: call it with a sequence, and it returns a normal `Pipe`. Build it once and use it with many inputs, for example once per request or once per test.

```swift
let double = OpenPipe {
    From(Int.self)
    Filter { (n: Int) in n > 0 }
    Map { (n: Int) in n * 2 }
}

await double([1, -1, 2]).toResult()   // .success([2, 4])
await double(1...3).toResult()        // .success([2, 4, 6])
```

The input can be any `Sequence` or `AsyncSequence` of the input type. The rules in <doc:SourcesAndSinks#Running-a-pipe-more-than-once> apply to the input too. `FromAsync(T.self)` works the same as `From(T.self)`.

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

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
