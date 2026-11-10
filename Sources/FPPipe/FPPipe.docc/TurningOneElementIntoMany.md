# Turning One Element into Many

Turn each value into a sequence of values, and control how far the work runs ahead.

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

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
