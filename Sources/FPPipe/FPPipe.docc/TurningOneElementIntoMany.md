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

Both stages take `bufferSize:`. The default is 1: each new element is made only when the reader asks for it, in the reader's task, and nothing is stored. With a larger number, a separate task makes up to `bufferSize` elements ahead of the reader, and then waits. Use it when the reader takes elements in bursts.

Even a very large inner sequence, like a range of a million numbers, is never stored in memory all at once. When the reader stops or is cancelled, the stage stops too.

`bufferSize:` must be 1 or more.

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
