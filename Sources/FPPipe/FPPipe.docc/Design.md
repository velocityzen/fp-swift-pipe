# Design

How a pipe is built, and why it is fast.

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

The benchmarks measure allocations, reference counting and time for every source, stage and sink. They are in their own package, in [`Benchmarks/`](https://github.com/velocityzen/fp-swift-pipe/blob/main/Benchmarks/README.md):

```bash
cd Benchmarks && swift package benchmark
```

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
