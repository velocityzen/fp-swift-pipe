# ``PipelineKit``

A small, opinionated library for composing async, error-aware pipelines in Swift.

## Overview

A ``Pipe`` is a re-iterable description of an async stream of `Result<Success, Failure>`. Stages compose in a `@resultBuilder` DSL declared with ``PipeBuilder``. Errors live in the `Result.failure` channel — the library is `Result`-only by design, and Swift `throws` never crosses a stage boundary; throwing code is bridged at the closure level using stdlib `Result(catching:)` or fp-swift's `Result.fromAsync { … }`.

```swift
let pipe = Pipe<Item, AppError> {
    From(urls)
    AsyncFlatMap { url in await fetch(url) }
    FlatMap { data in Result { try decode(data) }.mapError(AppError.parse) }
    Filter { $0.isInteresting }
    Tap { item in log("got \(item.id)") }
}

let result = await pipe.toResult()
```

Stages are typed by what they touch — five protocols cover every shape, from fully-bound `(Value, Failure)` transforms down to forwarding stages polymorphic in both axes.

## Topics

### Core types

- ``Pipe``
- ``OpenPipe``
- ``OpenSource``
- ``PipeBuilder``
- ``AnyAsyncSequence``

### Stage protocols

- ``PipeSource``
- ``PipeStage``
- ``PipePolyStage``
- ``PipePolyValueStage``
- ``PipeForwardingStage``
- ``PipeFlatErrorStage``

### Sources

- ``From(_:)-1``
- ``FromResult(_:)``
- ``Defer(_:)-1``
- ``DeferResult(_:)``
- ``FromAsync(_:)-1``
- ``FromAsync(_:)-2``
- ``FromAsyncResult(_:)``
- ``Success(_:)``
- ``Of(_:)``
- ``Failure(_:valueType:)``
- ``Empty(valueType:failureType:)``

### Transforming successes

- ``Map(_:)``
- ``AsyncMap(_:)``
- ``AsyncMapKeepOrder(_:)``
- ``FlatMap(_:)``
- ``AsyncFlatMap(_:)``
- ``CompactMap(_:)``
- ``AsyncCompactMap(_:)``

### Filtering and slicing

- ``Filter(_:)``
- ``AsyncFilter(_:)``
- ``Take(_:)``
- ``Drop(_:)``

### Fan-out

- ``FlatMapSequence(_:)``
- ``FlatMapAsyncSequence(_:)``

### Failure handling

- ``MapError(_:)``
- ``Alt(_:)``
- ``AsyncAlt(_:)``
- ``FlatMapError(_:)``
- ``AsyncFlatMapError(_:)``
- ``GetOrElse(_:)``
- ``AsyncGetOrElse(_:)``

### Folding

- ``Match(onSuccess:onFailure:)``
- ``AsyncMatch(onSuccess:onFailure:)``

### Observation

- ``Tap(_:)``
- ``AsyncTap(_:)``
- ``TapError(_:)``
- ``AsyncTapError(_:)``

### Sinks

- ``Pipe/toResult()``
- ``Pipe/toArray()``
- ``Pipe/reduce(_:_:)``
- ``Pipe/first()``
- ``Pipe/firstSuccess()``
- ``Pipe/firstError()``

