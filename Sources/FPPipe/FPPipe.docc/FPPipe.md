# ``FPPipe``

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
- ``OpenResultSource``
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
- ``AsyncFlatMapKeepOrder(_:)``
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
- ``AsyncMatch(concurrency:onSuccess:onFailure:)``

### Pattern matching

Act only on the elements a pattern matches — an `Equatable` value, a type, a predicate (`where:`) or a payload extractor (`extract:`). Everything else passes through.

- ``Match(_:then:)-(Value,_)``
- ``Match(_:then:)-(T.Type,_)``
- ``Match(where:then:)``
- ``Match(extract:then:)``
- ``AsyncMatch(_:concurrency:then:)-(Value,_,_)``
- ``AsyncMatch(_:concurrency:then:)-(T.Type,_,_)``
- ``AsyncMatch(where:concurrency:then:)``
- ``AsyncMatch(extract:concurrency:then:)``
- ``MatchError(_:then:)-(F,_)``
- ``MatchError(_:then:)-(E.Type,_)``
- ``MatchError(where:then:)``
- ``MatchError(extract:then:)``
- ``AsyncMatchError(_:concurrency:then:)-(F,_,_)``
- ``AsyncMatchError(_:concurrency:then:)-(E.Type,_,_)``
- ``AsyncMatchError(where:concurrency:then:)``
- ``AsyncMatchError(extract:concurrency:then:)``
- ``TapMatch(_:then:)->PipePolyStage<Value,Value>``
- ``TapMatch(_:then:)->PipeForwardingStage``
- ``TapMatch(where:then:)``
- ``TapMatch(extract:then:)``
- ``AsyncTapMatch(_:then:)->PipePolyStage<Value,Value>``
- ``AsyncTapMatch(_:then:)->PipeForwardingStage``
- ``AsyncTapMatch(where:then:)``
- ``AsyncTapMatch(extract:then:)``
- ``TapMatchError(_:then:)->PipePolyValueStage<F,F>``
- ``TapMatchError(_:then:)->PipeForwardingStage``
- ``TapMatchError(where:then:)``
- ``TapMatchError(extract:then:)``
- ``AsyncTapMatchError(_:then:)->PipePolyValueStage<F,F>``
- ``AsyncTapMatchError(_:then:)->PipeForwardingStage``
- ``AsyncTapMatchError(where:then:)``
- ``AsyncTapMatchError(extract:then:)``

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

