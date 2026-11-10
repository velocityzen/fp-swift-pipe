# ``FPPipe``

Build async data pipelines in which every element is a `Result`.

## Overview

FPPipe builds async data pipelines in Swift. A pipe reads elements from a source, passes them through stages, and gives you the results. Every element is a `Result`, so an error is a value that moves through the pipe like any other element. FPPipe is built on [fp-swift](https://github.com/velocityzen/fp-swift).

<!-- Quick start: made from Readme.md by Scripts/make-docc-articles.swift. -->

```swift
import FPPipe

let pipe = Pipe<Int, Never> {
    From(1...6)                                 // 1, 2, 3, 4, 5, 6
    Filter { (n: Int) in n.isMultiple(of: 2) }  // 2, 4, 6
    Map { (n: Int) in n * 10 }                  // 20, 40, 60
}

let result = await pipe.toResult()  // .success([20, 40, 60])
```

`Pipe<Int, Never>` gives `Int` values and cannot fail (`Never`). Creating a pipe does no work. It runs each time you read it, here with `toResult()`.


<!-- End of quick start. -->

The articles below start with the basics and end with advanced topics. Start with <doc:GettingStarted>.

## Topics

### Essentials

- <doc:GettingStarted>
- <doc:SourcesAndSinks>
- <doc:ChangingValues>
- <doc:HandlingFailures>

### Advanced Topics

- <doc:PatternMatching>
- <doc:AsyncAndParallelWork>
- <doc:TurningOneElementIntoMany>
- <doc:OpenPipes>
- <doc:ConditionsInsideAPipe>

### Examples and Reference

- <doc:ACompleteExample>
- <doc:StageReference>
- <doc:Design>

### Pipes

- ``Pipe``
- ``PipeBuilder``
- ``OptionalStage``
- ``AnyAsyncSequence``

### Sources

- ``From(_:)-2jxd7``
- ``From(_:)-7jlr8``
- ``FromResult(_:)``
- ``Success(_:)``
- ``Of(_:)``
- ``Failure(_:valueType:)``
- ``Empty(valueType:failureType:)``
- ``Defer(_:)-8qg2i``
- ``Defer(_:)-787aw``
- ``DeferResult(_:)``
- ``FromAsync(_:)-54dgo``
- ``FromAsync(_:)-faxo``
- ``FromAsyncResult(_:)``

### Reading Results

- ``Pipe/toResult()``
- ``Pipe/toArray()``
- ``Pipe/reduce(_:_:)``
- ``Pipe/first()``
- ``Pipe/firstSuccess()``
- ``Pipe/firstError()``

### Changing Values

- ``Map(_:)``
- ``AsyncMap(concurrency:ordered:bufferLimit:_:)``
- ``Filter(_:)``
- ``AsyncFilter(concurrency:ordered:bufferLimit:_:)``
- ``CompactMap(_:)``
- ``AsyncCompactMap(concurrency:ordered:bufferLimit:_:)``
- ``Tap(_:)``
- ``AsyncTap(_:)``
- ``Take(_:)``
- ``Drop(_:)``

### Handling Failures

- ``FlatMap(_:)``
- ``AsyncFlatMap(concurrency:ordered:bufferLimit:_:)``
- ``MapError(_:)``
- ``TapError(_:)``
- ``AsyncTapError(_:)``
- ``Alt(_:)``
- ``AsyncAlt(concurrency:ordered:bufferLimit:_:)``
- ``FlatMapError(_:)``
- ``AsyncFlatMapError(concurrency:ordered:bufferLimit:_:)``
- ``GetOrElse(_:)``
- ``AsyncGetOrElse(concurrency:ordered:bufferLimit:_:)``
- ``Match(onSuccess:onFailure:)``
- ``AsyncMatch(concurrency:ordered:bufferLimit:onSuccess:onFailure:)``

### Pattern Matching

A pattern is an `Equatable` value, a type, a condition (`where:`) or a payload (`extract:`). Elements that do not match pass through unchanged.

- ``Match(_:then:)-(Value,_)``
- ``Match(_:then:)-(T.Type,_)``
- ``Match(where:then:)``
- ``Match(extract:then:)``
- ``AsyncMatch(_:concurrency:ordered:bufferLimit:then:)-(Value,_,_,_,_)``
- ``AsyncMatch(_:concurrency:ordered:bufferLimit:then:)-(T.Type,_,_,_,_)``
- ``AsyncMatch(where:concurrency:ordered:bufferLimit:then:)``
- ``AsyncMatch(extract:concurrency:ordered:bufferLimit:then:)``
- ``MatchError(_:then:)-(F,_)``
- ``MatchError(_:then:)-(E.Type,_)``
- ``MatchError(where:then:)``
- ``MatchError(extract:then:)``
- ``AsyncMatchError(_:concurrency:ordered:bufferLimit:then:)-(F,_,_,_,_)``
- ``AsyncMatchError(_:concurrency:ordered:bufferLimit:then:)-(E.Type,_,_,_,_)``
- ``AsyncMatchError(where:concurrency:ordered:bufferLimit:then:)``
- ``AsyncMatchError(extract:concurrency:ordered:bufferLimit:then:)``
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

### Turning One Element into Many

- ``FlatMapSequence(bufferSize:_:)``
- ``FlatMapAsyncSequence(bufferSize:_:)``

### Open Pipes

- ``OpenPipe``
- ``From(_:)->OpenSource<Input>``
- ``FromAsync(_:)->OpenSource<Input>``
- ``FromResult(_:_:)``
- ``FromAsyncResult(_:_:)``
- ``OpenSource``
- ``OpenResultSource``

### Stage Protocols

- ``PipeSource``
- ``PipeStage``
- ``PipePolyStage``
- ``PipePolyValueStage``
- ``PipeForwardingStage``
- ``PipeFlatErrorStage``
- ``PipeFoldStage``

### Deprecated

- ``AsyncMapKeepOrder(concurrency:_:)``
- ``AsyncFlatMapKeepOrder(concurrency:_:)``
