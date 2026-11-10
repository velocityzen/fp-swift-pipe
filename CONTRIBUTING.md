# Contributing to FPPipe

## Setup

FPPipe is a plain SwiftPM package: clone it and build. Requirements: Swift 6.3+ (language mode 6), macOS 26+.

```sh
swift build
swift test                      # full suite, including benchmarks
swift test --skip Benchmark     # fast iteration without wall-clock benchmarks
```

CI runs both linters in strict mode; run them locally before pushing (`brew install swift-format swiftlint`):

```sh
swift-format lint --recursive --strict --configuration .swift-format \
    Sources Tests Scripts Benchmarks/Package.swift Benchmarks/Sources Benchmarks/Benchmarks
swiftlint --strict
```

## Project layout

| Directory | Contents |
|---|---|
| `Sources/FPPipe/Core/` | `Pipe`, `OpenPipe`, the stage protocols, `AnyAsyncSequence`, the step iterators most stages run on |
| `Sources/FPPipe/Builder/` | the `@resultBuilder` that folds a source and stages into a `Pipe` |
| `Sources/FPPipe/Sources/` | `From`, `Defer`, `FromAsync`, `Success`, `Failure`, `Empty` |
| `Sources/FPPipe/Stages/` | one file per stage |
| `Sources/FPPipe/Sinks/` | `toResult`, `toArray`, `reduce`, `first…` |
| `Sources/FPPipe/Concurrency/` | how the pipe uses fp-swift's ordered and unordered `compactMap`: `compactMapAsyncIterator` and the passthroughs of the `Async*` stages at `concurrency > 1`, and the read-ahead of the fan-out stages at `bufferSize > 1` |

## Design rules

- **Result-only.** Errors travel in the `Result.failure` channel; `throws` never crosses a stage boundary. No `TryMap`-style adapters. Throwing code is turned into a `Result` where it is called (see the Readme section "Calling throwing code").
- **Sequential by default.** Any knob that lets work run ahead of the consumer (`concurrency:`, `bufferSize:`) defaults to 1; parallelism and prefetch are explicit opt-ins at the call site.
- **Strict concurrency clean.** Everything public is `Sendable`, stage closures are `@Sendable`, and the package must build warning-free in Swift 6 language mode on all supported platforms (macOS, iOS, tvOS, watchOS, visionOS, and Linux).
- **Lazy and re-iterable.** Pipes store `@Sendable` builders and reconstruct the chain per iteration; a stage must not hold shared mutable state or assume it is iterated once.
- **As cheap as a hand-written loop.** Sources, stages and sinks are `@inlinable` over `@usableFromInline` internals, so pipelines specialize in the module that declares them. A stage builds its own iterator from concrete types instead of wrapping the standard library's `map` / `compactMap` / `prefix` / `dropFirst`, which are opaque across modules and cost an allocation and an unspecialized call per element. Check a change with the [benchmarks](Benchmarks/README.md): allocation and ARC counts must not regress.
- **Formatting is owned by `swift-format`** (`.swift-format`); SwiftLint covers bug-catching rules (`.swiftlint.yml`). Don't hand-fight either. CI runs both in strict mode.

## Adding a stage

1. **Pick the protocol** by what the stage must know about types:

   | Protocol | Value | Failure | Use for |
   |---|---|---|---|
   | `PipeStage` | bound | bound | transforms that can fail (`FlatMap`) |
   | `PipePolyStage` | bound | polymorphic | success-side transforms (`Map`, `Filter`, `Tap`) |
   | `PipePolyValueStage` | polymorphic | bound | failure-side transforms (`MapError`, `TapError`) |
   | `PipeForwardingStage` | polymorphic | polymorphic | element-count stages (`Take`, `Drop`) |
   | `PipeFlatErrorStage` | bound | bound in/out | recovery (`FlatMapError`, `Alt`, `GetOrElse`) |
   | `PipeFoldStage` | A → R | F → Never | folds of both channels (`Match`) |

2. **Implement** in a new file under `Sources/FPPipe/Stages/`: a `@usableFromInline` internal `struct …Stage` conforming to the protocol, plus an `@inlinable` public UpperCamelCase factory function returning `some <Protocol>`. Mark the stage's stored properties `@usableFromInline` and its `init` and `attach` `@inlinable`. A stage that turns each element into at most one element is a `…Step` struct conforming to `PipeStep` (or `AsyncPipeStep`), run by `StepIterator` (or `AsyncStepIterator`); `attach` returns `.iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }`. Other stages get their own iterator. `Map` and `AsyncMap` show both shapes.
3. **Builder overloads** are needed only when introducing a *new protocol shape* (rare). A stage that conforms to an existing protocol works in the builder with no new overload. The overload matrix (first-step identity, accumulated × stage, open-pipe variants, widening, optional) is documented at the top of `Sources/FPPipe/Builder/PipeBuilder.swift`; new overloads go in the matching `MARK` section.
4. **Test** in `Tests/FPPipeTests/Stages/<Stage>Tests.swift`: the success path, the failure path (pass-through or handling), and at least one composition with another stage. Stages with a `concurrency:` parameter also take `ordered:` and `bufferLimit:`, pass all three to `compactMapAsyncIterator`, and need ordering and cancellation tests in `Tests/FPPipeTests/Concurrency/`.
5. **Document**: a doc comment on the public factory (with caveats such as ordering, buffering and cancellation), a row in the Readme's "Stage reference" table, and a link in the Topics of `Sources/FPPipe/FPPipe.docc/FPPipe.md`. Then make the DocC articles again (see [Documentation](#documentation)).

## Documentation

`Readme.md` is the source of the documentation. A script makes the DocC articles in `Sources/FPPipe/FPPipe.docc` and the quick start on its landing page from it, so don't edit those by hand. Edit the Readme, then run:

```sh
swift Scripts/make-docc-articles.swift            # write the articles
swift Scripts/make-docc-articles.swift --check    # change nothing; fail when an article is out of date
```

- **Articles.** Each article is made from one or more `## ` sections of the Readme, as `articles` at the top of the script lists them. The script fails on a section that no article uses. A new article also needs a `<doc:>` link in the Topics of `FPPipe.md`.
- **Links.** DocC can't follow links to Readme sections (`#…`) or to relative paths. The script fails on them; add the DocC link that replaces each one to `linkEdits` in the script.
- **Line length.** Keep code lines in the Readme at 72 characters or fewer. DocC cuts off longer lines, and the script fails on them.
- **Tests.** Every Readme example is also a test in `Tests/FPPipeTests/Readme/`. When you change an example, change its test too.

To see the result, open the package in Xcode and choose Product → Build Documentation.

## Benchmarks

`Tests/FPPipeTests/Benchmarks/` runs as part of the regular test suite, asserting conservative wall-clock upper bounds. If a benchmark fails on CI but passes locally, raise the bound. Don't remove the assertion.

The [`Benchmarks/`](Benchmarks/README.md) package measures time, allocations and ARC traffic for every source, stage and sink with [ordo-one/benchmark](https://github.com/ordo-one/benchmark). Record a baseline before a change and compare after it; a new stage gets a benchmark next to its siblings. CI fails a pull request whose allocation or retain/release counts regress.
