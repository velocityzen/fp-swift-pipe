# Contributing to FPPipe

## Setup

FPPipe is a plain SwiftPM package — clone and build. Requirements: Swift 6.2+ (language mode 6), macOS 15+.

```sh
swift build
swift test                      # full suite, including benchmarks
swift test --skip Benchmark     # fast iteration without wall-clock benchmarks
```

CI runs both linters in strict mode; run them locally before pushing (`brew install swift-format swiftlint`):

```sh
swift-format lint --recursive --strict --configuration .swift-format Sources Tests
swiftlint --strict
```

## Project layout

| Directory | Contents |
|---|---|
| `Sources/FPPipe/Core/` | `Pipe`, `OpenPipe`, the stage protocols, `AnyAsyncSequence` |
| `Sources/FPPipe/Builder/` | the `@resultBuilder` that folds a source and stages into a `Pipe` |
| `Sources/FPPipe/Sources/` | `From`, `Defer`, `FromAsync`, `Success`, `Failure`, `Empty` |
| `Sources/FPPipe/Stages/` | one file per stage |
| `Sources/FPPipe/Sinks/` | `toResult`, `toArray`, `reduce`, `first…` |
| `Sources/FPPipe/Concurrency/` | bounded-concurrency map helpers used by the `Async*` stages |

## Design rules

- **Result-only.** Errors travel in the `Result.failure` channel; `throws` never crosses a stage boundary. No `TryMap`-style adapters — throwing code is bridged at call sites (see the Readme section "Working with throwing code").
- **Sequential by default.** Any knob that lets work run ahead of the consumer (`concurrency:`, `bufferSize:`) defaults to 1; parallelism and prefetch are explicit opt-ins at the call site.
- **Strict concurrency clean.** Everything public is `Sendable`, stage closures are `@Sendable`, and the package must build warning-free in Swift 6 language mode on all supported platforms (macOS, iOS, tvOS, watchOS, visionOS, and Linux).
- **Lazy and re-iterable.** Pipes store `@Sendable` builders and reconstruct the chain per iteration; a stage must not hold shared mutable state or assume it is iterated once.
- **Formatting is owned by `swift-format`** (`.swift-format`); SwiftLint covers bug-catching rules (`.swiftlint.yml`). Don't hand-fight either — CI runs both strict.

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

2. **Implement** in a new file under `Sources/FPPipe/Stages/`: an internal `struct …Stage` conforming to the protocol, plus a public UpperCamelCase factory function returning `some <Protocol>`. Any existing stage shows the shape.
3. **Builder overloads** are needed only when introducing a *new protocol shape* (rare) — a stage conforming to an existing protocol composes in the builder for free. The overload matrix (first-step identity, accumulated × stage, open-pipe variants, widening, optional) is documented at the top of `Sources/FPPipe/Builder/PipeBuilder.swift`; new overloads go in the matching `MARK` section.
4. **Test** in `Tests/FPPipeTests/Stages/<Stage>Tests.swift`: the success path, the failure path (pass-through or handling), and at least one composition with another stage. Stages with a `concurrency:` parameter also need cancellation coverage in `Tests/FPPipeTests/Concurrency/`.
5. **Document**: a doc comment on the public factory — including caveats such as ordering, buffering, and cancellation behavior — and a row in the Readme stage catalog.

## Benchmarks

`Tests/FPPipeTests/Benchmarks/` runs as part of the regular test suite, asserting conservative wall-clock upper bounds. If a benchmark fails on CI but passes locally, raise the bound — don't remove the assertion.
