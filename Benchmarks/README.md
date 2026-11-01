# Benchmarks

Performance benchmarks for FPPipe, built on [ordo-one/benchmark](https://github.com/ordo-one/benchmark). They live in their own package so the library never depends on the benchmark package.

Every benchmark records wall clock and CPU time, instructions, malloc count and bytes, bytes not freed, object allocations, and retain and release counts. All values are **per run** of the pipeline under test: one iteration from the first element to the last.

## Suites

| Target | Covers |
| --- | --- |
| `StageBenchmarks` | `From`, the synchronous value stages (`Map`, `Filter`, `CompactMap`, `FlatMap`, `Tap`, `Take`, `Drop`), a ten-stage chain, and the failure stages (`FromResult`, `MapError`, `TapError`, `FlatMapError`, `GetOrElse`, `Alt`, `Match`) |
| `AsyncStageBenchmarks` | the `Async*` stages, sequential and at `concurrency: 8`, and `FlatMapSequence` / `FlatMapAsyncSequence` at several `bufferSize`s |
| `PipeBenchmarks` | the sources (`From` over an array or an async sequence, `FromAsync`, `OpenPipe`), the sinks (`toResult`, `toArray`, `reduce`), and the fixed cost of a short run |

Most benchmarks run at `n: 16` and `n: 1024` elements, to separate the fixed cost of a run from its cost per element. Transforms are trivial (`$0 &* 2`), so the numbers are the pipeline's own overhead rather than the work it runs.

Entries marked `(baseline)` are the hand-written floor for the same job (a plain `for` loop, a plain `for await`, the ten stages of `Long chain` as one loop, a raw `withTaskGroup`). The gap between a baseline and the pipeline next to it is what an optimization can win back.

## Running

Requires Swift 6.3 or later. Malloc statistics come from the benchmark package's malloc interposer, so there is nothing to install.

Run everything from this directory:

```bash
swift package benchmark
```

Run one suite, or the benchmarks whose names match a regular expression. The expression must match the whole name without its `(n: …, concurrency: …)` tags, so a filter selects every size of a benchmark:

```bash
swift package benchmark --target StageBenchmarks
```

```bash
swift package benchmark --filter "Async.*"
```

List the benchmarks without running them:

```bash
swift package benchmark list
```

## Comparing before and after a change

Record a baseline, make the change, then compare the new code against it:

```bash
swift package --allow-writing-to-package-directory benchmark baseline update main
```

```bash
swift package benchmark baseline compare main
```

Baselines are saved in `.benchmarkBaselines/`, which is not committed: they are only comparable on the machine that recorded them.

Malloc, object-allocation and ARC counts are stable from run to run, so they are the numbers to watch first. Times move with the machine's load; close other work before comparing them.

`baseline check` fails only when one of those four counts gets worse; times and the other metrics are reported by `compare` but never fail a check. The tolerance is `checkedTolerance` in [BenchmarkSupport.swift](Sources/BenchmarkSupport/BenchmarkSupport.swift).

```bash
swift package benchmark baseline check main
```

## Benchmarking against a local fp-swift

The benchmarks build FPPipe from `..` and fp-swift from the version `Package.resolved` pins. To measure a change to fp-swift before it is released, point this package at a local checkout, and undo it when done:

```bash
swift package edit fp-swift --path ../../fp-swift
```

```bash
swift package unedit fp-swift
```

## Continuous integration

The [Benchmarks workflow](../.github/workflows/benchmarks.yml) benchmarks every push to `main` against the commit it was pushed on top of, and every pull request against its base branch. The job summary lists every metric's change, and the job fails when an allocation or ARC count regresses.

## Finding where allocations come from

Print the call stacks behind each benchmark's allocations, most frequent first:

```bash
swift package benchmark --target StageBenchmarks --filter "Map" --allocation-stacks
```

## Profiling

Each suite is an ordinary executable that runs all of its benchmarks when launched on its own, so it can be profiled in Instruments or debugged in lldb:

```bash
swift build -c release --product StageBenchmarks
```
