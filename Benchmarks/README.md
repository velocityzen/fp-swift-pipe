# Benchmarks

Performance benchmarks for FPPipe, built on [ordo-one/benchmark](https://github.com/ordo-one/benchmark). They live in their own package so the library never depends on the benchmark package.

Every benchmark records wall clock and CPU time, instructions, malloc count and bytes, bytes not freed, object allocations, and retain and release counts. All values are **per run** of the pipeline under test: one iteration from the first element to the last.

## Suites

| Target | Covers |
| --- | --- |
| `StageBenchmarks` | `From`, the synchronous value stages (`Map`, `Filter`, `CompactMap`, `FlatMap`, `Tap`, `Take`, `Drop`), a ten-stage chain, the failure stages (`FromResult`, `MapError`, `TapError`, `FlatMapError`, `GetOrElse`, `Alt`, `Match`), and the pattern-matching stages (`Match (pattern)`, `MatchError`, `TapMatch`, `TapMatchError`, plus its dynamic-cast form) |
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

`baseline check` fails only when one of those four counts gets worse; times and the other metrics are reported by `compare` but never fail a check. A count may move by 5% before it fails. The exceptions are the runs where tasks race the reader on fp-swift's concurrent map: the stages at `concurrency: 8`, and the flat-map stages with a `bufferSize` above 1, whose read-ahead task fills a buffer. A stage starts a worker only when none is idle, so a run starts one to eight of them, two allocations and four mallocs each, and its results buffer grows as far as results pile up. Both depend on scheduling: the same code has moved `AsyncMatchError` from 25 to 27 allocations and the flat-maps from 14 to 16 locally, and the flat-maps' retains by 7% on CI. These runs get an absolute allowance instead, the most their counts can move: seven workers' worth, a growth of the buffer for every doubling up to `n`, and half a retain or release per element. That still fails one more allocation, retain or release per element at `n: 1024`. The tolerances are `CountTolerance` in [BenchmarkSupport.swift](Sources/BenchmarkSupport/BenchmarkSupport.swift).

`baseline check` compares and prints raw samples. A sample at `n: 16` covers 1,000 runs, so its counts and allowances there are 1,000 times a run's.

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

The [Benchmarks workflow](../.github/workflows/benchmarks.yml) benchmarks every push to `main` against the commit it was pushed on top of, and every pull request against its base branch. The job summary lists every metric's change, and the job fails when an allocation or ARC count regresses; the job log shows every count that crossed its threshold. An improvement passes.

`baseline check base head` builds nothing: it compares two recorded baselines and takes its thresholds from the base one. So a change to the thresholds or the tolerances applies from the commit after it, and the commit that makes it is still checked against the old ones.

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
