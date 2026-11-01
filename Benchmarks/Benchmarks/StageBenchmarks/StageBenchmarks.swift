import Benchmark
import BenchmarkSupport
import FPPipe

// The synchronous stages. Each pipeline reads `0..<n`, or ready-made results
// (every third a failure) for the stages that act on failures, and is run to
// its end, so the numbers are the pipeline's own cost per run. "(baseline)"
// entries do the same job by hand.

let benchmarks: @Sendable () -> Void = {
    for n in [16, 1024] {
        let tags = ["n": "\(n)"]
        let results = ResultSequence(n)

        // MARK: Baselines

        Benchmark("for loop (baseline)", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                var count = 0
                for value in 0..<n {
                    blackHole(value &* 2)
                    count += 1
                }
                blackHole(count)
            }
        }

        Benchmark("for await (baseline)", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                var count = 0
                for await value in CountingSequence(n) {
                    blackHole(value &* 2)
                    count += 1
                }
                blackHole(count)
            }
        }

        // The ten stages of `Long chain`, written as one loop.
        Benchmark("Long chain (baseline)", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                var count = 0
                for value in 0..<n {
                    let doubled = (value + 1) * 2
                    guard doubled.isMultiple(of: 4) else { continue }
                    let shifted = doubled &- 3
                    guard shifted > 0 else { continue }
                    blackHole(String(shifted / 2).count &+ 1)
                    count += 1
                }
                blackHole(count)
            }
        }

        // MARK: Value stages

        let from = Pipe<Int, Never> { From(0..<n) }
        Benchmark("From", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(from))
            }
        }

        let map = Pipe<Int, Never> {
            From(0..<n)
            Map { (value: Int) in value &* 2 }
        }
        Benchmark("Map", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(map))
            }
        }

        let filter = Pipe<Int, Never> {
            From(0..<n)
            Filter { (value: Int) in value.isMultiple(of: 2) }
        }
        Benchmark("Filter", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(filter))
            }
        }

        let compactMap = Pipe<Int, Never> {
            From(0..<n)
            CompactMap { (value: Int) -> Int? in value.isMultiple(of: 2) ? value : nil }
        }
        Benchmark("CompactMap", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(compactMap))
            }
        }

        let flatMap = Pipe<Int, BenchError> {
            From(0..<n)
            FlatMap { (value: Int) -> IntResult in .success(value &* 2) }
        }
        Benchmark("FlatMap", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(flatMap))
            }
        }

        let tap = Pipe<Int, Never> {
            From(0..<n)
            Tap { (value: Int) in blackHole(value) }
        }
        Benchmark("Tap", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(tap))
            }
        }

        let take = Pipe<Int, Never> {
            From(0..<n)
            Take(n / 2)
        }
        Benchmark("Take", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(take))
            }
        }

        let drop = Pipe<Int, Never> {
            From(0..<n)
            Drop(n / 2)
        }
        Benchmark("Drop", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(drop))
            }
        }

        let longChain = Pipe<Int, Never> {
            From(0..<n)
            Map { (value: Int) in value + 1 }
            Map { (value: Int) in value * 2 }
            Filter { (value: Int) in value.isMultiple(of: 4) }
            Map { (value: Int) in value &- 3 }
            CompactMap { (value: Int) -> Int? in value > 0 ? value : nil }
            Map { (value: Int) in value / 2 }
            Map { (value: Int) in String(value) }
            Map { (string: String) in string.count }
            Map { (value: Int) in value &+ 1 }
        }
        Benchmark("Long chain", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(longChain))
            }
        }

        // MARK: Failure stages

        let fromResult = Pipe<Int, BenchError> { FromResult(results) }
        Benchmark("FromResult", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(fromResult))
            }
        }

        let mapError = Pipe<Int, OtherError> {
            FromResult(results)
            MapError { (_: BenchError) in OtherError() }
        }
        Benchmark("MapError", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(mapError))
            }
        }

        let tapError = Pipe<Int, BenchError> {
            FromResult(results)
            TapError { (error: BenchError) in blackHole(error) }
        }
        Benchmark("TapError", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(tapError))
            }
        }

        let flatMapError = Pipe<Int, OtherError> {
            FromResult(results)
            FlatMapError { (_: BenchError) -> Result<Int, OtherError> in .success(0) }
        }
        Benchmark("FlatMapError", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(flatMapError))
            }
        }

        let getOrElse = Pipe<Int, Never> {
            FromResult(results)
            GetOrElse { (_: BenchError) in 0 }
        }
        Benchmark("GetOrElse", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(getOrElse))
            }
        }

        let alt = Pipe<Int, BenchError> {
            FromResult(results)
            Alt { IntResult.success(0) }
        }
        Benchmark("Alt", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(alt))
            }
        }

        let match = Pipe<Int, Never> {
            FromResult(results)
            Match(onSuccess: { (value: Int) in value }, onFailure: { (_: BenchError) in 0 })
        }
        Benchmark("Match", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(match))
            }
        }
    }
}
