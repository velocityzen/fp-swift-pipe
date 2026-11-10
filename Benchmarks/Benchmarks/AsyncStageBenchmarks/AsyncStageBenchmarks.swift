import Benchmark
import BenchmarkSupport
import FPPipe

// The async and expanding stages. The closures return at once, so the numbers show each
// stage's own cost. "(baseline)" entries do the same job by hand.

func twice(_ value: Int) async -> Int {
    value &* 2
}

let benchmarks: @Sendable () -> Void = {
    for n in [16, 1024] {
        let tags = ["n": "\(n)"]
        let results = ResultSequence(n)
        // Switching tasks for every element makes a large run slow enough to need no `.kilo`.
        let scaling: BenchmarkScalingFactor = n > 64 ? .one : .kilo

        // MARK: Baselines

        Benchmark("for await (baseline)", configuration: .pipe(tags, scaling: scaling)) {
            benchmark in
            for _ in benchmark.scaledIterations {
                var count = 0
                for await value in CountingSequence(n) {
                    blackHole(await twice(value))
                    count += 1
                }
                blackHole(count)
            }
        }

        Benchmark(
            "withTaskGroup 8 at a time (baseline)",
            configuration: .pipe(tags, scaling: scaling)
        ) { benchmark in
            for _ in benchmark.scaledIterations {
                let count = await withTaskGroup(of: Int.self) { group in
                    var inFlight = 0
                    var count = 0
                    for await value in CountingSequence(n) {
                        if inFlight == 8, let result = await group.next() {
                            blackHole(result)
                            count += 1
                            inFlight -= 1
                        }
                        group.addTask { await twice(value) }
                        inFlight += 1
                    }
                    for await result in group {
                        blackHole(result)
                        count += 1
                    }
                    return count
                }
                blackHole(count)
            }
        }

        // MARK: Stages with a concurrency limit

        for concurrency in [1, 8] {
            let tags = ["n": "\(n)", "concurrency": "\(concurrency)"]

            let asyncMap = Pipe<Int, Never> {
                From(0..<n)
                AsyncMap(concurrency: concurrency) { (value: Int) async in value &* 2 }
            }
            Benchmark("AsyncMap", configuration: .pipe(tags, scaling: scaling)) { benchmark in
                for _ in benchmark.scaledIterations {
                    blackHole(await drain(asyncMap))
                }
            }

            let asyncMapOrdered = Pipe<Int, Never> {
                From(0..<n)
                AsyncMap(concurrency: concurrency, ordered: true) { (value: Int) async in
                    value &* 2
                }
            }
            Benchmark("AsyncMap ordered", configuration: .pipe(tags, scaling: scaling)) {
                benchmark in
                for _ in benchmark.scaledIterations {
                    blackHole(await drain(asyncMapOrdered))
                }
            }

            // A limit below `concurrency - 1` also lowers how many closures run at once.
            let asyncMapOrderedLimited = Pipe<Int, Never> {
                From(0..<n)
                AsyncMap(concurrency: concurrency, ordered: true, bufferLimit: 4) {
                    (value: Int) async in value &* 2
                }
            }
            Benchmark(
                "AsyncMap ordered",
                configuration: .pipe(tags.merging(["bufferLimit": "4"]) { $1 }, scaling: scaling)
            ) { benchmark in
                for _ in benchmark.scaledIterations {
                    blackHole(await drain(asyncMapOrderedLimited))
                }
            }

            let asyncFilter = Pipe<Int, Never> {
                From(0..<n)
                AsyncFilter(concurrency: concurrency) { (value: Int) async in
                    value.isMultiple(of: 2)
                }
            }
            Benchmark("AsyncFilter", configuration: .pipe(tags, scaling: scaling)) {
                benchmark in
                for _ in benchmark.scaledIterations {
                    blackHole(await drain(asyncFilter))
                }
            }

            let asyncFlatMap = Pipe<Int, BenchError> {
                From(0..<n)
                AsyncFlatMap(concurrency: concurrency) { (value: Int) async -> IntResult in
                    .success(value &* 2)
                }
            }
            Benchmark("AsyncFlatMap", configuration: .pipe(tags, scaling: scaling)) {
                benchmark in
                for _ in benchmark.scaledIterations {
                    blackHole(await drain(asyncFlatMap))
                }
            }

            let asyncMatchPattern = Pipe<Int, BenchError> {
                FromResult(results)
                AsyncMatch(
                    where: { (value: Int) in value.isMultiple(of: 2) },
                    concurrency: concurrency,
                    then: { value async -> IntResult in .success(value &* 2) }
                )
            }
            Benchmark("AsyncMatch (pattern)", configuration: .pipe(tags, scaling: scaling)) {
                benchmark in
                for _ in benchmark.scaledIterations {
                    blackHole(await drain(asyncMatchPattern))
                }
            }

            let asyncMatchError = Pipe<Int, BenchError> {
                FromResult(results)
                AsyncMatchError(BenchError(), concurrency: concurrency) { () async -> IntResult in
                    .success(0)
                }
            }
            Benchmark("AsyncMatchError", configuration: .pipe(tags, scaling: scaling)) {
                benchmark in
                for _ in benchmark.scaledIterations {
                    blackHole(await drain(asyncMatchError))
                }
            }
        }

        // MARK: Sequential stages

        let asyncCompactMap = Pipe<Int, Never> {
            From(0..<n)
            AsyncCompactMap { (value: Int) async -> Int? in
                value.isMultiple(of: 2) ? value : nil
            }
        }
        Benchmark("AsyncCompactMap", configuration: .pipe(tags, scaling: scaling)) {
            benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(asyncCompactMap))
            }
        }

        let asyncTap = Pipe<Int, Never> {
            From(0..<n)
            AsyncTap { (value: Int) async in blackHole(value) }
        }
        Benchmark("AsyncTap", configuration: .pipe(tags, scaling: scaling)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(asyncTap))
            }
        }

        let asyncFlatMapError = Pipe<Int, OtherError> {
            FromResult(results)
            AsyncFlatMapError { (_: BenchError) async -> Result<Int, OtherError> in .success(0) }
        }
        Benchmark("AsyncFlatMapError", configuration: .pipe(tags, scaling: scaling)) {
            benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(asyncFlatMapError))
            }
        }

        let asyncMatch = Pipe<Int, Never> {
            FromResult(results)
            AsyncMatch(
                onSuccess: { (value: Int) async in value },
                onFailure: { (_: BenchError) async in 0 }
            )
        }
        Benchmark("AsyncMatch", configuration: .pipe(tags, scaling: scaling)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(asyncMatch))
            }
        }

        let asyncTapMatch = Pipe<Int, Never> {
            From(0..<n)
            AsyncTapMatch(
                where: { (value: Int) in value.isMultiple(of: 2) },
                then: { value async in
                    blackHole(value)
                }
            )
        }
        Benchmark("AsyncTapMatch", configuration: .pipe(tags, scaling: scaling)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(asyncTapMatch))
            }
        }

        let asyncTapMatchError = Pipe<Int, BenchError> {
            FromResult(results)
            AsyncTapMatchError(BenchError()) { blackHole(0) }
        }
        Benchmark("AsyncTapMatchError", configuration: .pipe(tags, scaling: scaling)) {
            benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(asyncTapMatchError))
            }
        }

        // MARK: Expanding stages

        for bufferSize in [1, 16] {
            let tags = ["n": "\(n)", "bufferSize": "\(bufferSize)"]
            let tolerance: CountTolerance = bufferSize > 1 ? .racing : .standard

            let flatMapSequence = Pipe<Int, Never> {
                From(0..<n)
                FlatMapSequence(bufferSize: bufferSize) { (value: Int) in value..<(value + 2) }
            }
            Benchmark(
                "FlatMapSequence",
                configuration: .pipe(tags, scaling: scaling, tolerance: tolerance)
            ) { benchmark in
                for _ in benchmark.scaledIterations {
                    blackHole(await drain(flatMapSequence))
                }
            }

            let flatMapAsyncSequence = Pipe<Int, Never> {
                From(0..<n)
                FlatMapAsyncSequence(bufferSize: bufferSize) { (_: Int) in CountingSequence(2) }
            }
            Benchmark(
                "FlatMapAsyncSequence",
                configuration: .pipe(tags, scaling: scaling, tolerance: tolerance)
            ) { benchmark in
                for _ in benchmark.scaledIterations {
                    blackHole(await drain(flatMapAsyncSequence))
                }
            }
        }
    }
}
