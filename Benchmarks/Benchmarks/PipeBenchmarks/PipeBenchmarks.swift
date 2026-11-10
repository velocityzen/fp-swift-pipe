import Benchmark
import BenchmarkSupport
import FPPipe

// Sources, sinks and the fixed cost of a run. A pipe rebuilds its stages on every run, so
// a short run of a long pipe is mostly setup.

let benchmarks: @Sendable () -> Void = {
    // MARK: Setup

    let success = Pipe<Int, Never> { Success(1) }
    Benchmark("Success first", configuration: .pipe()) { benchmark in
        for _ in benchmark.scaledIterations {
            blackHole(await success.first())
        }
    }

    let tenStages = Pipe<Int, Never> {
        From(CollectionOfOne(1))
        Map { (value: Int) in value &+ 1 }
        Map { (value: Int) in value &+ 1 }
        Map { (value: Int) in value &+ 1 }
        Map { (value: Int) in value &+ 1 }
        Map { (value: Int) in value &+ 1 }
        Map { (value: Int) in value &+ 1 }
        Map { (value: Int) in value &+ 1 }
        Map { (value: Int) in value &+ 1 }
        Map { (value: Int) in value &+ 1 }
        Map { (value: Int) in value &+ 1 }
    }
    Benchmark("Ten stages over one element", configuration: .pipe()) { benchmark in
        for _ in benchmark.scaledIterations {
            blackHole(await drain(tenStages))
        }
    }

    for n in [16, 1024] {
        let tags = ["n": "\(n)"]
        let array = Array(0..<n)

        // MARK: Sources

        let fromArray = Pipe<Int, Never> { From(array) }
        Benchmark("From array", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(fromArray))
            }
        }

        let fromAsyncSequence = Pipe<Int, Never> { From(CountingSequence(n)) }
        Benchmark("From async sequence", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(fromAsyncSequence))
            }
        }

        let fromAsync = Pipe<Int, Never> { FromAsync { CountingSequence(n) } }
        Benchmark("FromAsync", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(fromAsync))
            }
        }

        let open = OpenPipe {
            From(Int.self)
            Map { (value: Int) in value &* 2 }
        }
        Benchmark("OpenPipe", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await drain(open(0..<n)))
            }
        }

        // MARK: Sinks

        let mapped = Pipe<Int, Never> {
            From(0..<n)
            Map { (value: Int) in value &* 2 }
        }

        Benchmark("toResult", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await mapped.toResult())
            }
        }

        Benchmark("toArray", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await mapped.toArray())
            }
        }

        Benchmark("reduce", configuration: .pipe(tags)) { benchmark in
            for _ in benchmark.scaledIterations {
                blackHole(await mapped.reduce(0) { $0 &+ $1 })
            }
        }
    }
}
