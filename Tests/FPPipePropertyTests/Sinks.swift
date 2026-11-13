// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe
import Testing

/// How the test reads the pipe.
enum Sink: Sendable, Hashable, CaseIterable {
    case toArray, toResult, first, firstSuccess, firstError, reduce, prefix
}

/// How fast the reader takes elements, and from where.
enum Consumer: Sendable, Hashable, CaseIterable {
    /// Reads each element as soon as it can.
    case prompt
    /// Suspends between reads, so stages run ahead and fill their buffers.
    case slow
    /// Reads from an actor, so the pipe runs off that actor.
    case onActor
}

/// Reads a pipe on an actor.
actor Drain {
    func toArray(_ pipe: Pipe<Int, Fail>) async -> [Element] {
        var elements: [Element] = []
        for await element in pipe {
            elements.append(element)
        }
        return elements
    }
}

/// What a sink returned.
enum SinkOutput: Hashable, Sendable {
    case array([Element])
    case result(Result<[Int], Fail>)
    case first(Element?)
    case firstSuccess(Int?)
    case firstError(Fail?)
    case reduce(Result<Int, Fail>)

    var elements: [Element]? {
        if case .array(let elements) = self { elements } else { nil }
    }
}

extension Sink {
    /// Reads the real pipe.
    func run(
        _ pipe: Pipe<Int, Fail>,
        _ consumer: Consumer = .prompt,
        prefixLength: Int = 0
    ) async -> SinkOutput {
        switch self {
            case .toArray: return .array(await Self.readAll(pipe, consumer))
            case .toResult: return .result(await pipe.toResult())
            case .first: return .first(await pipe.first())
            case .firstSuccess: return .firstSuccess(await pipe.firstSuccess())
            case .firstError: return .firstError(await pipe.firstError())
            case .reduce: return .reduce(await pipe.reduce(0) { $0 &* 31 &+ $1 })
            case .prefix:
                var elements: [Element] = []
                for await element in pipe {
                    if elements.count == prefixLength { break }
                    elements.append(element)
                }
                return .array(elements)
        }
    }

    /// Reads by hand, and checks that the iterator keeps returning `nil`
    /// after the end.
    private static func readAll(_ pipe: Pipe<Int, Fail>, _ consumer: Consumer) async -> [Element] {
        if consumer == .onActor { return await Drain().toArray(pipe) }
        var iterator = pipe.makeAsyncIterator()
        var elements: [Element] = []
        while let element = await iterator.next() {
            elements.append(element)
            if consumer == .slow {
                for _ in 0..<3 { await Task.yield() }
            }
        }
        for _ in 0..<2 {
            let late = await iterator.next()
            #expect(late == nil, "the iterator gave \(late!) after it ended")
        }
        return elements
    }
}

extension Sink {
    /// Reads the model the same way.
    func run(_ pull: Pull, prefixLength: Int = 0) -> SinkOutput {
        switch self {
            case .toArray:
                var elements: [Element] = []
                while let element = pull() { elements.append(element) }
                return .array(elements)
            case .toResult: return .result(Self.values(pull))
            case .first: return .first(pull())
            case .firstSuccess:
                while let element = pull() {
                    if case .success(let v) = element { return .firstSuccess(v) }
                }
                return .firstSuccess(nil)
            case .firstError:
                while let element = pull() {
                    if case .failure(let f) = element { return .firstError(f) }
                }
                return .firstError(nil)
            case .reduce: return .reduce(Self.values(pull).map { $0.reduce(0) { $0 &* 31 &+ $1 } })
            case .prefix: return .array(Self.prefix(pull, prefixLength))
        }
    }

    /// The values up to the first failure, which ends the read.
    private static func values(_ pull: Pull) -> Result<[Int], Fail> {
        var values: [Int] = []
        while let element = pull() {
            switch element {
                case .success(let v): values.append(v)
                case .failure(let f): return .failure(f)
            }
        }
        return .success(values)
    }

    private static func prefix(_ pull: Pull, _ length: Int) -> [Element] {
        var elements: [Element] = []
        // `for await` asks for one more element before it breaks.
        while let element = pull() {
            if elements.count == length { break }
            elements.append(element)
        }
        return elements
    }
}

/// Everything the model gives when it is read to the end.
func drained(_ input: [Element], _ stages: [Stage], log: ModelLog = ModelLog()) -> [Element] {
    Sink.toArray.run(modelPipe(input, stages, log: log)).elements!
}

/// The side effects every element could cause: the model read to the end,
/// with every `Take` letting everything through.
func everyEffect(_ input: [Element], _ stages: [Stage]) -> [String] {
    let open = stages.map { stage in
        stage.op.isTake ? Stage(op: .take(.max), async: nil, salt: 0) : stage
    }
    let log = ModelLog()
    _ = drained(input, open, log: log)
    return log.entries
}

/// Checks what any pipe must do, however it is timed: it reads its source
/// in order, from the start, without skipping, and causes only side effects
/// that some element could cause.
func expectPlausibleLog(_ log: [String], _ spec: PipeSpec) {
    let reads = log.filter { $0.hasPrefix("read ") }
    #expect(reads == reads.indices.map { "read \($0)" }, "the source was read out of order")
    let effects = log.filter { !$0.hasPrefix("read ") }
    let possible = everyEffect(spec.input, spec.stages).filter { !$0.hasPrefix("read ") }
    #expect(isSubmultiset(effects, of: possible), "side effects no element could cause")
}

#endif
