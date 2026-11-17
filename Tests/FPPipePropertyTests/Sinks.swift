// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FP
import FPPipe
import Testing

/// How the test reads the pipe: with one of fp-swift's sinks, or by hand.
enum Sink: Sendable, Hashable, CaseIterable {
    case toArray, toResult, separate, first, firstSuccess, firstError, firstSuccessOrLastError
    case last, lastSuccess, lastError, reduce, reduceInto, prefix
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
    case separated(successes: [Int], failures: [Fail])
    case first(Element?)
    case firstSuccess(Int?)
    case firstError(Fail?)
    case firstSuccessOrLastError(Element?)
    case last(Element?)
    case lastSuccess(Int?)
    case lastError(Fail?)
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
            case .first, .firstSuccess, .firstError, .firstSuccessOrLastError:
                return await runFirst(pipe)
            case .last, .lastSuccess, .lastError, .separate: return await runToTheEnd(pipe)
            case .toResult: return .result(await pipe.toResult())
            case .reduce: return .reduce(await pipe.reduce(0) { $0 &* 31 &+ $1 })
            case .reduceInto: return .reduce(await pipe.reduce(into: 0) { $0 = $0 &* 31 &+ $1 })
            case .prefix:
                var elements: [Element] = []
                for await element in pipe {
                    if elements.count == prefixLength { break }
                    elements.append(element)
                }
                return .array(elements)
        }
    }

    /// The sinks that stop at an element they look for.
    private func runFirst(_ pipe: Pipe<Int, Fail>) async -> SinkOutput {
        switch self {
            case .first: .first(await pipe.first())
            case .firstSuccess: .firstSuccess(await pipe.firstSuccess())
            case .firstError: .firstError(await pipe.firstError())
            default: .firstSuccessOrLastError(await pipe.firstSuccessOrLastError())
        }
    }

    /// The sinks that read to the end, besides `toArray`.
    private func runToTheEnd(_ pipe: Pipe<Int, Fail>) async -> SinkOutput {
        switch self {
            case .last: return .last(await pipe.last())
            case .lastSuccess: return .lastSuccess(await pipe.lastSuccess())
            case .lastError: return .lastError(await pipe.lastError())
            default:
                let (successes, failures) = await pipe.separate()
                return .separated(successes: successes, failures: failures)
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
            case .toArray, .separate, .last, .lastSuccess, .lastError:
                var elements: [Element] = []
                while let element = pull() { elements.append(element) }
                return output(ofAll: elements)
            case .toResult: return .result(Self.values(pull))
            case .reduce, .reduceInto:
                return .reduce(Self.values(pull).map { $0.reduce(0) { $0 &* 31 &+ $1 } })
            case .first: return .first(pull())
            case .firstSuccess: return .firstSuccess(Self.pull(pull, until: \.success))
            case .firstError: return .firstError(Self.pull(pull, until: \.failure))
            case .firstSuccessOrLastError:
                var lastFailure: Element?
                let success = Self.pull(pull) { element -> Element? in
                    if element.success != nil { return element }
                    lastFailure = element
                    return nil
                }
                return .firstSuccessOrLastError(success ?? lastFailure)
            case .prefix: return .array(Self.prefix(pull, prefixLength))
        }
    }

    /// What a sink that reads to the end gives for all the elements.
    private func output(ofAll elements: [Element]) -> SinkOutput {
        switch self {
            case .separate:
                .separated(
                    successes: elements.compactMap(\.success),
                    failures: elements.compactMap(\.failure)
                )
            case .last: .last(elements.last)
            case .lastSuccess: .lastSuccess(elements.compactMap(\.success).last)
            case .lastError: .lastError(elements.compactMap(\.failure).last)
            default: .array(elements)
        }
    }

    /// Pulls until `found` gives a value, or to the end.
    private static func pull<T>(_ pull: Pull, until found: (Element) -> T?) -> T? {
        while let element = pull() {
            if let value = found(element) { return value }
        }
        return nil
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

extension Result {
    /// The value of a success, or `nil`.
    var success: Success? {
        if case .success(let value) = self { value } else { nil }
    }

    /// The error of a failure, or `nil`.
    var failure: Failure? {
        if case .failure(let error) = self { error } else { nil }
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
