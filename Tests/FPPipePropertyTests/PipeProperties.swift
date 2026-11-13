// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe
import HegelTesting
import Testing

/// The states the properties exist to test, as `tc.event` labels. Each
/// `property(reaching:)` fails a run that never records one of its labels.
enum Reach {
    static let unordered = "an unordered stage"
    static let reordered = "output came out of source order"
    static let overlapped = "a stage's closures overlapped"
    static let atConcurrencyLimit = "a concurrent stage ran its limit of closures"
    static let atBufferLimit = "an ordered stage's bufferLimit held its closures back"
    static let stoppedEarly = "the reader stopped before the pipe ended"
    static let unorderedStoppedEarly = "the reader stopped an unordered pipe early"
    static let failureReached = "the pipe gave a failure"
    static let countCut = "Take or Drop changed an unordered output"
    static let concurrentRerun = "a concurrent pipe gave elements twice"
    static let cancelledMidway = "the reader was cancelled with elements left"
    static let leftHanging = "a closure hung when the reader stopped"
    static let cancelledWhileHanging = "the reader was cancelled with a closure hung"
    static let stoppedWhileHanging = "the reader stopped by itself with a closure hung"
    static let hungBehindStages = "a closure hung behind later stages"
    static let heldBack = "a slow reader held the source back"
}

@Suite("Pipe properties", .hegel)
struct PipeProperties {
    /// Any chain of stages, read by any sink, gives what the model gives.
    ///
    /// - With no unordered stage, the output is the same, in order.
    /// - With one, it is what some order of the model's output gives.
    /// - With nothing concurrent, the source reads and side effects happen
    ///   in the same order too: the pipe reads only what the sink needs.
    /// - Otherwise, when every element is read, the same side effects
    ///   happen, in some order; when not, only ones the model could cause.
    /// - No stage runs more closures at once than its `concurrency`, or, when
    ///   ordered, than `bufferLimit + 1`; the tap stages run one at a time.
    @Test
    func pipesMatchTheModel() async throws {
        try await property(
            reaching: [
                Reach.unordered, Reach.reordered, Reach.overlapped, Reach.atConcurrencyLimit,
                Reach.atBufferLimit, Reach.stoppedEarly, Reach.unorderedStoppedEarly,
                Reach.failureReached,
            ]
        ) { tc in
            let policy = try tc.draw(PipeGen.policy, named: "policy")
            let spec = try tc.draw(PipeGen.spec(policy), named: "pipe")
            let stages = spec.stages
            let unordered = stages.contains(where: \.isUnordered)
            let concurrent = stages.contains(where: \.isConcurrent)
            // `toArray` half the time: it reads every element, so it checks
            // the most, the multiset of an unordered pipe included.
            let sink = try tc.draw(
                .oneOf(.just(.toArray), .element(of: Sink.allCases)),
                named: "sink"
            )
            let consumer =
                sink == .toArray
                ? try tc.draw(.element(of: Consumer.allCases), named: "consumer") : .prompt
            let prefixLength =
                sink == .prefix
                ? try tc.draw(.oneOf(.int(in: 0...4), .int(in: 0...Int.max)), named: "prefix")
                : 0

            let (real, log) = try await tc.withStallDetection(after: stallLimit) { heartbeat in
                let log = Log(onProgress: { heartbeat.beat() })
                let pipe = spec.pipe(log: log)
                return (await sink.run(pipe, consumer, prefixLength: prefixLength), log)
            }
            let modelLog = ModelLog()
            let expected = sink.run(
                modelPipe(spec.input, stages, log: modelLog),
                prefixLength: prefixLength
            )
            let all = drained(spec.input, stages)

            if unordered {
                expectSomeOrder(real, of: all, sink: sink, prefixLength: prefixLength)
            } else {
                #expect(real == expected)
            }
            if !concurrent {
                #expect(log.all == modelLog.entries)
            } else if sink == .toArray, !stages.contains(where: \.op.isTake) {
                #expect(counts(log.all) == counts(modelLog.entries))
            } else {
                expectPlausibleLog(log.all, spec)
            }
            expectWithinClosureLimits(stages, log)

            let stoppedEarly =
                sink != .toArray && modelReads(modelLog) < modelReads(drainedLog(spec))
            try recordReach(tc, spec, real: real, log: log, stoppedEarly: stoppedEarly)
        }
    }

    /// `Take` or `Drop` after an unordered stage keeps elements that depend
    /// on timing, but how many it keeps does not, and each is one the pipe
    /// without it would give.
    @Test
    func countingAfterUnorderedStages() async throws {
        try await property(reaching: [Reach.countCut]) { tc in
            let policy = try tc.draw(PipeGen.policy, named: "policy")
            var spec = try tc.draw(PipeGen.spec(policy), named: "pipe")
            let unordered = try tc.draw(
                Hegel.zip(
                    PipeGen.op.filter(\.hasConcurrency),
                    PipeGen.parallel,
                    .int()
                ),
                named: "unordered stage"
            )
            let tail = try tc.draw(
                Hegel.zip(.bool(), PipeGen.count).map { $0 ? Op.take($1) : Op.drop($1) },
                named: "tail"
            )
            var concurrency = unordered.1
            concurrency.ordered = false
            spec.stages += [
                Stage(op: unordered.0, async: concurrency, salt: unordered.2),
                Stage(op: tail, async: nil, salt: 0),
            ]

            let counted = spec
            let (real, log) = try await tc.withStallDetection(after: stallLimit) { heartbeat in
                let log = Log(onProgress: { heartbeat.beat() })
                return (await counted.pipe(log: log).toArray(), log.all)
            }
            let all = drained(spec.input, Array(spec.stages.dropLast()))
            let count: Int =
                switch tail {
                    case .take(let n): min(n, all.count)
                    case .drop(let n): all.count - min(n, all.count)
                    default: preconditionFailure()
                }
            #expect(real.count == count)
            #expect(isSubmultiset(real, of: all), "\(real) has elements the pipe would not give")
            expectPlausibleLog(log, spec)

            if count > 0, count < all.count { try tc.event(Reach.countCut) }
        }
    }

    /// A pipe stores no state: reading it again gives the same elements.
    @Test
    func pipesRunAgain() async throws {
        try await property(reaching: [Reach.concurrentRerun]) { tc in
            let policy = try tc.draw(PipeGen.policy, named: "policy")
            let spec = try tc.draw(PipeGen.spec(policy), named: "pipe")
            let (first, second) = try await tc.withStallDetection(after: stallLimit) { heartbeat in
                let pipe = spec.pipe(log: Log(onProgress: { heartbeat.beat() }))
                return (await pipe.toArray(), await pipe.toArray())
            }
            if spec.stages.contains(where: \.isUnordered) {
                #expect(counts(first) == counts(second))
            } else {
                #expect(first == second)
            }

            if !first.isEmpty, spec.stages.contains(where: \.isConcurrent) {
                try tc.event(Reach.concurrentRerun)
            }
        }
    }

    /// The builder, with any of its optional stages switched on, an
    /// `if`/`else` and a `switch`, gives the same pipe as the model: as a
    /// pipe, as an `OpenPipe`, and from a source that cannot fail, widened by
    /// its first stage.
    @Test
    func builderMatchesTheModel() async throws {
        try await property(reaching: ["if", "else", "switch 0", "switch 1", "switch 2"]) { tc in
            let failureRate = try tc.draw(PipeGen.failureRate, named: "failureRate")
            let input = try tc.draw(PipeGen.input(failureRate: failureRate), named: "input")
            let p = try tc.draw(
                Hegel.zip(
                    .array(of: .bool(), count: BuilderSlots.count...BuilderSlots.count),
                    .int(in: 0...2),
                    PipeGen.small,
                    PipeGen.modulus,
                    PipeGen.value,
                    PipeGen.count,
                    PipeGen.bufferSize,
                    Hegel.zip(
                        .oneOf(.int(in: 1...4), .int(in: 1...Int.max), .just(.max)),
                        .oneOf(.just(Int.max), .int(in: 0...4), .int(in: 0...Int.max))
                    )
                ).map {
                    BuilderParameters(
                        on: $0,
                        branch: $1,
                        a: $2,
                        m: $3,
                        x: $4,
                        n: $5,
                        size: $6,
                        c: $7.0,
                        b: $7.1
                    )
                },
                named: "parameters"
            )
            let mode = try tc.draw(.element(of: BuilderMode.allCases), named: "mode")

            let values = input.compactMap { try? $0.get() }
            let modelInput = mode == .widened ? values.map(Element.success) : input
            let slots = BuilderSlots.stages(p)
            tc.note("stages: \(slots.map(\.stage))")
            let (real, log) = try await tc.withStallDetection(after: stallLimit) { heartbeat in
                let log = Log(onProgress: { heartbeat.beat() })
                let pipe: Pipe<Int, Fail> =
                    switch mode {
                        case .pipe: builtPipe(LoggedSource(elements: input, log: log), p, log: log)
                        case .openPipe:
                            builtOpenPipe(LoggedSource(elements: input, log: log), p, log: log)
                        case .widened: builtWidenedPipe(values, p, log: log)
                    }
                return (await pipe.toArray(), log.all)
            }

            let modelLog = ModelLog()
            var pull: Pull = modelPipe(modelInput, [], log: modelLog)
            for (index, stage) in slots {
                pull = stage.op.model(pull, index: index, log: modelLog)
            }
            #expect(real == Sink.toArray.run(pull).elements)
            if !slots.contains(where: \.stage.isConcurrent) {
                // `From` logs no reads.
                let expected =
                    mode == .widened
                    ? modelLog.entries.filter { !$0.hasPrefix("read ") } : modelLog.entries
                #expect(log == expected)
            }

            try tc.event(p.on[BuilderSlots.count - 1] ? "if" : "else")
            try tc.event("switch \(p.branch)")
        }
    }
}

// MARK: - Closure limits

/// Checks that no async stage ran more closures at once than the docs let
/// it: `concurrency`, and with `ordered`, `bufferLimit + 1` when that is
/// lower.
func expectWithinClosureLimits(_ stages: [Stage], _ log: Log) {
    for (index, stage) in stages.enumerated() {
        guard let async = stage.async else { continue }
        let peak = log.peak(index)
        #expect(peak <= async.concurrency, "stage \(index), \(stage), ran \(peak) closures at once")
        if async.ordered, async.isConcurrent, async.bufferLimit < async.concurrency - 1 {
            #expect(
                peak <= async.bufferLimit + 1,
                "stage \(index), \(stage), ran \(peak) closures at once"
            )
        }
    }
}

// MARK: - Reach

/// Records the states `pipesMatchTheModel` reached.
func recordReach(
    _ tc: TestCase,
    _ spec: PipeSpec,
    real: SinkOutput,
    log: Log,
    stoppedEarly: Bool
) throws {
    let all = drained(spec.input, spec.stages)
    let unordered = spec.stages.contains(where: \.isUnordered)
    if unordered {
        try tc.event(Reach.unordered)
        if case .array(let got) = real, got != Array(all.prefix(got.count)) {
            try tc.event(Reach.reordered)
        }
    }
    let peaks = spec.stages.indices.map(log.peak)
    if peaks.contains(where: { $0 > 1 }) { try tc.event(Reach.overlapped) }
    for (index, stage) in spec.stages.enumerated() {
        guard let async = stage.async, async.isConcurrent, peaks[index] > 0 else { continue }
        if peaks[index] == async.concurrency { try tc.event(Reach.atConcurrencyLimit) }
        // At the limit, with more closures to run than it lets run at once.
        if async.closureLimit < async.concurrency, peaks[index] == async.closureLimit,
            log.runs(index) > async.closureLimit
        {
            try tc.event(Reach.atBufferLimit)
        }
    }
    if stoppedEarly {
        try tc.event(Reach.stoppedEarly)
        if unordered { try tc.event(Reach.unorderedStoppedEarly) }
    }
    if all.contains(where: { (try? $0.get()) == nil }) { try tc.event(Reach.failureReached) }
    try tc.event("input count", value: Double(spec.input.count))
    try tc.event("stage count", value: Double(spec.stages.count))
}

// MARK: - Unordered sinks

/// Checks what a sink gave from an unordered pipe: what it gives from some
/// order of `all`, the model's output read to the end.
func expectSomeOrder(_ real: SinkOutput, of all: [Element], sink: Sink, prefixLength: Int) {
    let values = all.compactMap { try? $0.get() }
    let failures = all.compactMap { element -> Fail? in
        if case .failure(let f) = element { f } else { nil }
    }
    switch real {
        case .array(let got) where sink == .toArray:
            #expect(counts(got) == counts(all), "\(got) is not \(all) in any order")
        case .array(let got):
            #expect(got.count == min(prefixLength, all.count), "the prefix has the wrong length")
            #expect(isSubmultiset(got, of: all), "\(got) has elements the pipe would not give")
        case .first(let got): expectOneOf(got, all)
        case .firstSuccess(let got): expectOneOf(got, values)
        case .firstError(let got): expectOneOf(got, failures)
        case .result(.success(let got)):
            #expect(failures.isEmpty, "toResult succeeded, but the pipe gives \(failures)")
            #expect(counts(got) == counts(values), "toResult gave \(got), not \(values)")
        case .result(.failure(let got)):
            #expect(failures.contains(got), "toResult failed with \(got), not in \(failures)")
        case .reduce(.success):
            // The fold depends on the order, so only its kind is known.
            #expect(failures.isEmpty, "reduce succeeded, but the pipe gives \(failures)")
        case .reduce(.failure(let got)):
            #expect(failures.contains(got), "reduce failed with \(got), not in \(failures)")
    }
}

/// Checks what a `first` sink gave: one of `candidates`, or nil when there
/// are none.
func expectOneOf<T: Equatable>(
    _ got: T?,
    _ candidates: [T],
    sourceLocation: SourceLocation = #_sourceLocation
) {
    guard let got else {
        #expect(
            candidates.isEmpty,
            "gave nil, not one of \(candidates)",
            sourceLocation: sourceLocation
        )
        return
    }
    #expect(
        candidates.contains(got),
        "gave \(got), not one of \(candidates)",
        sourceLocation: sourceLocation
    )
}

// MARK: - Reach helpers

/// How many source reads a model log holds.
func modelReads(_ log: ModelLog) -> Int {
    log.entries.count(where: { $0.hasPrefix("read ") })
}

/// The log of the model read to the end.
func drainedLog(_ spec: PipeSpec) -> ModelLog {
    let log = ModelLog()
    _ = drained(spec.input, spec.stages, log: log)
    return log
}

#endif
