// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe
import HegelTesting
import Testing

@Suite("Pipe properties", .hegel)
struct PipeProperties {
    /// Any chain of stages, read by any sink, gives what the model gives.
    ///
    /// - With no unordered stage, the output is the same, in order.
    /// - With one, the same elements in some order.
    /// - With nothing concurrent, the source reads and side effects happen
    ///   in the same order too: the pipe reads only what the sink needs.
    /// - Otherwise, when every element is read, the same side effects
    ///   happen, in some order; when not, only ones the model could cause.
    @Test(.hegel(testCases: 1_000))
    func pipesMatchTheModel() async throws {
        try await property { tc in
            let spec = try tc.draw(PipeGen.spec, named: "pipe")
            let stages = spec.stages
            let unordered = stages.contains(where: \.isUnordered)
            let concurrent = stages.contains(where: \.isConcurrent)
            let sink =
                unordered
                ? Sink.toArray : try tc.draw(.element(of: Sink.allCases), named: "sink")
            let consumer =
                sink == .toArray
                ? try tc.draw(.element(of: Consumer.allCases), named: "consumer") : .prompt

            let prefixLength =
                sink == .prefix
                ? try tc.draw(.oneOf(.int(in: 0...4), .int(in: 0...Int.max)), named: "prefix")
                : 0

            let log = Log()
            let pipe = spec.pipe(log: log)
            guard
                let real = await withinDeadline(
                    deadline,
                    { await sink.run(pipe, consumer, prefixLength: prefixLength) }
                )
            else {
                Issue.record("the pipe hung")
                return
            }
            let modelLog = ModelLog()
            let expected = sink.run(
                modelPipe(spec.input, stages, log: modelLog),
                prefixLength: prefixLength
            )

            if unordered, case .array(let got) = real, case .array(let want) = expected {
                #expect(counts(got) == counts(want), "\(got) is not \(want) in any order")
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
        }
    }

    /// `Take` or `Drop` after an unordered stage keeps elements that depend
    /// on timing, but how many it keeps does not, and each is one the pipe
    /// without it would give.
    @Test(.hegel(testCases: 500))
    func countingAfterUnorderedStages() async throws {
        try await property { tc in
            var spec = try tc.draw(PipeGen.spec, named: "pipe")
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

            let log = Log()
            let pipe = spec.pipe(log: log)
            guard let real = await withinDeadline(deadline, { await pipe.toArray() }) else {
                Issue.record("the pipe hung")
                return
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
            expectPlausibleLog(log.all, spec)
        }
    }

    /// Cancelling the reading task at any point stops the pipe: a read that
    /// starts after the cancel gives nothing, what the reads before it gave
    /// is what the model gives, and with no cancel the pipe gives it all.
    ///
    /// The cancel lands on a drawn checkpoint: the source reading an
    /// element, a stage's side effect, in a concurrent stage's worker too,
    /// or the reader getting an element.
    @Test(.hegel(testCases: 500))
    func cancellingTheReaderStopsThePipe() async throws {
        try await property { tc in
            let spec = try tc.draw(PipeGen.spec, named: "pipe")
            let all = drained(spec.input, spec.stages)
            // Every source read and side effect is one the model could
            // cause (`expectPlausibleLog`), and the reader gets at most what
            // the model gives: the real number of checkpoints.
            let checkpoints = everyEffect(spec.input, spec.stages).count + all.count
            let run = try await tc.withRandomCancellation(
                checkpoints: 0...checkpoints
            ) { cancellation in
                let pipe = spec.pipe(log: Log(onEntry: { cancellation.checkpoint($0) }))
                // The reader runs in a task of its own, so a hang fails the
                // property instead of hanging it. It is cancelled with this
                // one.
                let reader = Task {
                    var iterator = pipe.makeAsyncIterator()
                    var got: [Element] = []
                    var after: [Element] = []
                    while true {
                        let cancelled = Task.isCancelled
                        guard let element = await iterator.next() else { break }
                        if cancelled {
                            after.append(element)
                        } else {
                            got.append(element)
                            cancellation.checkpoint("reader got \(got.count)")
                        }
                    }
                    return (got, after)
                }
                let read = await withTaskCancellationHandler {
                    await withinDeadline(deadline) { await reader.value }
                } onCancel: {
                    reader.cancel()
                }
                if read == nil { reader.cancel() }
                return read
            }
            guard let (got, after) = run.value else {
                Issue.record("the pipe hung")
                return
            }
            let unordered = spec.stages.contains(where: \.isUnordered)
            if unordered {
                #expect(isSubmultiset(got + after, of: all))
            } else {
                #expect(got + after == Array(all.prefix(got.count + after.count)))
            }
            #expect(after.isEmpty, "the pipe gave \(after) after it was cancelled")
            if run.cancelledAt == nil {
                if unordered {
                    #expect(counts(got) == counts(all), "the pipe stopped early")
                } else {
                    #expect(got == all, "the pipe stopped early")
                }
            }
        }
    }

    /// A pipe stores no state: reading it again gives the same elements.
    @Test(.hegel(testCases: 500))
    func pipesRunAgain() async throws {
        try await property { tc in
            let spec = try tc.draw(PipeGen.spec, named: "pipe")
            let pipe = spec.pipe(log: Log())
            guard
                let (first, second) = await withinDeadline(
                    deadline,
                    {
                        (await pipe.toArray(), await pipe.toArray())
                    }
                )
            else {
                Issue.record("the pipe hung")
                return
            }
            if spec.stages.contains(where: \.isUnordered) {
                #expect(counts(first) == counts(second))
            } else {
                #expect(first == second)
            }
        }
    }

    /// The builder, with any of its optional stages switched on, an
    /// `if`/`else` and a `switch`, gives the same pipe as the model: as a
    /// pipe, as an `OpenPipe`, and from a source that cannot fail, widened by
    /// its first stage.
    @Test(.hegel(testCases: 500))
    func builderMatchesTheModel() async throws {
        try await property { tc in
            let input = try tc.draw(PipeGen.input, named: "input")
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

            let log = Log()
            let modelInput: [Element]
            let pipe: Pipe<Int, Fail>
            switch mode {
                case .pipe:
                    modelInput = input
                    pipe = builtPipe(LoggedSource(elements: input, log: log), p, log: log)
                case .openPipe:
                    modelInput = input
                    pipe = builtOpenPipe(LoggedSource(elements: input, log: log), p, log: log)
                case .widened:
                    let values = input.compactMap { try? $0.get() }
                    modelInput = values.map(Element.success)
                    pipe = builtWidenedPipe(values, p, log: log)
            }
            let slots = BuilderSlots.stages(p)
            tc.note("stages: \(slots.map(\.stage))")
            guard let real = await withinDeadline(deadline, { await pipe.toArray() }) else {
                Issue.record("the pipe hung")
                return
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
                #expect(log.all == expected)
            }
        }
    }
}

#endif
