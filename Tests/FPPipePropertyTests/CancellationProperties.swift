// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe
import HegelTesting
import Testing

extension PipeProperties {
    /// Cancelling the reading task at any point stops the pipe within the
    /// stall limit: a read that starts after the cancel gives nothing, what
    /// the reads before it gave is what the model gives, and with no cancel
    /// the pipe gives it all.
    ///
    /// The cancel lands on a drawn checkpoint: the source reading an
    /// element, a stage's side effect, in a concurrent stage's worker too,
    /// or the reader getting an element. A read that is never cancelled runs
    /// to the end with no limit; `pipesMatchTheModel` checks those for
    /// stalls.
    @Test
    func cancellingTheReaderStopsThePipe() async throws {
        try await property(reaching: [Reach.cancelledMidway]) { tc in
            let policy = try tc.draw(PipeGen.policy, named: "policy")
            let spec = try tc.draw(PipeGen.spec(policy), named: "pipe")
            let all = drained(spec.input, spec.stages)
            // Every source read and side effect is one the model could
            // cause (`expectPlausibleLog`), and the reader gets at most what
            // the model gives: the real number of checkpoints.
            let checkpoints = everyEffect(spec.input, spec.stages).count + all.count
            let run = try await tc.withRandomCancellation(
                checkpoints: 0...checkpoints,
                endsWithin: stallLimit
            ) { cancellation in
                let pipe = spec.pipe(log: Log(onEntry: { cancellation.checkpoint($0) }))
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
            let (got, after) = run.value
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

            if run.cancelledAt != nil, got.count < all.count {
                try tc.event(Reach.cancelledMidway)
            }
        }
    }

    /// When the reader stops early, because it has what it wants or because
    /// its task is cancelled, every closure a concurrent stage started is
    /// cancelled: the pipe leaves no work running.
    ///
    /// A concurrent `AsyncMap` whose closure hangs until cancelled, on the
    /// values a drawn modulus picks, goes anywhere in the pipe. The reader
    /// takes at most the elements it surely gets without waiting on a hang,
    /// then stops; a drawn cancel may stop it sooner. Meanwhile the stage
    /// reads ahead, so it may have started hangs the reader never waits for,
    /// and the cancel has to reach them through the stages after it.
    @Test
    func stoppingTheReaderCancelsItsWork() async throws {
        try await property(
            reaching: [
                Reach.leftHanging, Reach.cancelledWhileHanging, Reach.stoppedWhileHanging,
                Reach.hungBehindStages,
            ]
        ) { tc in
            let policy = try tc.draw(PipeGen.policy, named: "policy")
            let spec = try tc.draw(PipeGen.spec(policy), named: "pipe")
            // 1 hangs on every value, 2 on half; any modulus sometimes.
            let (m, stage) = try tc.draw(
                Hegel.zip(.oneOf(.int(in: 1...3), PipeGen.modulus), PipeGen.parallel),
                named: "hanging stage"
            )
            let position = try tc.draw(.int(in: 0...spec.stages.count), named: "position")
            let before = Array(spec.stages[..<position])
            let after = Array(spec.stages[position...])
            let upstream = drained(spec.input, before)
            let passing = readyElements(
                upstream,
                through: stage,
                hangs: { F.matches(m, $0) },
                inOrder: !before.contains(where: \.isUnordered)
            )
            // How many elements the stages after it give, from those, does
            // not depend on their order.
            let ready = drained(passing, after).count
            let wanted = try tc.draw(.int(in: 0...ready), named: "wanted")
            // A reader that stops at once often leaves before the stage reads
            // ahead to a hanging value. One that lingers gives it time.
            let lingers = try tc.draw(.bool(), named: "lingers")
            let hangs = upstream.count { element in
                if case .success(let v) = element { F.matches(m, v) } else { false }
            }

            let probe = CancellationProbe()
            let run = try await tc.withRandomCancellation(
                checkpoints: 0...(wanted + hangs),
                endsWithin: stallLimit
            ) { cancellation in
                let pipe = spec.pipe(
                    hangingAt: position,
                    stage,
                    on: { F.matches(m, $0) },
                    hang: { v in
                        cancellation.checkpoint("hang \(v)")
                        await probe.hang(atMost: stallLimit)
                    }
                )
                var iterator = pipe.makeAsyncIterator()
                var got = 0
                while got < wanted, await iterator.next() != nil {
                    got += 1
                    cancellation.checkpoint("reader got \(got)")
                }
                if lingers, hangs > 0 {
                    await until(within: .milliseconds(50)) { probe.started > 0 }
                }
                return got
            }
            #expect(
                run.cancelledAt != nil || run.value == wanted,
                "the reader got \(run.value) of the \(wanted) elements it surely gets"
            )
            // Each hang counts as cancelled as the cancel is made, but an
            // ordered stage, or a stage behind others, makes its cancel from
            // a detached task, which may run a little after the reader
            // returns. A hang left running stalls here. While shrinking a
            // stall, Hegel shortens the limit, so a failing case stays cheap.
            tc.note("hangs started: \(probe.started), cancelled: \(probe.cancelled)")
            _ = try await tc.withStallDetection(after: .seconds(2)) { _ in
                while probe.uncancelled > 0 {
                    try await Task.sleep(for: .milliseconds(1))
                }
            }

            if hangs > 0 { try tc.event("a value upstream hangs") }
            if probe.started > 0 {
                try tc.event(Reach.leftHanging)
                try tc.event(
                    run.cancelledAt == nil ? Reach.stoppedWhileHanging : Reach.cancelledWhileHanging
                )
                if !after.isEmpty { try tc.event(Reach.hungBehindStages) }
            }
        }
    }
}

/// The elements a reader surely gets through a concurrent stage whose
/// closure hangs on the values `hangs` picks, from `elements` in the model's
/// order, though not always in that order. When `inOrder` is false, timing
/// upstream decides the order, so it counts on the worst one.
///
/// Ordered, a hang holds back every element after it. Unordered, each hang
/// holds one of the stage's `concurrency` slots, and once every slot is held
/// the stage reads no more.
func readyElements(
    _ elements: [Element],
    through stage: Concurrency,
    hangs: (Int) -> Bool,
    inOrder: Bool
) -> [Element] {
    let hanging = elements.map { element in
        if case .success(let v) = element { hangs(v) } else { false }
    }
    let blocking = stage.ordered ? 1 : stage.concurrency
    guard inOrder else {
        let hung = hanging.count(where: \.self)
        return hung < blocking ? zip(elements, hanging).filter { !$1 }.map(\.0) : []
    }
    var ready: [Element] = []
    var hung = 0
    for (element, hangs) in zip(elements, hanging) {
        if hangs {
            hung += 1
            if hung == blocking { break }
        } else {
            ready.append(element)
        }
    }
    return ready
}

extension PipeSpec {
    /// This pipe with one more stage before stage `position`: a concurrent
    /// `AsyncMap` that passes each value on, after `hang` for the values
    /// `hangs` picks.
    func pipe(
        hangingAt position: Int,
        _ stage: Concurrency,
        on hangs: @escaping @Sendable (Int) -> Bool,
        hang: @escaping @Sendable (Int) async -> Void
    ) -> Pipe<Int, Fail> {
        let log = Log()
        var pipe = PipeSpec(
            input: input,
            stages: Array(stages[..<position]),
            source: source,
            sourceYields: sourceYields
        ).pipe(log: log)
        pipe = AsyncMap(
            concurrency: stage.concurrency,
            ordered: stage.ordered,
            bufferLimit: stage.bufferLimit
        ) { (v: Int) in
            if hangs(v) { await hang(v) }
            return v
        }.attach(pipe)
        for (offset, later) in stages[position...].enumerated() {
            pipe = later.attach(pipe, index: position + 1 + offset, log: log)
        }
        return pipe
    }
}

/// Waits until `condition` holds, for at most `limit`, or until the task is
/// cancelled. Returns whether it holds.
@discardableResult
func until(within limit: Duration, _ condition: () -> Bool) async -> Bool {
    let clock = ContinuousClock()
    let deadline = clock.now + limit
    while !condition() {
        guard clock.now < deadline, !Task.isCancelled else { return false }
        try? await Task.sleep(for: .milliseconds(1))
    }
    return true
}

#endif
