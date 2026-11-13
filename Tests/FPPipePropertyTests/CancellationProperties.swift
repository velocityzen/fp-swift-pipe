// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe
import HegelTesting
import Synchronization
import Testing

extension PipeProperties {
    /// Cancelling the reading task at any point stops the pipe within the
    /// stall limit: a read that starts after the cancel gives nothing, what
    /// the reads before it gave is what the model gives, and with no cancel
    /// the pipe gives it all.
    ///
    /// The cancel lands on a drawn checkpoint: the source reading an
    /// element, a stage's side effect, in a concurrent stage's worker too,
    /// or the reader getting an element. The run before the cancel, or a
    /// run with no cancel, has a stall limit too: every checkpoint and every
    /// closure starting or ending is progress.
    @Test
    func cancellingTheReaderStopsThePipe() async throws {
        try await property(
            examples: Self.cancelledBeforeReading,
            reaching: [Reach.cancelledMidway]
        ) { tc in
            let policy = try tc.draw(PipeGen.policy, named: "policy")
            let spec = try tc.draw(PipeGen.spec(policy), named: "pipe")
            let all = drained(spec.input, spec.stages)
            // Every source read and side effect is one the model could
            // cause (`expectPlausibleLog`), and the reader gets at most what
            // the model gives: the real number of checkpoints.
            let checkpoints = everyEffect(spec.input, spec.stages).count + all.count
            let run = try await tc.withRandomCancellation(
                checkpoints: 0...checkpoints,
                endsWithin: stallLimit,
                stallsAfter: stallLimit
            ) { cancellation in
                let log = Log(
                    onEntry: { cancellation.checkpoint($0) },
                    onProgress: { cancellation.beat() }
                )
                var iterator = spec.pipe(log: log).makeAsyncIterator()
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

    /// The bug fixed in 3ad345c: a source read on after the reading task was
    /// cancelled. One element, from each kind of source, and a cancel before
    /// the reader reads it.
    static let cancelledBeforeReading: [Example] = SourceKind.allCases.map { source in
        [
            "policy": PipePolicy(failureRate: 0.5, ops: nil, asyncRate: 0.5, tight: false),
            "pipe": PipeSpec(input: [.success(0)], stages: [], source: source),
            "cancelAfter": Int?(0),
        ]
    }

    /// When the reader stops early, because it has what it wants or because
    /// its task is cancelled, every closure a concurrent stage started is
    /// cancelled, and the reader does not wait for them to end.
    ///
    /// A concurrent `AsyncMap` whose closure hangs, on the values a drawn
    /// modulus picks, goes anywhere in the pipe. A hang ends when its task is
    /// cancelled. A stubborn one, like a loop that never checks for
    /// cancellation, ends only after the test case has checked the reader
    /// ended, so a reader that waited for it would never end.
    ///
    /// The reader takes at most the elements it surely gets without waiting
    /// on a hang, then stops; a drawn cancel may stop it sooner. Meanwhile
    /// the stage reads ahead, so it may have started hangs the reader never
    /// waits for, and the cancel has to reach them through the stages after
    /// it.
    @Test
    func stoppingTheReaderCancelsItsWork() async throws {
        try await property(
            reaching: [
                Reach.leftHanging, Reach.cancelledWhileHanging, Reach.stoppedWhileHanging,
                Reach.hungBehindStages, Reach.endedBeforeItsWork,
            ]
        ) { tc in
            let setup = try HangingPipe.draw(tc)
            let probe = CancellationProbe()
            let stubborn = StubbornWork()
            defer { stubborn.release() }
            let started: @Sendable () -> Int = { probe.started + stubborn.started }
            let run = try await tc.withRandomCancellation(
                checkpoints: 0...(setup.wanted + setup.hangs),
                endsWithin: stallLimit,
                stallsAfter: stallLimit
            ) { cancellation in
                await setup.read(
                    cancellation: cancellation,
                    started: started,
                    hang: {
                        if setup.stubborn {
                            await stubborn.run()
                        } else {
                            await probe.hang(atMost: stallLimit)
                        }
                    }
                )
            }
            // The reader has ended, and no stubborn hang can have.
            let leftRunning = stubborn.running
            #expect(
                run.cancelledAt != nil || run.value == setup.wanted,
                "the reader got \(run.value) of the \(setup.wanted) elements it surely gets"
            )
            // Each hang counts as cancelled as the cancel is made, but an
            // ordered stage, or a stage behind others, makes its cancel from
            // a detached task, which may run a little after the reader
            // returns. A hang left running stalls here. While shrinking a
            // stall, Hegel shortens the limit, so a failing case stays cheap.
            tc.note("hangs started: \(started()), left running: \(leftRunning)")
            _ = try await tc.withStallDetection(after: .seconds(2)) { _ in
                while probe.uncancelled > 0 || stubborn.uncancelled > 0 {
                    try await Task.sleep(for: .milliseconds(1))
                }
            }

            if setup.hangs > 0 { try tc.event("a value upstream hangs") }
            if started() > 0 {
                try tc.event(Reach.leftHanging)
                try tc.event(
                    run.cancelledAt == nil ? Reach.stoppedWhileHanging : Reach.cancelledWhileHanging
                )
                if setup.position < setup.spec.stages.count { try tc.event(Reach.hungBehindStages) }
            }
            if leftRunning > 0 { try tc.event(Reach.endedBeforeItsWork) }
        }
    }
}

/// A pipe with a concurrent stage whose closure hangs on some values, and
/// how far a reader reads it: what `stoppingTheReaderCancelsItsWork` draws.
struct HangingPipe: Sendable {
    let spec: PipeSpec
    /// The closure hangs on the values this divides.
    let modulus: Int
    let stage: Concurrency
    /// The hanging stage goes before this stage of `spec`.
    let position: Int
    /// The elements the reader takes, at most the ones it surely gets.
    let wanted: Int
    /// After taking them, the reader waits a little for a hang to start.
    let lingers: Bool
    /// The hangs ignore their cancel.
    let stubborn: Bool
    /// The values upstream of the stage that hang.
    let hangs: Int

    static func draw(_ tc: TestCase) throws -> HangingPipe {
        let policy = try tc.draw(PipeGen.policy, named: "policy")
        let spec = try tc.draw(PipeGen.spec(policy), named: "pipe")
        // 1 hangs on every value, 2 on half; any modulus sometimes.
        let (m, stage) = try tc.draw(
            Hegel.zip(.oneOf(.int(in: 1...3), PipeGen.modulus), PipeGen.parallel),
            named: "hanging stage"
        )
        let position = try tc.draw(.int(in: 0...spec.stages.count), named: "position")
        let before = Array(spec.stages[..<position])
        let upstream = drained(spec.input, before)
        let passing = readyElements(
            upstream,
            through: stage,
            hangs: { F.matches(m, $0) },
            inOrder: !before.contains(where: \.isUnordered)
        )
        // How many elements the stages after it give, from those, does not
        // depend on their order.
        let ready = drained(passing, Array(spec.stages[position...])).count
        let wanted = try tc.draw(.int(in: 0...ready), named: "wanted")
        // A reader that stops at once often leaves before the stage reads
        // ahead to a hanging value. One that lingers gives it time.
        let lingers = try tc.draw(.bool(), named: "lingers")
        let stubborn = try tc.draw(.bool(), named: "stubborn")
        let hangs = upstream.count { element in
            if case .success(let v) = element { F.matches(m, v) } else { false }
        }
        return HangingPipe(
            spec: spec,
            modulus: m,
            stage: stage,
            position: position,
            wanted: wanted,
            lingers: lingers,
            stubborn: stubborn,
            hangs: hangs
        )
    }

    /// Builds the pipe, with `hang` as the work of a closure that hangs, and
    /// reads `wanted` elements. Lingering, it then waits a little for
    /// `started`, the hangs that started, to be more than 0. Returns how
    /// many elements it got.
    func read(
        cancellation: Cancellation,
        started: @escaping @Sendable () -> Int,
        hang: @escaping @Sendable () async -> Void
    ) async -> Int {
        let m = modulus
        let pipe = spec.pipe(
            hangingAt: position,
            stage,
            on: { F.matches(m, $0) },
            log: Log(onProgress: { cancellation.beat() }),
            hang: { v in
                cancellation.checkpoint("hang \(v)")
                await hang()
            }
        )
        var iterator = pipe.makeAsyncIterator()
        var got = 0
        while got < wanted, await iterator.next() != nil {
            got += 1
            cancellation.checkpoint("reader got \(got)")
        }
        if lingers, hangs > 0 {
            await until(within: .milliseconds(50)) { started() > 0 }
        }
        return got
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
        log: Log,
        hang: @escaping @Sendable (Int) async -> Void
    ) -> Pipe<Int, Fail> {
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

/// Work that ignores cancellation, like a long loop that never checks
/// `Task.isCancelled`: each `run()` goes on until `release()`, cancelled or
/// not. It counts the runs that started, the ones still going, and the ones
/// whose task was cancelled.
///
/// A run's cancel is counted as it is made, in its cancellation handler, as
/// `CancellationProbe` counts it, so `uncancelled` is 0 as soon as every
/// cancel has been made.
final class StubbornWork: Sendable {
    private struct State {
        var started = 0
        var ended = 0
        var cancelled = 0
        var released = false
        var waiting: [CheckedContinuation<Void, Never>] = []
    }
    private let state = Mutex(State())

    func run() async {
        state.withLock { $0.started += 1 }
        await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                let released = state.withLock { state in
                    if !state.released { state.waiting.append(continuation) }
                    return state.released
                }
                if released { continuation.resume() }
            }
        } onCancel: {
            state.withLock { $0.cancelled += 1 }
        }
        state.withLock { $0.ended += 1 }
    }

    /// Ends every run, and makes every later one end at once.
    func release() {
        let waiting = state.withLock { state in
            state.released = true
            defer { state.waiting = [] }
            return state.waiting
        }
        for continuation in waiting {
            continuation.resume()
        }
    }

    var started: Int { state.withLock { $0.started } }

    /// The runs that started and haven't ended.
    var running: Int { state.withLock { $0.started - $0.ended } }

    /// The runs that started and whose task hasn't been cancelled.
    var uncancelled: Int { state.withLock { $0.started - $0.cancelled } }
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
