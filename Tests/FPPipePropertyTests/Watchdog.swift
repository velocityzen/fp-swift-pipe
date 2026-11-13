// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import Synchronization

/// A stall limit for the body of `withRandomCancellation`. Hegel's
/// `withStallDetection` needs the test case, which the body can't use, so
/// the two don't nest. And `withRandomCancellation`'s own limit starts at
/// its drawn cancel, so a run it never cancels has none: a deadlock there
/// would hang the test instead of failing it.
///
/// Make one per property, outside its test cases, and run each case's body
/// through it. Like Hegel's stall detection, once a case has stalled, the
/// cases after it, the shrinker's, get a shorter limit: four times the
/// longest gap between beats in a passing case, and at least 100 ms. So a
/// stall shrinks quickly. Unlike Hegel's, it can't tell which cases a
/// failure report is built from, so those get the shorter limit too, and
/// the report says so.
final class Watchdog: Sendable {
    let limit: Duration
    private struct State {
        var stalled = false
        var longestGap: Duration = .zero
    }
    private let state = Mutex(State())

    init(limit: Duration = stallLimit) {
        self.limit = limit
    }

    /// The limit for the next case.
    private var currentLimit: Duration {
        state.withLock { state in
            guard state.stalled else { return limit }
            return min(limit, max(.milliseconds(100), state.longestGap * 4))
        }
    }

    /// Runs `body` in a task of its own, and gives up when `body` goes the
    /// limit without a beat, leaving it running. A cancel of the calling
    /// task reaches `body` and ends the watch: from then on, the caller's
    /// `endsWithin` limit applies.
    func run<T: Sendable>(
        _ body: @escaping @Sendable (_ pulse: Pulse) async throws -> T
    ) async throws -> T {
        let limit = currentLimit
        let pulse = Pulse()
        let outcome = FirstOutcome<T>()
        let work = Task {
            do {
                outcome.settle(.success(try await body(pulse)))
            } catch {
                outcome.settle(.failure(error))
            }
        }
        let watch = Task {
            while !Task.isCancelled {
                let quiet = pulse.quiet
                if quiet >= limit {
                    outcome.settle(.failure(Stall(limit: limit, full: self.limit)))
                    return
                }
                try? await Task.sleep(for: min(limit - quiet, .milliseconds(50)))
            }
        }
        let result = await withTaskCancellationHandler {
            await outcome.value
        } onCancel: {
            work.cancel()
            watch.cancel()
        }
        watch.cancel()
        switch result {
            case .success(let value):
                state.withLock { $0.longestGap = max($0.longestGap, pulse.longestGap) }
                return value
            case .failure(let error):
                if error is Stall { state.withLock { $0.stalled = true } }
                throw error
        }
    }

    /// A body that went the limit without a beat.
    struct Stall: Error, CustomStringConvertible {
        let limit: Duration
        let full: Duration

        var description: String {
            let shortened = limit < full ? ", a limit shortened from \(full) while shrinking" : ""
            return "the reader made no progress for \(limit)\(shortened)"
        }
    }
}

/// What a watched body beats on each step it makes.
final class Pulse: Sendable {
    private struct State {
        var last = ContinuousClock.now
        var longestGap: Duration = .zero
    }
    private let state = Mutex(State())

    func beat() {
        let now = ContinuousClock.now
        state.withLock { state in
            state.longestGap = max(state.longestGap, now - state.last)
            state.last = now
        }
    }

    /// How long since the last beat.
    var quiet: Duration { ContinuousClock.now - state.withLock { $0.last } }

    var longestGap: Duration { state.withLock { $0.longestGap } }
}

/// The first of several results, whichever comes first, for one waiter.
private final class FirstOutcome<T: Sendable>: Sendable {
    private struct State {
        var result: Result<T, any Error>?
        var waiter: CheckedContinuation<Result<T, any Error>, Never>?
    }
    private let state = Mutex(State())

    func settle(_ result: Result<T, any Error>) {
        let waiter = state.withLock { state -> CheckedContinuation<Result<T, any Error>, Never>? in
            guard state.result == nil else { return nil }
            state.result = result
            defer { state.waiter = nil }
            return state.waiter
        }
        waiter?.resume(returning: result)
    }

    var value: Result<T, any Error> {
        get async {
            await withCheckedContinuation { continuation in
                let ready = state.withLock { state in
                    if state.result == nil { state.waiter = continuation }
                    return state.result
                }
                if let ready { continuation.resume(returning: ready) }
            }
        }
    }
}

#endif
