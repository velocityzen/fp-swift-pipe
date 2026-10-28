import Synchronization

/// Proves transforms run in parallel without timing them.
///
/// Each `enter()` waits until `expected` callers are inside at once, then leaves. A stage
/// that runs them in parallel reaches `peak == expected` as soon as the last one starts,
/// however loaded the machine is; a sequential stage never gets past one caller inside, so
/// each wait gives up after ~2s and `peak` stays at 1.
final class Rendezvous: Sendable {
    private let state = Mutex((inside: 0, peak: 0))
    private let expected: Int

    init(expected: Int) {
        self.expected = expected
    }

    var peak: Int {
        state.withLock { $0.peak }
    }

    func enter() async {
        state.withLock {
            $0.inside += 1
            $0.peak = max($0.peak, $0.inside)
        }
        for _ in 0..<2_000 where peak < expected {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }
        state.withLock { $0.inside -= 1 }
    }
}
