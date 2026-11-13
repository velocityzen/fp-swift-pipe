// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import Synchronization

// MARK: - Multisets

func counts<T: Hashable>(_ items: [T]) -> [T: Int] {
    items.reduce(into: [:]) { $0[$1, default: 0] += 1 }
}

/// Whether every item of `part` is in `whole`, as many times or fewer.
func isSubmultiset<T: Hashable>(_ part: [T], of whole: [T]) -> Bool {
    let available = counts(whole)
    return counts(part).allSatisfy { item, count in count <= available[item, default: 0] }
}

// MARK: - Deadlines

/// Resumes a continuation at most once.
final class Once<T: Sendable>: Sendable {
    let continuation: Mutex<CheckedContinuation<T?, Never>?>
    init(_ continuation: CheckedContinuation<T?, Never>) {
        self.continuation = Mutex(continuation)
    }
    func resume(_ value: T?) -> Bool {
        guard let continuation = continuation.withLock({ $0.take() }) else { return false }
        continuation.resume(returning: value)
        return true
    }
}

/// How long a pipe gets before the run counts as hung. Generous: the long
/// inputs through many concurrent stages are slow under a loaded machine.
let deadline = Duration.seconds(10)

/// Runs `body` in its own task, and gives up on it after `limit`: `nil`
/// means it was still running. A hung pipe then fails one test case instead
/// of hanging the whole run.
func withinDeadline<T: Sendable>(
    _ limit: Duration,
    _ body: @escaping @Sendable () async -> T
) async -> T? {
    await withCheckedContinuation { continuation in
        let once = Once(continuation)
        let work = Task {
            _ = once.resume(await body())
        }
        Task {
            try? await Task.sleep(for: limit)
            if once.resume(nil) {
                work.cancel()
            }
        }
    }
}

#endif
