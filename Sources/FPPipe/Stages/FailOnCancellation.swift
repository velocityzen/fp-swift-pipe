import FP

/// The stage that `FailOnCancellation` returns.
@usableFromInline
struct FailOnCancellationStage<F: Error & Sendable>: PipePolyValueStage {
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

    @usableFromInline let error: F

    @inlinable
    init(_ error: F) {
        self.error = error
    }

    @inlinable
    func attach<V: Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let error = self.error
        // fp-swift's iterator, so the pipe and `failOnCancellation(with:)` follow one rule.
        return .iterating { .init(upstream.failOnCancellation(with: error).makeAsyncIterator()) }
    }
}

/// Ends the pipe with one `.failure(error)` when the task that reads it is cancelled.
///
/// A cancelled pipe stops reading its source and ends, so a sink that reads to the end, like
/// `toResult()`, gives the values it got so far as a success. Put this stage last to get a
/// failure instead:
///
/// ```swift
/// let pipe = Pipe<Int, SyncError> {
///     From(ids)
///     AsyncMap { (id: Int) in await upload(id) }
///     FailOnCancellation(SyncError.cancelled)
/// }
/// ```
///
/// It checks for cancellation before each read and when the pipe before it ends. If the task
/// is cancelled, it gives `.failure(error)` and ends, and does not read the pipe before it
/// again. Otherwise it passes every element through. A cancel that comes just as the pipe ends
/// gives the failure too. Inside `withTaskCancellationShield`, the task does not look
/// cancelled, so every element passes through.
///
/// `error` sets the failure type, so it works after steps that cannot fail. It follows the
/// same rule as fp-swift's `failOnCancellation(with:)`, which does the same before a sink.
@inlinable
public func FailOnCancellation<F: Error & Sendable>(
    _ error: F,
) -> some PipePolyValueStage<F, F> {
    FailOnCancellationStage(error)
}
