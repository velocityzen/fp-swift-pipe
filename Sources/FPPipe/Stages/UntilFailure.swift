import FP

/// The stage that `UntilFailure` returns.
@usableFromInline
struct UntilFailureStage: PipeForwardingStage {
    @inlinable
    init() {}

    @inlinable
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        // fp-swift's iterator, so the pipe and `untilFailure()` follow one rule.
        .iterating { .init(upstream.untilFailure().makeAsyncIterator()) }
    }
}

/// Keeps the elements up to and including the first failure, and then stops reading.
///
/// It passes each element through. After the first failure, it ends and does not read the
/// pipe before it again, so the source and any concurrent stage before it stop too:
///
/// ```swift
/// let uploads = Pipe<Receipt, UploadError> {
///     From(files)
///     AsyncFlatMap(concurrency: 4) { (file: File) in await upload(file) }
///     UntilFailure()
/// }
/// ```
///
/// Unlike `toResult()`, which also stops at the first failure, it keeps the values before the
/// failure, and the pipe can go on with more stages. It follows the same rule as fp-swift's
/// `untilFailure()`, which does the same before a sink.
@inlinable
public func UntilFailure() -> some PipeForwardingStage {
    UntilFailureStage()
}
