import FP

/// The stage that `UntilSuccess` returns.
@usableFromInline
struct UntilSuccessStage: PipeForwardingStage {
    @inlinable
    init() {}

    @inlinable
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        // fp-swift's iterator, so the pipe and `untilSuccess()` follow one rule.
        .iterating { .init(upstream.untilSuccess().makeAsyncIterator()) }
    }
}

/// Keeps the elements up to and including the first success, and then stops reading.
///
/// It passes each element through. After the first success, it ends and does not read the
/// pipe before it again, so the source and any concurrent stage before it stop too. Use it to
/// poll until a call succeeds:
///
/// ```swift
/// let checks = Pipe<Status, CheckError> {
///     FromResult(withRepeat(interval: .seconds(1)) { await check() })
///     UntilSuccess()
/// }
/// ```
///
/// Unlike `firstSuccess()`, it keeps the failures before the success, and the pipe can go on
/// with more stages. It follows the same rule as fp-swift's `untilSuccess()`, which does the
/// same before a sink.
@inlinable
public func UntilSuccess() -> some PipeForwardingStage {
    UntilSuccessStage()
}
