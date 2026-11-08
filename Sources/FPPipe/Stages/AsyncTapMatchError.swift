/// Asynchronously observes the failures a pattern matches and passes every element
/// through unchanged. Runs sequentially, like `AsyncTapError`, to keep side effects in
/// order.
@usableFromInline
struct AsyncTapMatchErrorStage<Matcher: PipeMatcher>: PipePolyValueStage
where Matcher.Subject: Error & Sendable {
    @usableFromInline typealias InputFailure = Matcher.Subject
    @usableFromInline typealias OutputFailure = Matcher.Subject

    @usableFromInline let matcher: Matcher
    @usableFromInline let action: @Sendable (Matcher.Payload) async -> Void

    @inlinable
    init(_ matcher: Matcher, _ action: @escaping @Sendable (Matcher.Payload) async -> Void) {
        self.matcher = matcher
        self.action = action
    }

    @inlinable
    func attach<V: Sendable>(_ upstream: Pipe<V, InputFailure>) -> Pipe<V, InputFailure> {
        let step = AsyncTapElementStep(FailureMatcher<V, Matcher>(matcher), action)
        return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// `AsyncTapMatchError(E.self)`: polymorphic in both channels, like `TapMatchError(E.self)`.
@usableFromInline
struct AsyncTapMatchErrorTypeStage<E: Error>: PipeForwardingStage {
    @usableFromInline let action: @Sendable (E) async -> Void

    @inlinable
    init(_ action: @escaping @Sendable (E) async -> Void) {
        self.action = action
    }

    @inlinable
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let matcher = FailureMatcher<V, CastMatcher<F, E>>(CastMatcher())
        let step = AsyncTapElementStep(matcher, action)
        return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// DSL: `AsyncTapMatchError(AppError.rateLimited) { await metrics.rateLimited() }` — runs
/// for each failure equal to `error`.
@inlinable
public func AsyncTapMatchError<F: Error & Equatable & Sendable>(
    _ error: F,
    then action: @escaping @Sendable () async -> Void,
) -> some PipePolyValueStage<F, F> {
    AsyncTapMatchErrorStage(EqualMatcher(error), action)
}

/// DSL: `AsyncTapMatchError(URLError.self) { e in await report(e) }` — runs for each
/// failure that is an `E`, with the downcast error. Fits any pipe.
@inlinable
public func AsyncTapMatchError<E: Error>(
    _ type: E.Type,
    then action: @escaping @Sendable (E) async -> Void,
) -> some PipeForwardingStage {
    AsyncTapMatchErrorTypeStage(action)
}

/// DSL: `AsyncTapMatchError(where: \AppError.isRetryable) { e in await log(e) }` — runs for
/// each failure the predicate accepts.
@inlinable
public func AsyncTapMatchError<F: Error & Sendable>(
    where predicate: @escaping @Sendable (F) -> Bool,
    then action: @escaping @Sendable (F) async -> Void,
) -> some PipePolyValueStage<F, F> {
    AsyncTapMatchErrorStage(PredicateMatcher(predicate), action)
}

/// DSL: `AsyncTapMatchError(extract: \AppError.statusCode) { code in await record(code) }` —
/// runs for each failure the extractor returns a payload for, with that payload.
@inlinable
public func AsyncTapMatchError<F: Error & Sendable, Payload>(
    extract: @escaping @Sendable (F) -> Payload?,
    then action: @escaping @Sendable (Payload) async -> Void,
) -> some PipePolyValueStage<F, F> {
    AsyncTapMatchErrorStage(ExtractMatcher(extract), action)
}
