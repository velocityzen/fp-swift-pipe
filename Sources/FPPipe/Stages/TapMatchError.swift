/// Observes the failures a pattern matches and passes every element through unchanged.
/// Successes and non-matching failures are not observed.
@usableFromInline
struct TapMatchErrorStage<Matcher: PipeMatcher>: PipePolyValueStage
where Matcher.Subject: Error & Sendable {
    @usableFromInline typealias InputFailure = Matcher.Subject
    @usableFromInline typealias OutputFailure = Matcher.Subject

    @usableFromInline let matcher: Matcher
    @usableFromInline let action: @Sendable (Matcher.Payload) -> Void

    @inlinable
    init(_ matcher: Matcher, _ action: @escaping @Sendable (Matcher.Payload) -> Void) {
        self.matcher = matcher
        self.action = action
    }

    @inlinable
    func attach<V: Sendable>(_ upstream: Pipe<V, InputFailure>) -> Pipe<V, InputFailure> {
        let step = TapElementStep(FailureMatcher<V, Matcher>(matcher), action)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// `TapMatchError(E.self)`: polymorphic in both channels, because a dynamic cast works on
/// any failure type — typically an `any Error` channel.
@usableFromInline
struct TapMatchErrorTypeStage<E: Error>: PipeForwardingStage {
    @usableFromInline let action: @Sendable (E) -> Void

    @inlinable
    init(_ action: @escaping @Sendable (E) -> Void) {
        self.action = action
    }

    @inlinable
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let step = TapElementStep(FailureMatcher<V, CastMatcher<F, E>>(CastMatcher()), action)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// DSL: `TapMatchError(AppError.rateLimited) { metrics.rateLimited() }` — runs for each
/// failure equal to `error`.
@inlinable
public func TapMatchError<F: Error & Equatable & Sendable>(
    _ error: F,
    then action: @escaping @Sendable () -> Void,
) -> some PipePolyValueStage<F, F> {
    TapMatchErrorStage(EqualMatcher(error), action)
}

/// DSL: `TapMatchError(URLError.self) { e in log(e.code) }` — runs for each failure that is
/// an `E`, with the downcast error. Fits any pipe; the cast decides what matches.
@inlinable
public func TapMatchError<E: Error>(
    _ type: E.Type,
    then action: @escaping @Sendable (E) -> Void,
) -> some PipeForwardingStage {
    TapMatchErrorTypeStage(action)
}

/// DSL: `TapMatchError(where: \AppError.isRetryable) { e in log(e) }` — runs for each
/// failure the predicate accepts.
@inlinable
public func TapMatchError<F: Error & Sendable>(
    where predicate: @escaping @Sendable (F) -> Bool,
    then action: @escaping @Sendable (F) -> Void,
) -> some PipePolyValueStage<F, F> {
    TapMatchErrorStage(PredicateMatcher(predicate), action)
}

/// DSL: `TapMatchError(extract: \AppError.statusCode) { code in metrics.record(code) }` —
/// runs for each failure the extractor returns a payload for (typically one enum case's
/// associated value), with that payload.
@inlinable
public func TapMatchError<F: Error & Sendable, Payload>(
    extract: @escaping @Sendable (F) -> Payload?,
    then action: @escaping @Sendable (Payload) -> Void,
) -> some PipePolyValueStage<F, F> {
    TapMatchErrorStage(ExtractMatcher(extract), action)
}
