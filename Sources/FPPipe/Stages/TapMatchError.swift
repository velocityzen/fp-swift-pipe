/// The stage behind `TapMatchError(pattern) { … }`.
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

/// The stage behind `TapMatchError(E.self) { … }`. It fits any pipe.
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

/// Runs `action` for each failure that equals `error`.
///
/// Every element passes through unchanged. See <doc:PatternMatching>.
@inlinable
public func TapMatchError<F: Error & Equatable & Sendable>(
    _ error: F,
    then action: @escaping @Sendable () -> Void,
) -> some PipePolyValueStage<F, F> {
    TapMatchErrorStage(EqualMatcher(error), action)
}

/// Runs `action` for each failure that is an `E`.
///
/// `action` gets the error cast to `E`. Every element passes through unchanged.
/// See <doc:PatternMatching>.
@inlinable
public func TapMatchError<E: Error>(
    _ type: E.Type,
    then action: @escaping @Sendable (E) -> Void,
) -> some PipeForwardingStage {
    TapMatchErrorTypeStage(action)
}

/// Runs `action` for each failure for which `predicate` returns `true`.
///
/// Every element passes through unchanged. See <doc:PatternMatching>.
@inlinable
public func TapMatchError<F: Error & Sendable>(
    where predicate: @escaping @Sendable (F) -> Bool,
    then action: @escaping @Sendable (F) -> Void,
) -> some PipePolyValueStage<F, F> {
    TapMatchErrorStage(PredicateMatcher(predicate), action)
}

/// Runs `action` for each failure for which `extract` returns a payload.
///
/// `action` gets the payload. Every element passes through unchanged. See <doc:PatternMatching>.
@inlinable
public func TapMatchError<F: Error & Sendable, Payload>(
    extract: @escaping @Sendable (F) -> Payload?,
    then action: @escaping @Sendable (Payload) -> Void,
) -> some PipePolyValueStage<F, F> {
    TapMatchErrorStage(ExtractMatcher(extract), action)
}
