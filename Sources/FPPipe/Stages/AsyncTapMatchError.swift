/// The stage behind `AsyncTapMatchError(pattern) { … }`.
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

/// The stage behind `AsyncTapMatchError(E.self) { … }`.
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

/// Runs `action` for each failure that equals `error`.
///
/// It handles one element at a time. Every element passes through unchanged.
/// See <doc:PatternMatching>.
@inlinable
public func AsyncTapMatchError<F: Error & Equatable & Sendable>(
    _ error: F,
    then action: @escaping @Sendable () async -> Void,
) -> some PipePolyValueStage<F, F> {
    AsyncTapMatchErrorStage(EqualMatcher(error), action)
}

/// Runs `action` for each failure that is an `E`.
///
/// `action` gets the error cast to `E`. It handles one element at a time. Every element passes
/// through unchanged. See <doc:PatternMatching>.
@inlinable
public func AsyncTapMatchError<E: Error>(
    _ type: E.Type,
    then action: @escaping @Sendable (E) async -> Void,
) -> some PipeForwardingStage {
    AsyncTapMatchErrorTypeStage(action)
}

/// Runs `action` for each failure for which `predicate` returns `true`.
///
/// It handles one element at a time. Every element passes through unchanged.
/// See <doc:PatternMatching>.
@inlinable
public func AsyncTapMatchError<F: Error & Sendable>(
    where predicate: @escaping @Sendable (F) -> Bool,
    then action: @escaping @Sendable (F) async -> Void,
) -> some PipePolyValueStage<F, F> {
    AsyncTapMatchErrorStage(PredicateMatcher(predicate), action)
}

/// Runs `action` for each failure for which `extract` returns a payload.
///
/// `action` gets the payload. It handles one element at a time. Every element passes through
/// unchanged. See <doc:PatternMatching>.
@inlinable
public func AsyncTapMatchError<F: Error & Sendable, Payload>(
    extract: @escaping @Sendable (F) -> Payload?,
    then action: @escaping @Sendable (Payload) async -> Void,
) -> some PipePolyValueStage<F, F> {
    AsyncTapMatchErrorStage(ExtractMatcher(extract), action)
}
