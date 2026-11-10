/// The stage behind `AsyncTapMatch(pattern) { … }`.
@usableFromInline
struct AsyncTapMatchStage<Matcher: PipeMatcher>: PipePolyStage where Matcher.Subject: Sendable {
    @usableFromInline typealias Input = Matcher.Subject
    @usableFromInline typealias Output = Matcher.Subject

    @usableFromInline let matcher: Matcher
    @usableFromInline let action: @Sendable (Matcher.Payload) async -> Void

    @inlinable
    init(_ matcher: Matcher, _ action: @escaping @Sendable (Matcher.Payload) async -> Void) {
        self.matcher = matcher
        self.action = action
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Input, F> {
        let step = AsyncTapElementStep(SuccessMatcher<Matcher, F>(matcher), action)
        return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// The stage behind `AsyncTapMatch(T.self) { … }`.
@usableFromInline
struct AsyncTapMatchTypeStage<T>: PipeForwardingStage {
    @usableFromInline let action: @Sendable (T) async -> Void

    @inlinable
    init(_ action: @escaping @Sendable (T) async -> Void) {
        self.action = action
    }

    @inlinable
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let matcher = SuccessMatcher<CastMatcher<V, T>, F>(CastMatcher())
        let step = AsyncTapElementStep(matcher, action)
        return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// Runs `action` for each success that equals `value`.
///
/// It handles one element at a time. Every element passes through unchanged.
/// See <doc:PatternMatching>.
@inlinable
public func AsyncTapMatch<Value: Equatable & Sendable>(
    _ value: Value,
    then action: @escaping @Sendable () async -> Void,
) -> some PipePolyStage<Value, Value> {
    AsyncTapMatchStage(EqualMatcher(value), action)
}

/// Runs `action` for each success that is a `T`.
///
/// `action` gets the value cast to `T`. It handles one element at a time. Every element passes
/// through unchanged. See <doc:PatternMatching>.
@inlinable
public func AsyncTapMatch<T>(
    _ type: T.Type,
    then action: @escaping @Sendable (T) async -> Void,
) -> some PipeForwardingStage {
    AsyncTapMatchTypeStage(action)
}

/// Runs `action` for each success for which `predicate` returns `true`.
///
/// It handles one element at a time. Every element passes through unchanged.
/// See <doc:PatternMatching>.
@inlinable
public func AsyncTapMatch<Value: Sendable>(
    where predicate: @escaping @Sendable (Value) -> Bool,
    then action: @escaping @Sendable (Value) async -> Void,
) -> some PipePolyStage<Value, Value> {
    AsyncTapMatchStage(PredicateMatcher(predicate), action)
}

/// Runs `action` for each success for which `extract` returns a payload.
///
/// `action` gets the payload. It handles one element at a time. Every element passes through
/// unchanged. See <doc:PatternMatching>.
@inlinable
public func AsyncTapMatch<Value: Sendable, Payload>(
    extract: @escaping @Sendable (Value) -> Payload?,
    then action: @escaping @Sendable (Payload) async -> Void,
) -> some PipePolyStage<Value, Value> {
    AsyncTapMatchStage(ExtractMatcher(extract), action)
}
