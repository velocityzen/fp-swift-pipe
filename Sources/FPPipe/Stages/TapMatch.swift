/// The stage behind `TapMatch(pattern) { … }`.
@usableFromInline
struct TapMatchStage<Matcher: PipeMatcher>: PipePolyStage where Matcher.Subject: Sendable {
    @usableFromInline typealias Input = Matcher.Subject
    @usableFromInline typealias Output = Matcher.Subject

    @usableFromInline let matcher: Matcher
    @usableFromInline let action: @Sendable (Matcher.Payload) -> Void

    @inlinable
    init(_ matcher: Matcher, _ action: @escaping @Sendable (Matcher.Payload) -> Void) {
        self.matcher = matcher
        self.action = action
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Input, F> {
        let step = TapElementStep(SuccessMatcher<Matcher, F>(matcher), action)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// The stage behind `TapMatch(T.self) { … }`. It fits any pipe.
@usableFromInline
struct TapMatchTypeStage<T>: PipeForwardingStage {
    @usableFromInline let action: @Sendable (T) -> Void

    @inlinable
    init(_ action: @escaping @Sendable (T) -> Void) {
        self.action = action
    }

    @inlinable
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let step = TapElementStep(SuccessMatcher<CastMatcher<V, T>, F>(CastMatcher()), action)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// Runs `action` for each success that equals `value`.
///
/// Every element passes through unchanged. See <doc:PatternMatching>.
@inlinable
public func TapMatch<Value: Equatable & Sendable>(
    _ value: Value,
    then action: @escaping @Sendable () -> Void,
) -> some PipePolyStage<Value, Value> {
    TapMatchStage(EqualMatcher(value), action)
}

/// Runs `action` for each success that is a `T`.
///
/// `action` gets the value cast to `T`. Every element passes through unchanged.
/// See <doc:PatternMatching>.
@inlinable
public func TapMatch<T>(
    _ type: T.Type,
    then action: @escaping @Sendable (T) -> Void,
) -> some PipeForwardingStage {
    TapMatchTypeStage(action)
}

/// Runs `action` for each success for which `predicate` returns `true`.
///
/// Every element passes through unchanged. See <doc:PatternMatching>.
@inlinable
public func TapMatch<Value: Sendable>(
    where predicate: @escaping @Sendable (Value) -> Bool,
    then action: @escaping @Sendable (Value) -> Void,
) -> some PipePolyStage<Value, Value> {
    TapMatchStage(PredicateMatcher(predicate), action)
}

/// Runs `action` for each success for which `extract` returns a payload.
///
/// `action` gets the payload. Every element passes through unchanged. See <doc:PatternMatching>.
@inlinable
public func TapMatch<Value: Sendable, Payload>(
    extract: @escaping @Sendable (Value) -> Payload?,
    then action: @escaping @Sendable (Payload) -> Void,
) -> some PipePolyStage<Value, Value> {
    TapMatchStage(ExtractMatcher(extract), action)
}
