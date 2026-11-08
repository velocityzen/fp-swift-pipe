/// Observes the successes a pattern matches and passes every element through unchanged.
/// Failures and non-matching successes are not observed.
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

/// `TapMatch(T.self)`: polymorphic in both channels, because a dynamic cast works on any
/// value type — the pipe's types are whatever the upstream carries.
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

/// DSL: `TapMatch(Event.logout) { session.clear() }` — runs for each success equal to
/// `value`.
@inlinable
public func TapMatch<Value: Equatable & Sendable>(
    _ value: Value,
    then action: @escaping @Sendable () -> Void,
) -> some PipePolyStage<Value, Value> {
    TapMatchStage(EqualMatcher(value), action)
}

/// DSL: `TapMatch(Dog.self) { dog in print(dog.name) }` — runs for each success that is a
/// `T`, with the downcast value. Fits any pipe; the cast decides what matches.
@inlinable
public func TapMatch<T>(
    _ type: T.Type,
    then action: @escaping @Sendable (T) -> Void,
) -> some PipeForwardingStage {
    TapMatchTypeStage(action)
}

/// DSL: `TapMatch(where: { (n: Int) in n > 100 }, then: { n in log(n) })` — runs for each
/// success the predicate accepts.
@inlinable
public func TapMatch<Value: Sendable>(
    where predicate: @escaping @Sendable (Value) -> Bool,
    then action: @escaping @Sendable (Value) -> Void,
) -> some PipePolyStage<Value, Value> {
    TapMatchStage(PredicateMatcher(predicate), action)
}

/// DSL: `TapMatch(extract: \Event.loggedInUser) { user in audit(user) }` — runs for each
/// success the extractor returns a payload for (typically one enum case's associated
/// value), with that payload.
@inlinable
public func TapMatch<Value: Sendable, Payload>(
    extract: @escaping @Sendable (Value) -> Payload?,
    then action: @escaping @Sendable (Payload) -> Void,
) -> some PipePolyStage<Value, Value> {
    TapMatchStage(ExtractMatcher(extract), action)
}
