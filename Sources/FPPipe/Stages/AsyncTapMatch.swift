/// Asynchronously observes the successes a pattern matches and passes every element
/// through unchanged. Runs sequentially, like `AsyncTap`, to keep side effects in order.
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

/// `AsyncTapMatch(T.self)`: polymorphic in both channels, like `TapMatch(T.self)`.
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

/// DSL: `AsyncTapMatch(Event.logout) { await session.clear() }` — runs for each success
/// equal to `value`.
@inlinable
public func AsyncTapMatch<Value: Equatable & Sendable>(
    _ value: Value,
    then action: @escaping @Sendable () async -> Void,
) -> some PipePolyStage<Value, Value> {
    AsyncTapMatchStage(EqualMatcher(value), action)
}

/// DSL: `AsyncTapMatch(Dog.self) { dog in await kennel.checkIn(dog) }` — runs for each
/// success that is a `T`, with the downcast value. Fits any pipe.
@inlinable
public func AsyncTapMatch<T>(
    _ type: T.Type,
    then action: @escaping @Sendable (T) async -> Void,
) -> some PipeForwardingStage {
    AsyncTapMatchTypeStage(action)
}

/// DSL: `AsyncTapMatch(where: { (o: Order) in o.isLarge }, then: { o in await review(o) })`
/// — runs for each success the predicate accepts.
@inlinable
public func AsyncTapMatch<Value: Sendable>(
    where predicate: @escaping @Sendable (Value) -> Bool,
    then action: @escaping @Sendable (Value) async -> Void,
) -> some PipePolyStage<Value, Value> {
    AsyncTapMatchStage(PredicateMatcher(predicate), action)
}

/// DSL: `AsyncTapMatch(extract: \Event.loggedInUser) { user in await audit(user) }` — runs
/// for each success the extractor returns a payload for, with that payload.
@inlinable
public func AsyncTapMatch<Value: Sendable, Payload>(
    extract: @escaping @Sendable (Value) -> Payload?,
    then action: @escaping @Sendable (Payload) async -> Void,
) -> some PipePolyStage<Value, Value> {
    AsyncTapMatchStage(ExtractMatcher(extract), action)
}
