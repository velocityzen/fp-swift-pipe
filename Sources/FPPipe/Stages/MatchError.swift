/// The stage behind `MatchError(pattern) { … }`.
@usableFromInline
struct MatchErrorStage<Value: Sendable, Matcher: PipeMatcher>: PipeFlatErrorStage
where Matcher.Subject: Error & Sendable {
    @usableFromInline typealias F = Matcher.Subject
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

    @usableFromInline let matcher: Matcher
    @usableFromInline let handler: @Sendable (Matcher.Payload) -> Result<Value, F>

    @inlinable
    init(_ matcher: Matcher, _ handler: @escaping @Sendable (Matcher.Payload) -> Result<Value, F>) {
        self.matcher = matcher
        self.handler = handler
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = MatchElementStep(FailureMatcher<Value, Matcher>(matcher), handler)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// Recovers from, or fails again on, each failure that equals `error`.
///
/// Other elements pass through unchanged. See <doc:PatternMatching>.
@inlinable
public func MatchError<Value: Sendable, F: Error & Equatable & Sendable>(
    _ error: F,
    then handler: @escaping @Sendable () -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    MatchErrorStage(EqualMatcher(error), handler)
}

/// Recovers from, or fails again on, each failure that is an `E`.
///
/// The handler gets the error cast to `E`. Other elements pass through unchanged. Write the
/// handler's return type, like `e -> Result<Data, any Error> in`. See <doc:PatternMatching>.
@inlinable
public func MatchError<Value: Sendable, F: Error & Sendable, E: Error>(
    _ type: E.Type,
    then handler: @escaping @Sendable (E) -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    MatchErrorStage(CastMatcher<F, E>(), handler)
}

/// Recovers from, or fails again on, each failure for which `predicate` returns `true`.
///
/// Other elements pass through unchanged. See <doc:PatternMatching>.
@inlinable
public func MatchError<Value: Sendable, F: Error & Sendable>(
    where predicate: @escaping @Sendable (F) -> Bool,
    then handler: @escaping @Sendable (F) -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    MatchErrorStage(PredicateMatcher(predicate), handler)
}

/// Recovers from, or fails again on, each failure for which `extract` returns a payload.
///
/// The handler gets the payload. Other elements pass through unchanged. See <doc:PatternMatching>.
@inlinable
public func MatchError<Value: Sendable, F: Error & Sendable, Payload>(
    extract: @escaping @Sendable (F) -> Payload?,
    then handler: @escaping @Sendable (Payload) -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    MatchErrorStage(ExtractMatcher(extract), handler)
}
