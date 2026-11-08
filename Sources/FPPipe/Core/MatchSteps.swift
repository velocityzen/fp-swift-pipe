// The steps behind the `Match`-family stages. Each runs on whole `Result` elements
// through a matcher lifted onto one channel (`SuccessMatcher` / `FailureMatcher`), so
// the same step serves the success-side and the failure-side stages.

/// Replaces each element the matcher matches with the element its handler returns.
@usableFromInline
struct MatchElementStep<Matcher: PipeMatcher>: PipeStep {
    @usableFromInline let matcher: Matcher
    @usableFromInline let handler: @Sendable (Matcher.Payload) -> Matcher.Subject

    @inlinable
    init(_ matcher: Matcher, _ handler: @escaping @Sendable (Matcher.Payload) -> Matcher.Subject) {
        self.matcher = matcher
        self.handler = handler
    }

    @inlinable
    func callAsFunction(_ element: Matcher.Subject) -> Matcher.Subject? {
        guard let payload = matcher(element) else { return element }
        return handler(payload)
    }
}

/// Runs the action for each element the matcher matches; every element passes through.
@usableFromInline
struct TapElementStep<Matcher: PipeMatcher>: PipeStep {
    @usableFromInline let matcher: Matcher
    @usableFromInline let action: @Sendable (Matcher.Payload) -> Void

    @inlinable
    init(_ matcher: Matcher, _ action: @escaping @Sendable (Matcher.Payload) -> Void) {
        self.matcher = matcher
        self.action = action
    }

    @inlinable
    func callAsFunction(_ element: Matcher.Subject) -> Matcher.Subject? {
        if let payload = matcher(element) {
            action(payload)
        }
        return element
    }
}

/// `MatchElementStep` with an asynchronous handler.
@usableFromInline
struct AsyncMatchElementStep<Matcher: PipeMatcher>: AsyncPipeStep {
    @usableFromInline let matcher: Matcher
    @usableFromInline let handler: @Sendable (Matcher.Payload) async -> Matcher.Subject

    @inlinable
    init(
        _ matcher: Matcher,
        _ handler: @escaping @Sendable (Matcher.Payload) async -> Matcher.Subject,
    ) {
        self.matcher = matcher
        self.handler = handler
    }

    @inlinable
    func callAsFunction(_ element: Matcher.Subject) async -> Matcher.Subject? {
        guard let payload = matcher(element) else { return element }
        return await handler(payload)
    }

    /// The element a sorted element ends up as: the handler's result for a match, the
    /// element itself otherwise.
    @inlinable
    func resolve(_ sorted: Matched<Matcher.Payload, Matcher.Subject>) async -> Matcher.Subject {
        switch sorted {
            case .matched(let payload): await handler(payload)
            case .unmatched(let element): element
        }
    }
}

/// `TapElementStep` with an asynchronous action.
@usableFromInline
struct AsyncTapElementStep<Matcher: PipeMatcher>: AsyncPipeStep {
    @usableFromInline let matcher: Matcher
    @usableFromInline let action: @Sendable (Matcher.Payload) async -> Void

    @inlinable
    init(_ matcher: Matcher, _ action: @escaping @Sendable (Matcher.Payload) async -> Void) {
        self.matcher = matcher
        self.action = action
    }

    @inlinable
    func callAsFunction(_ element: Matcher.Subject) async -> Matcher.Subject? {
        if let payload = matcher(element) {
            await action(payload)
        }
        return element
    }
}
