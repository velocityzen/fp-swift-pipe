/// Returns the payload for the handler, or `nil` when there is no match.
@usableFromInline
protocol PipeMatcher<Subject, Payload>: Sendable {
    associatedtype Subject
    associatedtype Payload

    func callAsFunction(_ subject: Subject) -> Payload?
}

/// Matches subjects equal to `expected`.
@usableFromInline
struct EqualMatcher<Subject: Equatable & Sendable>: PipeMatcher {
    @usableFromInline let expected: Subject

    @inlinable
    init(_ expected: Subject) {
        self.expected = expected
    }

    @inlinable
    func callAsFunction(_ subject: Subject) -> Void? {
        subject == expected ? () : nil
    }
}

/// Matches subjects that cast to `Payload`.
@usableFromInline
struct CastMatcher<Subject, Payload>: PipeMatcher {
    @inlinable
    init() {}

    @inlinable
    func callAsFunction(_ subject: Subject) -> Payload? {
        subject as? Payload
    }
}

/// Matches subjects for which the predicate returns `true`.
@usableFromInline
struct PredicateMatcher<Subject>: PipeMatcher {
    @usableFromInline let predicate: @Sendable (Subject) -> Bool

    @inlinable
    init(_ predicate: @escaping @Sendable (Subject) -> Bool) {
        self.predicate = predicate
    }

    @inlinable
    func callAsFunction(_ subject: Subject) -> Subject? {
        predicate(subject) ? subject : nil
    }
}

/// Matches subjects for which `extract` returns a payload.
@usableFromInline
struct ExtractMatcher<Subject, Payload>: PipeMatcher {
    @usableFromInline let extract: @Sendable (Subject) -> Payload?

    @inlinable
    init(_ extract: @escaping @Sendable (Subject) -> Payload?) {
        self.extract = extract
    }

    @inlinable
    func callAsFunction(_ subject: Subject) -> Payload? {
        extract(subject)
    }
}

// MARK: - Channel lifts
//
// A pattern checks one channel only. These matchers check the whole `Result` element, so
// one step works for the success stages (`Match`, `TapMatch`) and the failure stages
// (`MatchError`, `TapMatchError`).

/// Matches `.success` elements whose value matches `base`.
@usableFromInline
struct SuccessMatcher<Base: PipeMatcher, F: Error>: PipeMatcher {
    @usableFromInline let base: Base

    @inlinable
    init(_ base: Base) {
        self.base = base
    }

    @inlinable
    func callAsFunction(_ element: Result<Base.Subject, F>) -> Base.Payload? {
        guard case .success(let value) = element else { return nil }
        return base(value)
    }
}

/// Matches `.failure` elements whose error matches `base`.
@usableFromInline
struct FailureMatcher<Value, Base: PipeMatcher>: PipeMatcher where Base.Subject: Error {
    @usableFromInline let base: Base

    @inlinable
    init(_ base: Base) {
        self.base = base
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, Base.Subject>) -> Base.Payload? {
        guard case .failure(let error) = element else { return nil }
        return base(error)
    }
}
