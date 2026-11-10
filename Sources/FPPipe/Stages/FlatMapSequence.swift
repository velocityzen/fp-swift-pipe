import FP

/// The stage that `FlatMapSequence` returns.
@usableFromInline
struct FlatMapSequenceStage<Input: Sendable, Inner: Sequence & Sendable>: PipePolyStage
where Inner.Element: Sendable {
    @usableFromInline typealias Output = Inner.Element

    @usableFromInline let bufferSize: Int
    @usableFromInline let transform: @Sendable (Input) -> Inner

    @inlinable
    init(_ transform: @escaping @Sendable (Input) -> Inner, bufferSize: Int) {
        precondition(bufferSize >= 1, "bufferSize must be at least 1")
        self.bufferSize = bufferSize
        self.transform = transform
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let transform = self.transform
        let expansion = Pipe<Output, F>.iterating {
            .init(FlatMapSequenceIterator(upstream.makeAsyncIterator(), transform: transform))
        }
        if bufferSize == 1 {
            return expansion
        }
        let bufferSize = self.bufferSize
        return .iterating { .init(expansion.readAhead(bufferSize).makeAsyncIterator()) }
    }
}

/// Reads the next input element only when the current inner sequence ends. With `bufferSize`
/// above 1, it runs in the read-ahead task.
@usableFromInline
struct FlatMapSequenceIterator<Base: AsyncIteratorProtocol, Input, Inner: Sequence, F: Error>:
    AsyncIteratorProtocol
where Base.Element == Result<Input, F>, Base.Failure == Never {
    @usableFromInline var base: Base
    @usableFromInline let transform: @Sendable (Input) -> Inner
    @usableFromInline var inner: Inner.Iterator?

    @inlinable
    init(_ base: Base, transform: @escaping @Sendable (Input) -> Inner) {
        self.base = base
        self.transform = transform
    }

    @inlinable
    mutating func next() async -> Result<Inner.Element, F>? {
        await next(isolation: #isolation)
    }

    @inlinable
    mutating func next(
        isolation actor: isolated (any Actor)?
    ) async -> Result<Inner.Element, F>? {
        while !Task.isCancelled {
            if inner != nil {
                if let value = inner!.next() { return .success(value) }
                inner = nil
            }
            switch await base.next(isolation: actor) {
                case nil: return nil
                case .failure(let error)?: return .failure(error)
                case .success(let value)?: inner = transform(value).makeIterator()
            }
        }
        return nil
    }
}

/// Turns each value into a `Sequence`, and passes on all of its elements. Failures pass
/// through unchanged.
///
/// `bufferSize` must be 1 or more. With the default of 1, nothing is made ahead of the
/// reader. With a larger value, a separate task makes up to that many elements ahead. See
/// <doc:TurningOneElementIntoMany>.
@inlinable
public func FlatMapSequence<Input: Sendable, Inner: Sequence & Sendable>(
    bufferSize: Int = 1,
    _ transform: @escaping @Sendable (Input) -> Inner,
) -> some PipePolyStage<Input, Inner.Element> where Inner.Element: Sendable {
    FlatMapSequenceStage(transform, bufferSize: bufferSize)
}
