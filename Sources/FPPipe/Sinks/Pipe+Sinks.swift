// MARK: - Sinks
//
// Building a pipe does no work. It runs only when a sink or `for await` reads it.
//
// To stop at your own condition, use `for await` and `break`. When the loop ends, its
// iterator is released, and that stops the earlier stages.

public extension Pipe {
    /// Returns all values, or the first failure.
    ///
    /// It stops reading at the first failure. To only run the pipe, ignore the result.
    @inlinable
    @discardableResult
    func toResult() async -> Result<[Success], Failure> {
        var values: [Success] = []
        for await element in self {
            switch element {
                case .success(let value):
                    values.append(value)
                case .failure(let error):
                    return .failure(error)
            }
        }
        return .success(values)
    }

    /// Returns every element, values and failures.
    ///
    /// It reads to the end.
    @inlinable
    func toArray() async -> [Result<Success, Failure>] {
        var elements: [Result<Success, Failure>] = []
        for await element in self {
            elements.append(element)
        }
        return elements
    }

    /// Returns the first element, or `nil` if the pipe is empty.
    ///
    /// It stops reading after the first element.
    @inlinable
    func first() async -> Result<Success, Failure>? {
        for await element in self {
            return element
        }
        return nil
    }

    /// Returns the first value, or `nil` if there is none.
    ///
    /// It stops reading at the first success.
    @inlinable
    func firstSuccess() async -> Success? {
        for await element in self {
            if case .success(let value) = element { return value }
        }
        return nil
    }

    /// Returns the first error, or `nil` if there is none.
    ///
    /// It stops reading at the first failure.
    @inlinable
    func firstError() async -> Failure? {
        for await element in self {
            if case .failure(let error) = element { return error }
        }
        return nil
    }

    /// Combines all values into one value, or returns the first failure.
    ///
    /// It starts with `initial` and stops reading at the first failure.
    @inlinable
    func reduce<U: Sendable>(
        _ initial: U,
        _ combine: @Sendable (U, Success) -> U,
    ) async -> Result<U, Failure> {
        var acc = initial
        for await element in self {
            switch element {
                case .success(let value):
                    acc = combine(acc, value)
                case .failure(let error):
                    return .failure(error)
            }
        }
        return .success(acc)
    }
}
