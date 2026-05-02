public extension Pipe {
    /// Collect every success into an array, all-or-nothing.
    /// On the first `.failure`, iteration stops and that failure is returned.
    /// `@discardableResult` so this also serves as a "drive to completion" terminal
    /// when the values aren't needed.
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

    /// Iterate the pipeline to completion and return every element. Never stops on failure.
    func toArray() async -> [Result<Success, Failure>] {
        var elements: [Result<Success, Failure>] = []
        for await element in self {
            elements.append(element)
        }
        return elements
    }

    /// Return the first element produced by the pipeline (success or failure), or
    /// `nil` if the pipeline is empty.
    func first() async -> Result<Success, Failure>? {
        for await element in self {
            return element
        }
        return nil
    }

    /// Return the first successful value, skipping any leading failures.
    /// `nil` if the pipeline never emits a success.
    func firstSuccess() async -> Success? {
        for await element in self {
            if case .success(let value) = element { return value }
        }
        return nil
    }

    /// Return the first error, skipping any leading successes.
    /// `nil` if the pipeline never emits a failure.
    func firstError() async -> Failure? {
        for await element in self {
            if case .failure(let error) = element { return error }
        }
        return nil
    }

    /// Reduce the success channel into a single value, all-or-nothing.
    /// The first `.failure` short-circuits.
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
