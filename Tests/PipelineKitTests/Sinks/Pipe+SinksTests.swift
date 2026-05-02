@testable import PipelineKit
import Synchronization
import Testing

private enum E: Error, Equatable { case bad }

// MARK: - toResult

@Test
func toResultIsDiscardableForFireAndForgetUse() async {
    let counter = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        Map { (n: Int) -> Int in
            counter.withLock { $0 += 1 }
            return n
        }
    }
    await pipe.toResult()
    #expect(counter.withLock { $0 } == 3)
}

@Test
func toResultOnEmptySourceYieldsEmptyArray() async {
    let pipe = Pipe<Int, Never> {
        From([Int]())
        Map { (n: Int) in n }
    }
    let result = await pipe.toResult()
    #expect(result == .success([]))
}

@Test
func toResultShortCircuitsAtFirstFailure() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in
            n == 2 ? .failure(.bad) : .success(n)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .failure(.bad))
}

// MARK: - toArray

@Test
func toArrayCollectsEveryElementAndDoesNotShortCircuit() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3, 4])
        FlatMap { (n: Int) -> Result<Int, E> in
            n.isMultiple(of: 2) ? .failure(.bad) : .success(n)
        }
    }

    let elements = await pipe.toArray()
    #expect(elements == [.success(1), .failure(.bad), .success(3), .failure(.bad)])
}

@Test
func toArrayOnEmptySourceIsEmpty() async {
    let pipe = Pipe<Int, E> {
        Empty(valueType: Int.self, failureType: E.self)
    }
    let elements = await pipe.toArray()
    #expect(elements.isEmpty)
}

// MARK: - reduce

@Test
func reduceSumsSuccess() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4])
        Map { (n: Int) in n }
    }
    let total = await pipe.reduce(0, +)
    #expect(total == .success(10))
}

// MARK: - first / firstSuccess / firstError

@Test
func firstReturnsLeadingElementRegardlessOfKind() async {
    let pipe = Pipe<Int, E> {
        From([1, 2])
        FlatMap { (_: Int) -> Result<Int, E> in .failure(.bad) }
    }
    let head = await pipe.first()
    #expect(head == .failure(.bad))
}

@Test
func firstSuccessSkipsLeadingFailures() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in
            n < 3 ? .failure(.bad) : .success(n * 10)
        }
    }
    let value = await pipe.firstSuccess()
    #expect(value == 30)
}

@Test
func firstSuccessIsNilWhenAllFailures() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap { (_: Int) -> Result<Int, E> in .failure(.bad) }
    }
    let value = await pipe.firstSuccess()
    #expect(value == nil)
}

@Test
func firstErrorSkipsLeadingSuccesses() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3, 4])
        FlatMap { (n: Int) -> Result<Int, E> in
            n < 3 ? .success(n) : .failure(.bad)
        }
    }
    let error = await pipe.firstError()
    #expect(error == .bad)
}

@Test
func firstErrorIsNilWhenAllSuccesses() async {
    let pipe = Pipe<Int, Never> { From([1, 2, 3]) }
    let error: Never? = await pipe.firstError()
    #expect(error == nil)
}
