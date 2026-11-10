// Examples from "Conditions inside a pipe" in Readme.md and the DocC articles. Keep all
// three copies the same.

import FPPipe
import Testing

private enum ParseError: Error, Equatable {
    case notANumber(String)
}

private enum Mode { case even, odd, all }

private func parse(_ text: String) -> Result<Int, ParseError> {
    Int(text).map { .success($0) } ?? .failure(.notANumber(text))
}

@Test
func readmeIfElseAndSwitch() async {
    let needsDoubling = false
    let mode = Mode.even

    let pipe = Pipe<Int, Never> {
        From(0..<10)

        if needsDoubling {
            Map { (n: Int) in n * 2 }
        } else {
            Map { (n: Int) in n + 1 }
        }

        switch mode {
            case .even: Filter { (n: Int) in n.isMultiple(of: 2) }
            case .odd: Filter { (n: Int) in !n.isMultiple(of: 2) }
            case .all: Filter { (_: Int) in true }
        }
    }

    #expect(await pipe.toResult() == .success([2, 4, 6, 8, 10]))
}

@Test
func readmeIfWithoutElse() async {
    let logging = false
    let skipFirst = true
    let evenOnly = true

    let pipe = Pipe<Int, Never> {
        From(0..<10)
        if logging { Tap { (n: Int) in print(n) } }
        if skipFirst { Drop(2) }
        if evenOnly { Filter { (n: Int) in n.isMultiple(of: 2) } }
    }
    #expect(await pipe.toResult() == .success([2, 4, 6, 8]))

    let lines = ["1", "x"]
    let lenient = true

    let parsed = Pipe<Int, ParseError> {
        From(lines)
        FlatMap(parse)  // String -> Result<Int, ParseError>
        if lenient {
            Alt { Result<Int, ParseError>.success(0) }  // keeps ParseError
        }
    }
    #expect(await parsed.toResult() == .success([1, 0]))
}

@Test
func readmeForLoopOutsideThePipe() async {
    let transforms: [@Sendable (Int) -> Int] = [{ $0 + 1 }, { $0 * 2 }]

    // Instead of `for f in transforms { Map(f) }`:
    let combined: @Sendable (Int) -> Int = { n in
        transforms.reduce(n) { value, transform in transform(value) }
    }

    let pipe = Pipe<Int, Never> {
        From(0..<3)
        Map(combined)
    }

    // The transforms run in order: (n + 1) * 2.
    #expect(await pipe.toResult() == .success([2, 4, 6]))
}

@Test
func readmeBranchOutsideThePipe() async {
    let needsDoubling = true
    let base = Pipe<Int, Never> { From(0..<10) }

    let final =
        needsDoubling
        ? Pipe<Int, Never> {
            FromResult(base)
            Map { (n: Int) in n * 2 }
        }
        : base

    #expect(await final.toResult() == .success(Array(stride(from: 0, to: 20, by: 2))))
}
