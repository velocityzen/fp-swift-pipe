# Getting Started

Add FPPipe to your package, build your first pipe, and learn how its parts work together.

## Installation

Requirements:

- Swift 6.3 or later, in Swift 6 language mode
- macOS, iOS, tvOS, watchOS or visionOS 26 or later, or Linux

Add the package to `Package.swift`:

```swift
dependencies: [
    .package(
        url: "https://github.com/velocityzen/fp-swift-pipe",
        from: "2.1.0"
    ),
],
```

Then add the `FPPipe` product to your target:

```swift
.target(
    name: "MyApp",
    dependencies: [.product(name: "FPPipe", package: "fp-swift-pipe")]
),
```

## Quick start

```swift
import FPPipe

let pipe = Pipe<Int, Never> {
    From(1...6)                                 // 1, 2, 3, 4, 5, 6
    Filter { (n: Int) in n.isMultiple(of: 2) }  // 2, 4, 6
    Map { (n: Int) in n * 10 }                  // 20, 40, 60
}

let result = await pipe.toResult()  // .success([20, 40, 60])
```

`Pipe<Int, Never>` gives `Int` values and cannot fail (`Never`). Creating a pipe does no work. It runs each time you read it, here with `toResult()`.

## How it works

A pipe has three kinds of parts:

| Part   | Job                                                 | Examples                  |
| ------ | --------------------------------------------------- | ------------------------- |
| Source | Puts elements into the pipe. It always comes first. | `From`, `FromAsync`       |
| Stage  | Changes, removes or watches elements.               | `Map`, `Filter`, `Tap`    |
| Sink   | Reads the results out of the pipe.                  | `toResult()`, `for await` |

Each element is a `Result<Success, Failure>`: a value (a success) or an error (a failure). Most stages work on one side only:

- Value stages, like `Map` and `Filter`, change successes. Failures pass through.
- Error stages, like `MapError` and `FlatMapError`, change failures. Successes pass through.

So one failure does not stop the other elements:

```swift
enum ParseError: Error, Equatable {
    case notANumber(String)
}

let numbers = Pipe<Int, ParseError> {
    From(["1", "x", "3"])
    FlatMap { (text: String) -> Result<Int, ParseError> in
        guard let n = Int(text) else {
            return .failure(.notANumber(text))
        }
        return .success(n)
    }
    Map { (n: Int) in n * 2 }   // runs for 1 and 3 only
}

await numbers.toArray()
// [.success(2), .failure(.notANumber("x")), .success(6)]

await numbers.toResult()  // .failure(.notANumber("x"))
```

`toArray()` returns every element. `toResult()` stops at the first failure.

`From` cannot fail. `FlatMap` is the first stage that can, so it sets the failure type to `ParseError`. You do not need a stage to change `Never` into `ParseError`.

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
