# Conditions Inside a Pipe

Choose stages with `if`, `else` and `switch`.

You can use `if`, `if`/`else` and `switch` inside a pipe.

With `if`/`else` and `switch`, every branch must give the same stage type. In the example below, each `if` branch is a `Map<Int, Int>`, and each `switch` case is a `Filter<Int>`. A `Map` in one branch and a `Filter` in another does not compile.

```swift
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
```

`if` without `else` works only for stages that keep the element types the same. When the condition is false, the pipe must have the same type as when it is true. These stages work:

- `Take`, `Drop`, `Tap`, `TapError` and their async versions
- every pattern-matching stage (see <doc:PatternMatching>)
- `Map`, `Filter`, `FlatMap` and their async versions, when the value type does not change
- `Alt`, `FlatMapError` and their async versions, when the failure type does not change

```swift
let pipe = Pipe<Int, Never> {
    From(0..<10)
    if logging { Tap { (n: Int) in print(n) } }
    if skipFirst { Drop(2) }
    if evenOnly { Filter { (n: Int) in n.isMultiple(of: 2) } }
}

let parsed = Pipe<Int, ParseError> {
    From(lines)
    FlatMap(parse)  // String -> Result<Int, ParseError>
    if lenient {
        Alt { Result<Int, ParseError>.success(0) }  // keeps ParseError
    }
}
```

A stage that changes a type, like `Map { (n: Int) in String(n) }`, does not compile in an `if` without `else`. Add an `else` branch with another stage of the same type.

`for` loops do not work inside a pipe. Combine the loop's work into one stage outside the pipe:

```swift
// Instead of `for f in transforms { Map(f) }`:
let combined: @Sendable (Int) -> Int = { n in
    transforms.reduce(n) { value, transform in transform(value) }
}

let pipe = Pipe<Int, Never> {
    From(0..<10)
    Map(combined)
}
```

When branches need different stage types, choose outside the pipe. A pipe is an `AsyncSequence` of `Result`s, so `FromResult` can read it as the source of a new pipe:

```swift
let base = Pipe<Int, Never> { From(0..<10) }

let final = needsDoubling
    ? Pipe<Int, Never> {
        FromResult(base)
        Map { (n: Int) in n * 2 }
    }
    : base
```

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->
