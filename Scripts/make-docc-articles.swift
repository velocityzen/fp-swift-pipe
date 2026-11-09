#!/usr/bin/env swift

// Makes the DocC articles in Sources/FPPipe/FPPipe.docc from the sections of Readme.md,
// and the quick start on the DocC landing page. Readme.md is the source: edit it, then run
// this script from the package root.
//
//     swift Scripts/make-docc-articles.swift            write the articles
//     swift Scripts/make-docc-articles.swift --check    fail when an article is out of date
//
// Both fail when a line of code in Readme.md is longer than 72 characters, because DocC
// cuts off longer lines in a normal browser window.

import Foundation

// MARK: - Settings

let packageRoot = URL(fileURLWithPath: #filePath)
    .deletingLastPathComponent()
    .deletingLastPathComponent()
let readmeURL = packageRoot.appending(path: "Readme.md")
let catalogURL = packageRoot.appending(path: "Sources/FPPipe/FPPipe.docc")
let landingPageURL = catalogURL.appending(path: "FPPipe.md")
let maxCodeLineLength = 72

let generatedNote =
    "<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->"
let quickStartStart =
    "<!-- Quick start: made from Readme.md by Scripts/make-docc-articles.swift. -->"
let quickStartEnd = "<!-- End of quick start. -->"

struct Article {
    let file: String
    let title: String
    let abstract: String
    /// Readme sections, in order. A section with a heading level keeps its heading at that
    /// level; a section without one becomes the article's opening text.
    let sections: [(title: String, headingLevel: Int?)]
}

let articles = [
    Article(
        file: "GettingStarted",
        title: "Getting Started",
        abstract:
            "Add FPPipe to your package, build your first pipe, and learn how its parts work together.",
        sections: [("Installation", 2), ("Quick start", 2), ("How it works", 2)]
    ),
    Article(
        file: "SourcesAndSinks",
        title: "Sources and Sinks",
        abstract: "Put elements into a pipe, and read the results out of it.",
        sections: [("Sources", 2), ("Reading results", 2)]
    ),
    Article(
        file: "ChangingValues",
        title: "Changing Values",
        abstract: "Change, remove and watch the values that pass through a pipe.",
        sections: [("Changing values", nil)]
    ),
    Article(
        file: "HandlingFailures",
        title: "Handling Failures",
        abstract: "Turn values into failures, change errors, and recover from them.",
        sections: [("Handling failures", nil), ("Calling throwing code", 2)]
    ),
    Article(
        file: "PatternMatching",
        title: "Pattern Matching",
        abstract: "Act only on the elements that match a pattern, like the cases of a `switch`.",
        sections: [("Pattern matching", nil)]
    ),
    Article(
        file: "AsyncAndParallelWork",
        title: "Async and Parallel Work",
        abstract:
            "Run async closures, run several of them at the same time, and stop when the reader stops.",
        sections: [("Async stages", nil), ("Running work in parallel", 2)]
    ),
    Article(
        file: "TurningOneElementIntoMany",
        title: "Turning One Element into Many",
        abstract:
            "Turn each value into a sequence of values, and control how far the work runs ahead.",
        sections: [("Turning one element into many", nil)]
    ),
    Article(
        file: "OpenPipes",
        title: "Open Pipes",
        abstract: "Build a pipe once, and run it with different inputs.",
        sections: [("Open pipes", nil)]
    ),
    Article(
        file: "ConditionsInsideAPipe",
        title: "Conditions Inside a Pipe",
        abstract: "Choose stages with `if`, `else` and `switch`.",
        sections: [("Conditions inside a pipe", nil)]
    ),
    Article(
        file: "ACompleteExample",
        title: "A Complete Example",
        abstract: "A pipe that downloads, decodes, filters and recovers, all in one place.",
        sections: [("A complete example", nil)]
    ),
    Article(
        file: "StageReference",
        title: "Stage Reference",
        abstract: "Every stage, the closure it takes, and what it does.",
        sections: [("Stage reference", nil)]
    ),
    Article(
        file: "Design",
        title: "Design",
        abstract: "How a pipe is built, and why it is fast.",
        sections: [("Design", nil)]
    ),
]

/// Readme sections that no article uses. DocC makes its own contents, and the license is
/// on the package page.
let readmeOnlySections: Set = ["Contents", "License"]

/// Readme links that DocC can't follow, and what replaces them in the articles. A link to a
/// Readme section becomes a `<doc:>` link, which shows the article or section title as its
/// text, so the sentence around it may change too.
let linkEdits: [(section: String, old: String, new: String)] = [
    ("Reading results", "[How it works](#how-it-works)", "<doc:GettingStarted#How-it-works>"),
    (
        "Handling failures", "use [`MatchError`](#pattern-matching).",
        "use `MatchError` (see <doc:PatternMatching>)."
    ),
    (
        "Pattern matching", "(see [Running work in parallel](#running-work-in-parallel))",
        "(see <doc:AsyncAndParallelWork#Running-work-in-parallel>)"
    ),
    (
        "Open pipes", "[Running a pipe more than once](#running-a-pipe-more-than-once)",
        "<doc:SourcesAndSinks#Running-a-pipe-more-than-once>"
    ),
    (
        "Conditions inside a pipe", "- every [pattern-matching](#pattern-matching) stage",
        "- every pattern-matching stage (see <doc:PatternMatching>)"
    ),
    ("Stage reference", "match a [pattern](#pattern-matching).", "match a pattern."),
    (
        "Stage reference", "All `Async` stages except the tap stages take `concurrency:`.",
        "All `Async` stages except the tap stages take `concurrency:`. <doc:PatternMatching> explains patterns."
    ),
    (
        "Design", "[`Benchmarks/`](Benchmarks/README.md)",
        "[`Benchmarks/`](https://github.com/velocityzen/fp-swift-pipe/blob/main/Benchmarks/README.md)"
    ),
]

// MARK: - Helpers

func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data("error: \(message)\n".utf8))
    exit(1)
}

func read(_ url: URL) -> String {
    do {
        return try String(contentsOf: url, encoding: .utf8)
    } catch {
        fail("can't read \(url.path): \(error)")
    }
}

/// The lines of `text`, with `true` for each line inside a fenced code block, fences included.
func linesWithCodeFlag(_ text: String) -> [(line: String, isCode: Bool)] {
    var inFence = false
    return text.split(separator: "\n", omittingEmptySubsequences: false).map { line in
        let isFence = line.hasPrefix("```")
        if isFence { inFence.toggle() }
        return (String(line), inFence || isFence)
    }
}

/// The text of each `## ` section of `markdown`, without its heading and outer blank lines.
func sections(of markdown: String) -> [String: String] {
    var result: [String: String] = [:]
    var title: String?
    var body: [String] = []

    func finish() {
        guard let title else { return }
        let text = body.joined(separator: "\n").trimmingCharacters(in: .newlines)
        result[title] = text + "\n"
    }

    for (line, isCode) in linesWithCodeFlag(markdown) {
        if !isCode, line.hasPrefix("## ") {
            finish()
            title = String(line.dropFirst(3)).trimmingCharacters(in: .whitespaces)
            body = []
        } else {
            body.append(line)
        }
    }
    finish()
    return result
}

/// Readme code lines longer than `maxCodeLineLength`, as `Readme.md:<line>` descriptions.
func longCodeLines(in markdown: String) -> [String] {
    linesWithCodeFlag(markdown).enumerated().compactMap { index, entry in
        guard entry.isCode, entry.line.count > maxCodeLineLength else { return nil }
        return "Readme.md:\(index + 1) has \(entry.line.count) characters"
    }
}

/// Links in `text`, outside code, whose target is a Readme section or a relative path.
func linksDocCCantFollow(in text: String) -> [String] {
    let prose = linesWithCodeFlag(text).filter { !$0.isCode }.map(\.line).joined(separator: "\n")
    var links: [String] = []
    var rest = prose[...]
    while let open = rest.range(of: "](") {
        rest = rest[open.upperBound...]
        guard let close = rest.firstIndex(of: ")") else { break }
        let target = String(rest[..<close])
        if !target.hasPrefix("http://"), !target.hasPrefix("https://") {
            links.append(target)
        }
        rest = rest[close...]
    }
    return links
}

/// The quick start's code block and the paragraph after it.
func quickStart(from section: String) -> String {
    let blocks = section.components(separatedBy: "\n\n")
    guard let codeStart = blocks.firstIndex(where: { $0.hasPrefix("```swift") }),
        let codeEnd = blocks[codeStart...].firstIndex(where: { $0.hasSuffix("```") }),
        codeEnd + 1 < blocks.count
    else {
        fail("the Readme's \"Quick start\" must start with a code block and a paragraph")
    }
    return blocks[codeStart...codeEnd + 1].joined(separator: "\n\n") + "\n"
}

// MARK: - Making the files

let readme = read(readmeURL)

let longLines = longCodeLines(in: readme)
if !longLines.isEmpty {
    fail(
        "code lines longer than \(maxCodeLineLength) characters, which DocC cuts off:\n"
            + longLines.joined(separator: "\n")
    )
}

var readmeSections = sections(of: readme)

let usedSections = Set(articles.flatMap { $0.sections.map(\.title) })
let unusedSections = readmeSections.keys.filter {
    !usedSections.contains($0) && !readmeOnlySections.contains($0)
}
if !unusedSections.isEmpty {
    fail(
        "no article uses the Readme sections \(unusedSections.sorted().joined(separator: ", ")). "
            + "Add them to an article in `articles`, or to `readmeOnlySections`."
    )
}

for edit in linkEdits {
    guard let text = readmeSections[edit.section] else {
        fail("the Readme has no \"\(edit.section)\" section")
    }
    guard text.components(separatedBy: edit.old).count == 2 else {
        fail("\"\(edit.section)\" must contain \(edit.old) exactly once")
    }
    readmeSections[edit.section] = text.replacingOccurrences(of: edit.old, with: edit.new)
}

/// The text of every file this script makes, by file name in the catalog.
var expected: [String: String] = [:]

for article in articles {
    var parts: [String] = []
    for (title, headingLevel) in article.sections {
        guard let text = readmeSections[title] else {
            fail("the Readme has no \"\(title)\" section, which \(article.file).md needs")
        }
        let links = linksDocCCantFollow(in: text)
        if !links.isEmpty {
            fail(
                "\"\(title)\" links to \(links.joined(separator: ", ")). Add a DocC link to linkEdits."
            )
        }
        if let headingLevel {
            parts.append("\(String(repeating: "#", count: headingLevel)) \(title)\n\n\(text)")
        } else {
            parts.append(text)
        }
    }
    let body = parts.joined(separator: "\n")
    expected["\(article.file).md"] =
        "# \(article.title)\n\n\(article.abstract)\n\n\(body)\n\(generatedNote)\n"
}

let landingPage = read(landingPageURL)
guard let quickStartRange = landingPage.range(of: quickStartStart),
    let quickStartEndRange = landingPage.range(
        of: quickStartEnd,
        range: quickStartRange.upperBound..<landingPage.endIndex
    )
else {
    fail("FPPipe.md must contain \(quickStartStart) and \(quickStartEnd)")
}
guard let readmeQuickStart = readmeSections["Quick start"] else {
    fail("the Readme has no \"Quick start\" section")
}
expected["FPPipe.md"] =
    landingPage[..<quickStartRange.upperBound] + "\n\n"
    + quickStart(from: readmeQuickStart) + "\n"
    + landingPage[quickStartEndRange.lowerBound...]

for article in articles where !landingPage.contains("<doc:\(article.file)>") {
    fail("FPPipe.md doesn't list <doc:\(article.file)> under Topics")
}

// MARK: - Writing or checking

let checkOnly = CommandLine.arguments.dropFirst().contains("--check")
var changed: [String] = []
var removed: [String] = []

for (file, text) in expected.sorted(by: { $0.key < $1.key }) {
    let url = catalogURL.appending(path: file)
    let current = try? String(contentsOf: url, encoding: .utf8)
    guard current != text else { continue }
    changed.append(file)
    if !checkOnly {
        do {
            try text.write(to: url, atomically: true, encoding: .utf8)
        } catch {
            fail("can't write \(url.path): \(error)")
        }
    }
}

// Articles this script made earlier but no longer makes.
let catalogFiles = (try? FileManager.default.contentsOfDirectory(atPath: catalogURL.path)) ?? []
for file in catalogFiles.sorted() where file.hasSuffix(".md") && expected[file] == nil {
    let url = catalogURL.appending(path: file)
    guard read(url).contains(generatedNote) else { continue }
    removed.append(file)
    if !checkOnly {
        do {
            try FileManager.default.removeItem(at: url)
        } catch {
            fail("can't remove \(url.path): \(error)")
        }
    }
}

if changed.isEmpty, removed.isEmpty {
    print("The DocC articles match Readme.md.")
} else if checkOnly {
    var problems: [String] = []
    if !changed.isEmpty {
        problems.append(
            "these DocC files don't match Readme.md: \(changed.joined(separator: ", "))."
        )
    }
    if !removed.isEmpty {
        problems.append(
            "these DocC files are no longer made from Readme.md: \(removed.joined(separator: ", "))."
        )
    }
    fail((problems + ["Run `swift Scripts/make-docc-articles.swift`."]).joined(separator: "\n"))
} else {
    if !changed.isEmpty { print("Updated \(changed.joined(separator: ", ")).") }
    if !removed.isEmpty { print("Removed \(removed.joined(separator: ", ")).") }
}
