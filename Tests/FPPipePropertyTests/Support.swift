// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

// MARK: - Multisets

func counts<T: Hashable>(_ items: [T]) -> [T: Int] {
    items.reduce(into: [:]) { $0[$1, default: 0] += 1 }
}

/// Whether every item of `part` is in `whole`, as many times or fewer.
func isSubmultiset<T: Hashable>(_ part: [T], of whole: [T]) -> Bool {
    let available = counts(whole)
    return counts(part).allSatisfy { item, count in count <= available[item, default: 0] }
}

// MARK: - Hangs

/// How long a pipe may go without progress, a source read or a side effect,
/// before its test case fails as stalled. Generous: long inputs through many
/// concurrent stages are slow on a loaded machine.
let stallLimit = Duration.seconds(10)

#endif
