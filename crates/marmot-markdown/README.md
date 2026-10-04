# marmot-markdown

CommonMark and Nostr-aware display parser for Marmot app messages. It turns plaintext message content into a typed,
serde-friendly AST that CLI, TUI, and mobile renderers draw from. It does not define wire format, persistence, or CGKA
engine behavior.

## What this crate does

- Parses CommonMark plus GFM-style tables and strikethrough, math, and a bounded `<details>` extension into `Block` /
  `Inline` nodes.
- Recognizes Nostr entities inline: bare `@npub1…` handles (`Inline::NostrMention`) and explicit `nostr:<hrp>1…`
  references (`Inline::NostrUri`), and classifies private-key destinations as sensitive.
- Does not parse general HTML. Tag-like sequences stay literal text; only autolinks and the `<details>` extension get
  structured treatment.
- Uses `serde` for the AST and the workspace `url` implementation for URL/IDNA normalization,
  and Unicode categories for hostname boundaries.

```rust
use marmot_markdown::{Block, parse};

let doc = parse("# Hi *there*");
assert!(matches!(doc.blocks.as_slice(), [Block::Heading { level: 1, .. }]));
```

## Scheme-less web domains

A bare domain such as `example.chat` produces an ordinary `Inline::Link` with
that display text and an HTTPS destination. Optional ports, paths, queries and
fragments are retained; URL normalization applies to the destination, including
IDNA conversion of international hostnames. No DNS queries or TLD allowlist are
used. Domain labels and hostnames follow DNS length/syntax bounds; the final
label must be alphabetic (at least two characters) or a valid IDN A-label.

Explicit URI and `www.` autolinks keep their existing representation. Existing
Markdown link/image labels, code and math are not reinterpreted. Bare emails,
credential-shaped tokens, explicit filesystem paths, numeric endings and malformed
hosts stay literal. A standalone filename such as `example.rs` is indistinguishable
from a valid domain. Prose joined to a hostname without punctuation or whitespace
can be equally ambiguous. Use separators or code formatting for literal text.
Escaped domains stay literal. In unmatched bracket prose, a path split by Markdown
formatting stays inert instead of becoming a link to a truncated destination;
recovery is skipped when escapes or decoded entities obscure the original spelling.
As with existing `www.` autolinks, a recognized raw URL tail retains its original
spelling (including ampersands and Markdown-looking path characters).
International labels are validated as a complete hostname, never shortened at a
script transition. Renderers retain their confirmation policy for IDN destinations.
Failed probes use bounded host scans and cached tail stops, so repeated candidates
cannot repeatedly scan a long rejected path.

## Renderer security contract

Message Markdown and every link destination are untrusted. The parser preserves explicit destinations and supplies normalized HTTPS
destinations for recognized bare domains, and annotates links, images, and autolinks with `LinkDestinationKind`. That classification is context
for client policy; it is not authorization to navigate or fetch.

Renderers **must inspect `classification` before making a destination actionable**. In particular:

- `Dangerous` (`javascript:`, `data:`, `vbscript:`, `file:`) and `Sensitive` (`nsec` / `ncryptsec`, including Nostr
  wrappers) should remain non-actionable by default.
- `Unknown` and `Relative` require an explicit client decision rather than inheriting a WebView or OS opener default.
- `Web`, `Contact`, `App`, and `Nostr` identify recognized destination families, but clients still apply their own
  navigation, deep-link, privacy, and network policies.
- Image renderers must apply the classification and their network policy before fetching `dest`; classification alone
  does not establish that a host or resolved address is safe.

The same classification is present on the MarmotKit/UniFFI Markdown types. Clients may still display the original
destination or link text when policy keeps it inert.

## What it does not do

- No CGKA engine, storage, transport, or runtime state.
- No UniFFI surface of its own (bindings expose parsed output through `marmot-app` / `marmot-uniffi` as needed).

## Bounded details/summary disclosures

The parser recognizes structural `<details>` / `<summary>` lines as `Block::Details`.
This is a block-oriented extension, not a general HTML parser:

- The opening `<details ...>` and closing `</details>` each occupy their own
  logical line after quote/list prefixes and at most three columns of local
  indent. Surrounding horizontal whitespace is allowed. Compact one-line HTML
  such as `<details><summary>x</summary>y</details>` stays literal text.
- An optional first nonblank `<summary ...>...</summary>` child is inline-parsed
  (formatting, entities, links, and Nostr mentions). Missing or empty summaries
  yield `summary: []`; clients may supply a localized fallback label. A later
  summary is ordinary body text. A malformed initial summary falls back to
  ordinary Markdown instead of dropping text.
- The `open` attribute means expanded even when written `open="false"`. Other
  attributes are ignored and never tokenized as destinations. Styles, scripts,
  event handlers, and URLs are not rendering instructions.
- Recognition uses original source before entity decoding. Escaped
  (`\<details>`, `&lt;details&gt;`), inline-code, fenced/indented code, and math
  forms stay literal. Failed or unclosed candidates keep their tags as ordinary
  Markdown and do not swallow following siblings.
- Recognition is limited to a 65536-byte original-source prefix and a 4096-byte
  structural tag cap (inclusive of `<` through `>`). Each details block consumes
  one existing container-depth slot. Recognition work is linear in that capped
  prefix: each continuation line is scanned once with carried code-span state,
  and unmatched backtick runs are never rescanned. An open summary code span
  keeps interior `</summary>` and `</details>` lines as content until a
  matching run closes it. Failed candidates restore ordinary block structure
  and source gaps instead of collapsing to one paragraph.
- Body blank-line counts align with `body` and saturate at
  `MAX_SOURCE_BLANK_LINES`. Delimiter-only lines are not blocks. Blanks before
  the closer stay inside the disclosure.

## Run the tests

```sh
cargo test -p marmot-markdown
```

Golden fixtures under `tests/golden/` lock parser output. After an intentional output change, regenerate them with
`MARMOT_MD_UPDATE_GOLDEN=1 cargo test -p marmot-markdown --test golden`, review the diff, and re-run without the variable.

See [`AGENTS.md`](AGENTS.md) for scope and invariants.
