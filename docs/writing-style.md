# Writing style for docs and ADRs

Guidance for prose in this repo: ADRs, design docs, READMEs, code comments, 
docstrings (publishable code comments), and commit messages.
Inferred from the ADR-0004 revision. The goal is plain, factual, technical writing
that does not read as machine-generated.

## Voice

- Active voice, present tense, matter-of-fact. Say what a thing does, then stop.
- Terse. Cut adjectives, framing, and throat-clearing. One idea per sentence.
- No self-congratulation or drama. Avoid "the crux", "the heart of it", "largest
  change yet", "crucially", and similar.
- No corporate slang. Never "load-bearing", "earns its keep", "earns its place". Use
  "seam" sparingly, ideally not at all.
- Be sensitive to the reader. Don't burden them with unnecessary technical terms if more common words can be used without loss of precision.

## Constructions to avoid

- **No em dashes** - Use periods, commas, or a spaced hyphen (` - `) in labeled
  lists. Check with `grep -F '—'` before committing a doc.
- **No aside narration** - Do not tuck the real point behind a dash, colon, or
  parenthesis ("the workflow is notified: it may clean up"). Rewrite as a plain
  sentence. Swapping an em dash for a colon does not fix it; the construction is the
  problem.
- **No "X, not Y" contrast beats** - Delete the contrast and state the fact. When a
  comparison genuinely carries a design decision, front-load it as "Rather than B, A"
  and use even that sparingly.
- **No jarringly short sentences** left as an abrupt stop. Fold the point into an
  adjacent sentence.
- **No tricolons or rule-of-three** for rhetorical effect. A real list is fine.
- **No "X is the Y"** definitional flourishes, and no rhetorical italics on whole
  clauses.
- **Avoid semicolon lists** - Avoid creating long lists of information with semicolons. If there's a lot of information to present, find a way to do so in easily readable prose.

## Claims

- **No specious sentences** - The tell to watch for hardest (hilariously, this sentence is a tell for generated text that should be flagged). Every claim about how
  something works must be checkable against the actual API or design. If you cannot
  point to the mechanism, cut the claim. Before writing "X is expressible as Y",
  confirm Y exists.
- **Ground terms of art before naming them** - Describe the behavior in plain words,
  then attach the label where the meaning is already established. Prefer concrete
  phrasing over series shorthand ("when the activation ends", not "at the frontier").
- **Hedge what is unverified** - If a claim depends on behavior you have not
  confirmed (e.g. sdk-core internals), say so inline ("Confirm that ...") or in Open
  questions.
- **Cite the official SDKs for behavior, express it in idiomatic OCaml** - Match
  Temporal's semantics and cite the source SDKs, but name and shape the API the way
  an OCaml developer expects, not as a transliteration of another SDK (`spawn` and
  `await`, not `async`/`await`).

## Formatting conventions

- **Labeled points** use `**Term** - explanation`, a single-line lead-in.
- **Emphasis** uses single-word italics on a key term (`_request_`, `_not_`). Do not
  italicize whole clauses.
- **Source quotations** are italicized, `_"..."_`, with the source named in
  parentheses after.
- **Footnotes** (`[^name]`) carry a tangential clarification out of the main flow.
- **Proposed code and APIs** carry a note that the code in `main` is the source of
  truth, since an ADR is a proposal, not the implementation.
- **Consequences** list pros and cons with one consistent marker. Pick a single
  convention for the series: ADR-0002 and ADR-0003 use `**+**` / `**−**`; ADR-0004
  uses 👍 / 👎.
- Section and subsection headers use title case.

## Spelling and consistency

- Pick one spelling and hold it across the document: "cancellation" (not
  "cancelation"), "supersede" (not "supercede"). If multiple spelling alternatives can be used, default to English (US) dictionary.
- Keep a term's formatting consistent. Bold `**sdk-core**` everywhere or nowhere, not
  some of each.

## ADR structure (established by ADR-0001 through ADR-0004)

`# N. Title` / `- Status:` / `- Date:` / `## Context` (with a `### What the other
SDKs do` or `### Prior Art` subsection) / `## Decision` / `## Consequences` /
`## Alternatives considered` / `## Open questions` / `## References`. Pin wire field
tags against the vendored protos. Reference prior ADRs by number when extending or
revisiting them.
