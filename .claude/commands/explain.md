---
description: Explain how a class, endpoint, or business flow works
argument-hint: <class | endpoint | flow, e.g. "POST /orders" or "refund flow">
---
Explain: $ARGUMENTS

Use the `code-explorer` subagent, then give me:
- A 3-5 sentence summary in plain language.
- The flow as numbered steps with `path:line` references.
- A Mermaid sequence diagram if more than 3 components are involved.
- Transaction boundaries, side effects (DB writes, events, external calls), and failure modes.
- Anything surprising, risky, or undocumented.
Do not modify any files.
