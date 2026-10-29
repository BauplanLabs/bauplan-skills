
## [2.6.0] - 2026-10-29

### New Features

- Add GCS as supported object storage (1f3b701)

## [2.5.0] - 2026-10-22

### New Features

- Use duckdb 2.0.0 in skill examples (01779eb)

## [2.4.0] - 2026-10-19

### New Features

- Bump polars, duckdb and python in skill examples (dedd69e)

## [2.3.0] - 2026-10-12

### New Features

- Document the `name` parameter of `TableField` (03cd4bc)

### Bugfixes

- Remove WARN examples from pipeline expectations (de33f80)

## [2.2.0] - 2026-10-07

### New Features

- Add dag optimization skill (fdce0d8)

### Bugfixes

- Drop the `user` None guard in safe-ingestion (5f17d3c)

## [2.1.0] - 2026-09-18

### New Features

- Add skill for semantic documentation (53bd62c)
- Update skills for semantic docs (13f8ba5)

### Bugfixes

- Update skills with internet_access and type contract vs expectations, and docs link (7538d78)
- Replace uv run with uvx, remove warning mentions (4041757)
- Improvements based on feedback (70fbf2f)

## [2.0.0] - 2026-09-10

### New Features

- Include Codex and Cursor as alternatives to Claude Code (fff1e0c)
- GH action to constantly check link validity (5ec26c7)
- Add .editorconfig for styling (53a92f6)
- Update all skills to 0.3.0 and add migration skill (2e10dcc)
- Point documentation lookup to llms.txt markdown pages (24e6321)
- Add domain-specific doc page links to all skill Reference sections (54deb3b)
- Add CLAUDE.md and update README with setup instructions (e299b8e)

### Bugfixes

- Fix links pointing to docs which moved (9e0b318)
- Remove references to Claude (only minor) to ensure portability to cursor and codex (af3080e)
- Fix absence of .md (dc9f287)
- [skills] Update tutorial for import (dc08166)
- Update to reflect strict mode as default (32787ea)
- Add bauplan.Any (52435bb)

## [1.0.0] - 2026-03-20

### New Features

- Add Claude Code marketplace plugin with Bauplan lakehouse skills (86750ae)
