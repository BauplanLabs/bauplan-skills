# Bauplan Skills

The recommended setup for developing on [Bauplan](https://www.bauplanlabs.com/) with AI coding assistants. This repo provides two things that work together:

1. **Skills plugin** — task-specific workflows (build pipelines, ingest data, debug failures, etc.) that Claude Code can follow autonomously.
2. **CLAUDE.md** — project-level instructions (safety rules, CLI/SDK guidance, authentication) that ground every conversation in Bauplan best practices.

Install the plugin for the skills, and copy or integrate the `CLAUDE.md` into your own repo for the baseline context. Both are part of the same workflow — AI-assisted development on Bauplan.

## Install the plugin

1. Add the marketplace:
```
/plugin marketplace add https://github.com/BauplanLabs/bauplan-skills
```

2. Open the plugin installer:
```
/plugin
```

3. Select **Browse and install plugins** → select **bauplan-skills** → press `Space` to select **bauplan** → press `i` to install.

4. Restart Claude Code.

## Use the CLAUDE.md

Copy `CLAUDE.md` from this repo into the root of your project, or merge its contents into your existing `CLAUDE.md`:

```bash
curl -o CLAUDE.md https://raw.githubusercontent.com/BauplanLabs/bauplan-skills/main/CLAUDE.md
```

This gives Claude Code the baseline context it needs — safety rules, CLI vs SDK guidance, authentication setup, and pointers to the skills — even before any skill is triggered.

## Skills

| Skill | Description |
|-------|-------------|
| `bauplan-explore-data` | Read-only exploration of lakehouse tables, schemas, and profiling |
| `bauplan-data-assessment` | Assess whether a business question can be answered with available data |
| `bauplan-data-pipeline` | Create data pipeline projects with SQL and Python models |
| `bauplan-safe-ingestion` | Ingest data from S3 with branch isolation and quality checks (WAP) |
| `bauplan-debug-and-fix-pipeline` | Diagnose and fix failed pipeline jobs |
| `bauplan-data-quality-checks` | Generate data quality check code for pipelines and ingestion |

## License

MIT
