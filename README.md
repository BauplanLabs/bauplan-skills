# Bauplan Skills

The recommended setup for developing on [Bauplan](https://www.bauplanlabs.com/) with AI coding assistants. This repo provides two things that work together:

1. **Skills plugin**: task-specific workflows (build pipelines, ingest data, debug failures, etc.) that AI assistants can follow autonomously.
2. **Context file**: project-level instructions (safety rules, CLI/SDK guidance, authentication) that ground every conversation in Bauplan best practices.

Install the skills for your AI assistant of choice, and copy or integrate the context file into your own repo for the baseline context.

## Table of Contents

- [Claude Code](#claude-code)
- [Codex](#codex)
- [Cursor](#cursor)
- [Skills](#skills)
- [License](#license)

## Claude Code

### Install the plugin

Install Bauplan Skills in Claude Code by running:

```sh
claude plugin marketplace add BauplanLabs/bauplan-skills
claude plugin install bauplan@bauplan-skills
```

To update to the latest version, run:

```sh
claude plugin update bauplan@bauplan-skills
```

### Use the CLAUDE.md

Copy `CLAUDE.md` from this repo into the root of your project, or merge its contents into your existing `CLAUDE.md`:

```sh
curl -o CLAUDE.md https://raw.githubusercontent.com/BauplanLabs/bauplan-skills/main/CLAUDE.md
```

This gives Claude Code the baseline context it needs (safety rules, CLI vs SDK guidance, authentication setup, and pointers to the skills) even before any skill is triggered.

## Codex

### Install the plugin

Install Bauplan Skills in Codex by running:

```sh
codex plugin marketplace add BauplanLabs/bauplan-skills
codex plugin add bauplan@bauplan-skills
```

To update to the latest version, run:

```sh
codex plugin marketplace upgrade bauplan-skills
```

### Use the AGENTS.md

Codex uses `AGENTS.md` as its project context file. Copy it into the root of your project:

```sh
curl -o AGENTS.md https://raw.githubusercontent.com/BauplanLabs/bauplan-skills/main/CLAUDE.md
```

## Cursor

### Install the plugin

Install Bauplan Skills in Cursor by following the instructions in this [video](https://youtu.be/TD7gME7JnZw?t=248), or by going to **Settings > Customize > Plugins > Add > From GitHub Repository**, entering `https://github.com/BauplanLabs/bauplan-skills`, and clicking **Add**.

To update to the latest version, click **Refresh**.

### Use the AGENTS.md

Cursor supports [granular rules](https://cursor.com/docs/rules), but `AGENTS.md` works too. Copy it into the root of your project:

```sh
curl -o AGENTS.md https://raw.githubusercontent.com/BauplanLabs/bauplan-skills/main/CLAUDE.md
```

## Skills

| Skill | Description |
|-------|-------------|
| `bauplan-explore-data` | Read-only exploration of lakehouse tables, schemas, and profiling |
| `bauplan-data-assessment` | Assess whether a business question can be answered with available data |
| `bauplan-data-pipeline` | Create data pipeline projects with SQL and Python models |
| `bauplan-safe-ingestion` | Ingest data from S3 with branch isolation and quality checks (WAP) |
| `bauplan-debug-and-fix-pipeline` | Diagnose and fix failed pipeline jobs |
| `bauplan-data-quality-checks` | Generate data quality check code for pipelines and ingestion |
| `bauplan-migrate-to-typed-sdk` | Migrate existing codebases to the typed SDK (0.3.0+) |

## License

MIT
