# Bauplan Skills

The recommended setup for developing on [Bauplan](https://www.bauplanlabs.com/) with AI coding assistants. This repo provides two things that work together:

1. **Skills plugin** — task-specific workflows (build pipelines, ingest data, debug failures, etc.) that AI assistants can follow autonomously.
2. **Context file** — project-level instructions (safety rules, CLI/SDK guidance, authentication) that ground every conversation in Bauplan best practices.

Install the skills for your AI assistant of choice, and copy or integrate the context file into your own repo for the baseline context. Both are part of the same workflow — AI-assisted development on Bauplan.

## Table of Contents

- [Claude Code](#claude-code)
- [Codex](#codex)
- [Cursor](#cursor)
- [Skills](#skills)
- [License](#license)

---

## Claude Code

### Install the plugin

1. Add the marketplace:
```
/plugin marketplace add https://github.com/BauplanLabs/bauplan-skills
```

Restart Claude Code to make sure the changes are visible.

2. Open the plugin installer:
```
/plugin
```

3. Select **Browse and install plugins** → select **bauplan-skills** → press `Space` to select **bauplan** → press `i` to install.

4. Restart Claude Code.

### Use the CLAUDE.md

Copy `CLAUDE.md` from this repo into the root of your project, or merge its contents into your existing `CLAUDE.md`:

```bash
curl -o CLAUDE.md https://raw.githubusercontent.com/BauplanLabs/bauplan-skills/main/CLAUDE.md
```

This gives Claude Code the baseline context it needs — safety rules, CLI vs SDK guidance, authentication setup, and pointers to the skills — even before any skill is triggered.

---

## Codex

### Install skills

Inside Codex, run the skill installer pointing at the Bauplan skills directory:

```
$skill-installer https://github.com/BauplanLabs/bauplan-skills/tree/main/plugins/bauplan/skills
```

Codex will fetch and install the Bauplan skills automatically. Restart Codex once the installer completes.

To verify the installation, run `/skills` and select **List skills** — you should see the Bauplan skills listed.

### Use the AGENTS.md

Codex uses `AGENTS.md` as its project context file. Copy it into the root of your project:

```bash
curl -o AGENTS.md https://raw.githubusercontent.com/BauplanLabs/bauplan-skills/main/CLAUDE.md
```

---

## Cursor

### Install skills

Go to **Settings > Cursor Settings > Rules, Skills, Subagents**.

From there you have two options:

- **If you already use Claude Code** with Bauplan skills installed: enable the **Include third-party Plugins, Skills and Other Configs** toggle. Bauplan skills will appear automatically — no additional import needed.

- **If you only use Cursor**: in the Skills section, click **New** and prompt the agent to import Bauplan skills from:
  ```
  https://github.com/BauplanLabs/bauplan-skills/tree/main/plugins/bauplan/skills
  ```

### Use the AGENTS.md

Cursor supports [granular rules](https://cursor.com/docs/rules), but `AGENTS.md` works too. Copy it into the root of your project:

```bash
curl -o AGENTS.md https://raw.githubusercontent.com/BauplanLabs/bauplan-skills/main/CLAUDE.md
```

---

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
