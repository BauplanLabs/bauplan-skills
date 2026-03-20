# Bauplan Skills

Claude Code marketplace plugin for [Bauplan](https://www.bauplanlabs.com/) data lakehouse skills.

## Install

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
