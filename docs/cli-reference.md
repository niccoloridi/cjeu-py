# cjeu-py CLI Reference

Complete reference for all `cjeu-py` CLI commands. For a quick-start guide and installation instructions, see the [README](../README.md).

All commands are invoked as subcommands of `cjeu-py`:

```
cjeu-py <command> [options]
```

---

## Table of Contents

- [Global Options](#global-options)
- [Data Collection](#data-collection)
  - [download-cellar](#download-cellar)
  - [download-cellar-meta](#download-cellar-meta)
  - [fetch-texts](#fetch-texts)
  - [download-taxonomy](#download-taxonomy)
- [Text Processing](#text-processing)
  - [parse-headers](#parse-headers)
  - [extract-citations](#extract-citations)
- [Data Integration](#data-integration)
  - [merge](#merge)
- [LLM Classification](#llm-classification)
  - [classify](#classify)
  - [validate](#validate)
- [Biographical Data](#biographical-data)
  - [scrape-judges](#scrape-judges)
  - [extract-judge-bios](#extract-judge-bios)
- [Data Exploration](#data-exploration)
  - [browse](#browse)
  - [search](#search)
- [Export](#export)
  - [export](#export-1)
  - [export-network](#export-network)
  - [enrich-network](#enrich-network)
  - [codebook](#codebook)
- [Analysis](#analysis)
  - [analyze](#analyze)

---

## Global Options

All commands that read or write pipeline data respect a shared data directory setting.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--data-dir` | `STR` | `~/.cjeu-py/data/` | Path to the pipeline data directory. Overrides the environment variable. |

You can also set the data directory via the `CJEU_DATA_DIR` environment variable:

```bash
export CJEU_DATA_DIR=/path/to/my/data
```

If both `--data-dir` and `CJEU_DATA_DIR` are set, the command-line flag takes precedence. If neither is set, data is stored in `~/.cjeu-py/data/`.

---

## Data Collection

Commands for downloading case-law metadata, full texts, and reference taxonomies from CELLAR and EUR-Lex.

### download-cellar

Fetch base metadata (decisions, citations, subject-matter classifications) from the CELLAR SPARQL endpoint. This is typically the first step in any pipeline run.

```bash
# Download all available Court of Justice judgments
cjeu-py download-cellar --court CJ --resource-type JUDG

# Download General Court decisions from 2020 onward
cjeu-py download-cellar --court TJ --date-from 2020-01-01

# Download only decisions, skipping citation and subject-matter queries
cjeu-py download-cellar --skip-citations --skip-subjects

# Download decisions involving a specific Advocate General, limited to 100
cjeu-py download-cellar --ag "Bobek" --max-items 100
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--max-items` | `INT` | all | Maximum number of items to fetch. |
| `--court` | `STR` | all courts | Court filter. One of `CJ` (Court of Justice), `TJ` (General Court), or `FJ` (Civil Service Tribunal). |
| `--resource-type` | `STR` | all types | Resource type filter. One of `JUDG` (judgment) or `ORDER`. |
| `--formation` | `STR` | all | Filter by court formation (e.g., `Grand Chamber`). |
| `--judge` | `STR` | all | Filter by judge rapporteur name. |
| `--ag` | `STR` | all | Filter by Advocate General name. |
| `--date-from` | `YYYY-MM-DD` | none | Include only decisions on or after this date. |
| `--date-to` | `YYYY-MM-DD` | none | Include only decisions on or before this date. |
| `--doc-types` | `STR` | `CJ,TJ,FJ` | Comma-separated CELEX document-type codes to include. |
| `--skip-citations` | flag | `false` | Skip the citation-pair query. |
| `--skip-subjects` | flag | `false` | Skip the subject-matter query. |
| `--force` | flag | `false` | Re-download even if data already exists locally. |

---

### download-cellar-meta

Fetch extended metadata from CELLAR, including procedural joins, appeal links, legislation references, and other relational data. Run this after `download-cellar` to enrich the base dataset.

```bash
# Fetch medium-detail metadata (default)
cjeu-py download-cellar-meta

# Fetch high-detail metadata for Court of Justice documents only
cjeu-py download-cellar-meta --detail high --doc-types CJ

# Fetch exhaustive metadata, forcing a refresh
cjeu-py download-cellar-meta --detail exhaustive --force

# Limit to 500 items
cjeu-py download-cellar-meta --max-items 500
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--max-items` | `INT` | all | Maximum number of items to fetch. |
| `--detail` | `STR` | `medium` | Level of metadata detail. One of `high`, `medium`, `all`, `exhaustive`, or `kitchen_sink`. |
| `--doc-types` | `STR` | all | Comma-separated CELEX document-type codes to include. |
| `--force` | flag | `false` | Re-download even if data already exists locally. |

---

### fetch-texts

Download full judgment texts (XHTML) from EUR-Lex. Texts are cached locally so subsequent runs skip already-downloaded documents.

```bash
# Download all texts in English (default)
cjeu-py fetch-texts

# Download texts in French and English with higher concurrency
cjeu-py fetch-texts --lang fra,eng --concurrency 40

# Download only the first 200 texts
cjeu-py fetch-texts --max-items 200
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--max-items` | `INT` | all | Maximum number of texts to download. |
| `--concurrency` | `INT` | `20` | Number of concurrent download requests. |
| `--lang` | `STR` | `eng` | Comma-separated ISO 639-2/B language codes (e.g., `eng`, `fra`, `deu`). |

---

### download-taxonomy

Download the CELLAR subject-matter taxonomy, which maps subject codes to human-readable labels. Useful for interpreting the subject-matter classifications attached to decisions.

```bash
# Download the taxonomy
cjeu-py download-taxonomy

# Force a fresh download
cjeu-py download-taxonomy --force
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--force` | flag | `false` | Re-download even if the taxonomy file already exists locally. |

---

## Text Processing

Commands for parsing downloaded texts and extracting structured information.

### parse-headers

Parse judgment XHTML headers to extract structured metadata such as case numbers, parties, formation, date, and procedural information.

```bash
# Parse all XHTML files in a directory
cjeu-py parse-headers ./data/xhtml/

# Parse with a custom output path
cjeu-py parse-headers ./data/xhtml/ --output ./data/headers.parquet

# Parse only the first 50 files (useful for testing)
cjeu-py parse-headers ./data/xhtml/ --limit 50
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `xhtml_dir` | positional, required | -- | Path to the directory containing XHTML judgment files. |
| `--output` | `STR` | auto | Output file path. |
| `--limit` | `INT` | `0` (all) | Maximum number of files to parse. Set to `0` to parse all files. |

---

### extract-citations

Extract case-law citations from downloaded judgment texts. Each citation is extracted with a surrounding context window for downstream classification.

```bash
# Extract citations with default context window (1 paragraph)
cjeu-py extract-citations

# Extract citations with a wider context window
cjeu-py extract-citations --window 3
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--window` | `INT` | `1` | Size of the context window (in paragraphs) captured around each citation. |

---

## Data Integration

### merge

Merge all data sources (base metadata, extended metadata, texts, citations, subjects) into enriched, analysis-ready datasets. This command takes no arguments and operates on the current data directory.

```bash
cjeu-py merge
```

This command has no additional arguments.

---

## LLM Classification

Commands for running LLM-based classification on extracted citations and exporting samples for human validation.

### classify

Run LLM classification on extracted citations. Supports multiple LLM providers and allows fine-grained control over concurrency, model selection, and API configuration.

```bash
# Classify using Gemini (default provider)
cjeu-py classify

# Classify using OpenAI with a specific model
cjeu-py classify --provider openai --model gpt-4o

# Classify with a custom API base (e.g., local or proxy endpoint)
cjeu-py classify --provider openai --api-base http://localhost:8000/v1 --api-key sk-...

# Classify only 100 citations with 10 concurrent workers
cjeu-py classify --max-items 100 --max-workers 10
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--max-items` | `INT` | all | Maximum number of citations to classify. |
| `--max-workers` | `INT` | `5` | Number of concurrent LLM requests. |
| `--provider` | `STR` | `gemini` | LLM provider. One of `gemini` or `openai`. |
| `--model` | `STR` | provider default | Model name to use (e.g., `gemini-2.5-flash`, `gpt-4o`). |
| `--api-base` | `STR` | provider default | Custom API base URL. |
| `--api-key` | `STR` | from env | API key. If not provided, read from the relevant environment variable (`GEMINI_API_KEY` or `OPENAI_API_KEY`). |

---

### validate

Export a random sample of classified citations for human validation. Produces a spreadsheet or file that a researcher can review to assess classification accuracy.

```bash
# Export a default sample of 200 citations
cjeu-py validate

# Export a larger sample to a specific file
cjeu-py validate --sample-size 500 --output ./validation/sample_500.csv
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--sample-size` | `INT` | `200` | Number of citations to include in the validation sample. |
| `--output` | `STR` | auto | Output file path for the validation sample. |

---

## Biographical Data

Commands for collecting and structuring judge and Advocate General biographical information.

### scrape-judges

Scrape judge and Advocate General biographical pages from the Curia website. Raw HTML pages are cached locally for subsequent extraction.

```bash
# Scrape all judge/AG bios
cjeu-py scrape-judges

# Scrape with a custom output and cache directory
cjeu-py scrape-judges --output ./data/judges_raw.json --cache-dir ./data/cache/curia/
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--output` | `STR` | auto | Output file path for the scraped data. |
| `--cache-dir` | `STR` | auto | Directory for caching downloaded HTML pages. |

---

### extract-judge-bios

Extract structured biographical information (nationality, education, career history, tenure) from scraped judge pages using an LLM.

```bash
# Extract bios from the default scraped input
cjeu-py extract-judge-bios

# Extract from a specific input file, limited to 50 entries
cjeu-py extract-judge-bios --input ./data/judges_raw.json --output ./data/judges_bios.json --max-items 50
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--input` | `STR` | auto | Path to the scraped judge data file (from `scrape-judges`). |
| `--output` | `STR` | auto | Output file path for the structured biographical data. |
| `--max-items` | `INT` | all | Maximum number of biographies to process. |

---

## Data Exploration

Commands for browsing and searching the collected data directly in the terminal.

### browse

Browse pipeline data tables interactively in the terminal. Supports viewing table summaries, column listings, row data, and individual judgment texts.

```bash
# List all available tables
cjeu-py browse

# Browse the decisions table (first 20 rows)
cjeu-py browse decisions

# Browse with stats and column info
cjeu-py browse decisions --stats --columns

# View a specific judgment text by CELEX number
cjeu-py browse text 62019CJ0344

# Output as JSON with a custom limit
cjeu-py browse citations --limit 50 --format json
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `table` | positional, optional | none | Table to browse (e.g., `decisions`, `citations`, `subjects`, `text`). If omitted, lists all available tables. |
| `celex` | positional, optional | none | CELEX number for the `text` browsing mode. |
| `--stats` | flag | `false` | Show summary statistics for the table. |
| `--columns` | flag | `false` | Show column names and types. |
| `--limit` | `INT` | `20` | Maximum number of rows to display. |
| `--format` | `STR` | `table` | Output format. One of `table`, `csv`, or `json`. |
| `--data-dir` | `STR` | global default | Path to the data directory. |

---

### search

Search collected case-law data across eight different modes. Each mode targets a different aspect of the dataset.

**Search modes:**

| Mode | Description |
|------|-------------|
| `text` | Full-text search across judgment bodies. |
| `headnote` | Search within headnotes / summary paragraphs. |
| `party` | Search by party name. |
| `citing` | Find all cases that cite a given case. |
| `cited-by` | Find all cases cited by a given case. |
| `topic` | Search by subject-matter topic. |
| `legislation` | Search by legislation reference. |
| `list` | List cases matching filter criteria (no query needed). |

```bash
# Full-text search for "proportionality"
cjeu-py search text "proportionality"

# Find all cases citing a specific CELEX number
cjeu-py search citing 62019CJ0344

# Find cases cited by a specific decision
cjeu-py search cited-by 62019CJ0344

# Search by party name, output as JSON
cjeu-py search party "Commission" --format json --limit 10

# Search by topic within a date range
cjeu-py search topic "competition" --date-from 2015-01-01 --date-to 2020-12-31

# List all General Court cases from 2023
cjeu-py search list --court TJ --date-from 2023-01-01 --date-to 2023-12-31

# Search legislation references with verbose output
cjeu-py search legislation "Regulation 1215/2012" --verbose
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `mode` | positional, required | -- | Search mode. One of `text`, `headnote`, `party`, `citing`, `cited-by`, `topic`, `legislation`, or `list`. |
| `query` | positional, optional | none | Search query string. Required for all modes except `list`. |
| `--limit` | `INT` | `25` | Maximum number of results to return. |
| `--format` | `STR` | `table` | Output format. One of `table`, `csv`, or `json`. |
| `--date-from` | `STR` | none | Filter results to decisions on or after this date. |
| `--date-to` | `STR` | none | Filter results to decisions on or before this date. |
| `--court` | `STR` | all courts | Filter by court (`CJ`, `TJ`, or `FJ`). |
| `--data-dir` | `STR` | global default | Path to the data directory. |
| `--verbose` | flag | `false` | Show additional detail in search results. |

---

## Export

Commands for exporting pipeline data into various formats for external analysis and visualization.

### export

Export pipeline data as CSV or Excel files for use in statistical software, spreadsheets, or other tools.

```bash
# Export all tables as CSV (default)
cjeu-py export

# Export as Excel workbook
cjeu-py export --format xlsx --output ./exports/cjeu_data.xlsx

# Export from a specific data directory
cjeu-py export --data-dir ./data/my_run/ --output ./exports/
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--data-dir` | `STR` | global default | Path to the data directory to export from. |
| `--output` | `STR` | auto | Output file or directory path. |
| `--format` | `STR` | `csv` | Export format. One of `csv` or `xlsx`. |

---

### export-network

Export the citation network in a graph format suitable for visualization or network analysis. Supports GEXF (for Gephi), D3.js JSON, and standalone interactive HTML.

```bash
# Export as GEXF for Gephi
cjeu-py export-network --format gexf --output ./network/citations.gexf

# Export a D3.js-compatible JSON file filtered by topic
cjeu-py export-network --format d3 --topic "competition" --output ./network/competition.json

# Export an interactive HTML visualization for Grand Chamber cases
cjeu-py export-network --format html --formation "Grand Chamber" --output ./network/grand_chamber.html

# Export internal citations only (exclude external references), limited to 500 nodes
cjeu-py export-network --internal-only --max-nodes 500

# Export Court of Justice cases from a specific period, including legislation nodes
cjeu-py export-network --court CJ --date-from 2010-01-01 --date-to 2020-12-31 --include-legislation
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--format` | `STR` | `gexf` | Output format. One of `gexf`, `d3`, or `html`. |
| `--output` | `STR` | auto | Output file path. |
| `--data-dir` | `STR` | global default | Path to the data directory. |
| `--topic` | `STR` | none | Filter the network to cases matching this subject-matter topic. |
| `--formation` | `STR` | none | Filter by court formation (e.g., `Grand Chamber`). |
| `--court` | `STR` | all courts | Filter by court (`CJ`, `TJ`, or `FJ`). |
| `--date-from` | `STR` | none | Include only cases on or after this date. |
| `--date-to` | `STR` | none | Include only cases on or before this date. |
| `--include-legislation` | flag | `false` | Include legislation nodes in the network graph. |
| `--max-nodes` | `INT` | all | Maximum number of nodes in the exported network. |
| `--internal-only` | flag | `false` | Only include citations between cases within the dataset (exclude external references). |

---

### enrich-network

Fetch metadata from CELLAR for external cited cases -- cases that appear as citation targets in your dataset but were not themselves downloaded. This fills in metadata gaps for network analysis and visualization.

```bash
# Enrich external case metadata
cjeu-py enrich-network

# Enrich from a specific data directory, forcing a refresh
cjeu-py enrich-network --data-dir ./data/my_run/ --force
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--data-dir` | `STR` | global default | Path to the data directory. |
| `--force` | flag | `false` | Re-fetch metadata even if it already exists. |

---

### codebook

Generate a variable codebook in Markdown format documenting all pipeline tables, their columns, data types, and descriptions. Useful for understanding the data schema or for inclusion in research documentation.

```bash
# Generate the codebook
cjeu-py codebook

# Generate to a specific file
cjeu-py codebook --output ./docs/CODEBOOK.md
```

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--output` | `STR` | auto | Output file path for the generated codebook. |

---

## Analysis

### analyze

Run analysis scripts on pipeline data. This command is a stub and is not yet implemented.

```bash
cjeu-py analyze
```

This command currently has no arguments and will print a placeholder message.
