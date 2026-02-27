# cjeu-py Codebook

Variable definitions for all pipeline output tables.

## decisions

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier (primary key) |  |
| `ecli` | str | European Case Law Identifier |  |
| `date` | str | Decision date (ISO 8601) |  |
| `court_code` | str | Court | `CJ`, `TJ`, `FJ` |
| `resource_type` | str | Document type | `JUDG`, `ORDER`, `OPIN_AG`, `THIRDPARTY_OPIN_AG`, `OPIN_JUR` |
| `formation_code` | str | Court formation (CDM URI suffix) |  |
| `judge_rapporteur` | str | Judge-Rapporteur name |  |
| `advocate_general` | str | Advocate General name |  |
| `procedure_type` | str | Procedure type (CDM URI suffix) |  |
| `orig_country` | str | Country of origin (ISO 2-letter code) |  |
| `proc_lang` | str | Language of procedure (ISO 2-letter code) |  |
| `case_year` | str | Year the case was registered |  |
| `defendant_agent` | str | Defendant agent (corporate body code) |  |
| `applicant_agent` | str | Applicant agent (corporate body code) |  |
| `referring_court` | str | Referring national court (preliminary rulings) |  |
| `treaty_basis` | str | Treaty basis (e.g. TFEU_2008) |  |
| `date_lodged` | str | Date application was lodged |  |
| `procjur` | str | Procedural classification (e.g. REFER_PREL, APPEAL, ANNU_DIR) |  |
| `published_ecr` | str | Published in electronic Reports of Cases | `true`, `false` |
| `authentic_lang` | str | Authentic/original language (ISO 639-2/B code) |  |
| `eea_relevant` | str | EEA relevance flag | `true`, `false` |
| `natural_celex` | str | Natural case number (e.g. C-489/19) |  |

## citations_cellar

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `citing_celex` | str | CELEX of the citing decision |  |
| `cited_celex` | str | CELEX of the cited decision |  |

## subjects

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `subject_code` | str | Subject code (format depends on source) |  |
| `subject_label` | str | English label for the subject code |  |
| `subject_source` | str | Source taxonomy | `eurovoc`, `case_law_subject`, `case_law_directory`, `case_law_directory_old` |

Four taxonomies are collected:

- **eurovoc**: 260 broad thematic categories (flat). Codes are short alphanumeric strings (e.g. `SOPO` = Social Policy, `CONC` = Competition).
- **case_law_subject**: Older case-law subject matter classification.
- **case_law_directory**: Hierarchical directory of EU case law (fd_578, ~3,800 codes). Codes use dotted numbering with implicit hierarchy (e.g. `4.14` = Social Policy, `4.14.01` = Equality and non-discrimination). Older entries use a letter-numeric scheme (e.g. `B-09` = Competition).
- **case_law_directory_old**: Earlier directory (fd_577, ~27,000 entries). Same dotted/letter-numeric format as fd_578.

## joined_cases

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `joined_celex` | str | CELEX of the joined case |  |

## appeals

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier (lower court decision) |  |
| `appeal_celex` | str | CELEX of the appeal decision |  |

## annulled_acts

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `annulled_celex` | str | CELEX of the annulled act |  |

## interveners

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `agent_name` | str | Intervener name (from CDM agent URI) |  |

## ag_opinions

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX of the judgment |  |
| `ag_opinion_celex` | str | CELEX of the AG opinion |  |

## legislation_links

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `legislation_celex` | str | CELEX of the linked legislation |  |
| `link_type` | str | Type of case-to-legislation relationship | 17 categories |

Common link types: `interprets`, `confirms`, `requests_interpretation`, `requests_annulment`, `states_failure`, `amends`, `declares_valid`, `declares_void_preliminary`, `declares_incidentally_valid`. Rare types (fetched with `--detail all`): `suspends`, `corrects_judgment`, `incidentally_declares_void`, `interprets_judgment`, `partially_annuls`, `immediately_enforces`, `reviews_judgment`, `reexamined_by`.

## academic_citations

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `citation_text` | str | Bibliographic reference (author, title, journal, year) |  |

## referring_judgments

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `national_judgment` | str | Referring court name, decision type, date, and reference |  |

The following five tables are collected at `--detail exhaustive` or higher:

## dossiers

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `dossier_uri` | str | Dossier URI suffix (groups related proceedings) |  |

## summaries

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `summary_celex` | str | CELEX of the summary or information note (SUM/INF suffix) |  |

## misc_info

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `info_text` | str | Miscellaneous information (often appeal case numbers) |  |

## successors

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `successor_celex` | str | CELEX of the logical successor case |  |

## incorporates

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `incorporated_celex` | str | CELEX of the incorporated legislation |  |

The following table is collected at `--detail kitchen_sink` only:

## admin_metadata

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `property` | str | CDM property name | 18 categories |
| `value` | str | Property value (URIs shortened to suffix) |  |

Long-format table storing every remaining CDM property not captured by the other tables. The 18 property categories are:

- **country_role**: country of origin with ISO 3-letter code and role qualifier
- **created_by**: authoring institution (typically the Court itself)
- **seq_celex**: CELEX sequence number
- **collection**: collection document grouping
- **event**: event grouping
- **complex_work**: complex work membership
- **version**: work version number
- **embargo**: embargo date
- **date_creation**: work creation date
- **date_creation_legacy**: legacy creation date (work level)
- **datetime_transmission**: transmission timestamp
- **date_creation_legacy_2**: legacy creation date (resource level)
- **datetime_negotiation**: negotiation timestamp
- **obsolete_doc**: obsolete document identifier
- **obsolete_notice**: obsolete notice identifier
- **comment_internal**: internal comments
- **do_not_index**: indexing flag
- **document_id**: document URI identifier

## header_metadata

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier (from filename) |  |
| `doc_type` | str | Document type | `judgment`, `ag_opinion`, `order` |
| `date` | str | Decision date (YYYY-MM-DD) |  |
| `case_numbers` | list[str] | Case numbers (e.g. ['C-16/19']) |  |
| `formation` | str | Court formation (free text) |  |
| `parties` | dict | Parties: {applicants, defendants, interveners} |  |
| `composition` | list[dict] | Judges: [{name, role}, ...] |  |
| `advocate_general` | str | Advocate General name |  |
| `registrar` | str | Registrar name |  |
| `representatives` | list[dict] | Legal representatives per party |  |
| `hearing_date` | str | Hearing date (YYYY-MM-DD) |  |
| `ag_opinion_date` | str | AG opinion delivery date (YYYY-MM-DD) |  |

## assignments

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `judge_name` | str | Judge name (as appears in header) |  |
| `role` | str | Role in this case | `President`, `Vice-President`, `Presidents of Chambers`, `Rapporteur`, `Judges` |
| `is_rapporteur` | bool | Whether this judge is the Rapporteur | `True`, `False` |

## case_names

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `case_name` | str | Short case name (Applicant v Defendant) |  |
| `applicants` | str | Applicant names (semicolon-separated) |  |
| `defendants` | str | Defendant names (semicolon-separated) |  |

## operative_parts

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `celex` | str | CELEX identifier |  |
| `operative_part` | str | Full text of the operative part (dispositif) |  |

## citations_extracted

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `citing_celex` | str | CELEX of the citing document |  |
| `paragraph_num` | int | Paragraph number in the citing document |  |
| `citation_string` | str | Raw citation text as found in the document |  |
| `pattern_type` | str | Regex pattern that matched | 10 categories |
| `span_start` | int | Character offset (start) in paragraph |  |
| `span_end` | int | Character offset (end) in paragraph |  |
| `context_text` | str | Surrounding paragraph text for classification |  |

## classified_citations

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `citing_celex` | str | CELEX of the citing document |  |
| `paragraph_num` | int | Paragraph number |  |
| `citation_string` | str | Raw citation text |  |
| `precision` | str | Citation precision level | `string_citation`, `general_reference`, `substantive_engagement` |
| `use` | str | How the citation is used | 9 categories |
| `treatment` | str | How the cited case is treated | 8 categories |
| `topic` | str | Area of EU law (free text) |  |
| `confidence` | float | LLM confidence score (0.0 -- 1.0) |  |
| `reasoning` | str | LLM reasoning for the classification |  |

## judges_raw

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `name` | str | Full name as listed on Curia |  |
| `role` | str | Role at the Court (free text) |  |
| `bio_text` | str | Raw biographical text from Curia |  |
| `is_current` | bool | Whether currently serving | `True`, `False` |

## judges_structured

| Variable | Type | Description | Values |
|----------|------|-------------|--------|
| `name` | str | Full name |  |
| `role` | str | Role at the Court |  |
| `is_current` | bool | Whether currently serving | `True`, `False` |
| `birth_year` | int | Year of birth |  |
| `birth_place` | str | City of birth |  |
| `nationality` | str | Country (e.g. 'Italy', 'Germany') |  |
| `is_female` | bool | Gender | `True`, `False` |
| `education` | list[str] | Degrees and institutions |  |
| `prior_careers` | list[str] | Positions held before CJEU |  |
| `cjeu_roles` | list[dict] | CJEU roles: [{role, start_year, end_year}] |  |
| `death_year` | int | Year of death (if applicable) |  |
