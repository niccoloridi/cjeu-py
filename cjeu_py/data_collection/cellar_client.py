"""
CELLAR SPARQL client — primary data source for CJEU case law.

Queries the EU Publications Office SPARQL endpoint to extract:
- Decision metadata (CELEX, ECLI, dates, court, formation, procedure type,
  procedural classification, published-in-eReports flag, authentic language)
- Judge-Rapporteur and Advocate General names
- Parties (applicant/defendant agents), referring court, treaty basis
- Citation network (which cases cite which cases)
- Procedural links: joined cases, appeals, annulled acts, interveners
- Subject matter codes (EuroVoc + case-law subject + case-law directory fd_578/fd_577)
- AG opinion links (judgment → AG opinion CELEX)
- Legislation links (interprets, confirms, amends, annuls, etc.)
- Academic journal citations related to each case
- Referring national judgment details (preliminary rulings)
"""
import os
import time
import logging
import pandas as pd
from typing import List, Dict, Optional
from SPARQLWrapper import SPARQLWrapper, JSON, POST

from cjeu_py import config

logger = logging.getLogger(__name__)

CDM = "http://publications.europa.eu/ontology/cdm#"

# ── CELEX sector-6 document type codes ───────────────────────────────────
# Full inventory from EUR-Lex: https://eur-lex.europa.eu/content/tools/
#   TableOfSectors/types_of_documents_in_eurlex.html
#
# Keyed by 2-letter CELEX code → (description, court).
# "court" is one of CJ (Court of Justice), GC (General Court),
# CST (Civil Service Tribunal).

CELEX_DOC_TYPES = {
    # ── Court of Justice ──
    "CJ": ("Judgment", "CJ"),
    "CO": ("Order", "CJ"),
    "CC": ("AG Opinion", "CJ"),
    "CV": ("Opinion (avis)", "CJ"),
    "CP": ("View (prise de position)", "CJ"),
    "CD": ("Decision", "CJ"),
    "CX": ("Ruling", "CJ"),
    "CS": ("Seizure", "CJ"),
    "CT": ("Third party proceeding", "CJ"),
    "CN": ("Communication: new case", "CJ"),
    "CA": ("Communication: judgment", "CJ"),
    "CB": ("Communication: order", "CJ"),
    "CU": ("Communication: request for opinion", "CJ"),
    "CG": ("Communication: opinion", "CJ"),
    # ── General Court ──
    "TJ": ("Judgment", "GC"),
    "TO": ("Order", "GC"),
    "TC": ("AG Opinion", "GC"),
    "TT": ("Third party proceeding", "GC"),
    "TN": ("Communication: new case", "GC"),
    "TA": ("Communication: judgment", "GC"),
    "TB": ("Communication: order", "GC"),
    # ── Civil Service Tribunal ──
    "FJ": ("Judgment", "CST"),
    "FO": ("Order", "CST"),
    "FT": ("Third party proceeding", "CST"),
    "FN": ("Communication: new case", "CST"),
    "FA": ("Communication: judgment", "CST"),
    "FB": ("Communication: order", "CST"),
}

# Predefined groups for convenience
DOC_TYPE_JUDGMENTS = ["CJ", "TJ", "FJ"]
DOC_TYPE_ORDERS = ["CO", "TO", "FO"]
DOC_TYPE_AG_OPINIONS = ["CC", "TC"]
DOC_TYPE_OTHER_JUDICIAL = ["CV", "CP", "CD", "CX"]
DOC_TYPE_ALL_JUDICIAL = (
    DOC_TYPE_JUDGMENTS + DOC_TYPE_ORDERS + DOC_TYPE_AG_OPINIONS + DOC_TYPE_OTHER_JUDICIAL
)
DOC_TYPE_COMMUNICATIONS = [
    "CN", "CA", "CB", "CU", "CG",  # CJ
    "TN", "TA", "TB",              # GC
    "FN", "FA", "FB",              # CST
]
DOC_TYPE_PROCEDURAL = ["CS", "CT", "TT", "FT"]


class CellarClient:
    """Client for querying CJEU case law from the CELLAR SPARQL endpoint."""

    def __init__(self, endpoint: str = None, rate_limit: float = None):
        self.endpoint = endpoint or config.CELLAR_SPARQL_ENDPOINT
        self.rate_limit = rate_limit or config.CELLAR_RATE_LIMIT
        self.sparql = SPARQLWrapper(self.endpoint)
        self.sparql.setReturnFormat(JSON)
        self.sparql.setMethod(POST)
        self._last_request_time = 0

    @staticmethod
    def _celex_filter(doc_types: List[str] = None, var: str = "celex") -> str:
        """Build a SPARQL FILTER clause for CELEX document type codes.

        Args:
            doc_types: List of 2-letter CELEX codes (e.g. ["CJ", "TJ", "FJ"]).
                       None or empty → defaults to DOC_TYPE_JUDGMENTS.
            var: SPARQL variable name (without ?).

        Returns:
            A FILTER(REGEX(...)) string for embedding in SPARQL WHERE clauses.
        """
        codes = doc_types or DOC_TYPE_JUDGMENTS
        alternatives = "|".join(codes)
        return f'FILTER(REGEX(?{var}, "^6[0-9]{{4}}({alternatives})"))'

    def _throttle(self):
        """Respect rate limit between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def _query(self, sparql_str: str) -> List[Dict]:
        """Execute a SPARQL query and return bindings as list of dicts."""
        self._throttle()
        self.sparql.setQuery(sparql_str)
        try:
            results = self.sparql.query().convert()
            bindings = results.get("results", {}).get("bindings", [])
            return [
                {k: v["value"] for k, v in row.items()}
                for row in bindings
            ]
        except Exception as e:
            logger.error(f"SPARQL query failed: {e}")
            return []

    # ── Decision metadata ─────────────────────────────────────────────────

    def fetch_decisions(
        self,
        court: str = None,
        resource_type: str = None,
        formation: str = None,
        judge: str = None,
        advocate_general: str = None,
        date_from: str = None,
        date_to: str = None,
        max_items: int = None,
        offset: int = 0,
        doc_types: List[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch CJEU decision metadata from CELLAR.

        Args:
            court: Filter by court code — 'CJ', 'TJ', 'FJ' (or None for all)
            resource_type: Filter by type — 'JUDG', 'ORDER', 'OPIN_AG', etc.
            formation: Filter by court formation (substring match, e.g. 'GRAND_CH')
            judge: Filter by judge-rapporteur name (substring match)
            advocate_general: Filter by AG name (substring match)
            date_from: Earliest decision date (YYYY-MM-DD)
            date_to: Latest decision date (YYYY-MM-DD)
            max_items: Maximum number of decisions to fetch
            offset: Starting offset for pagination
            doc_types: CELEX document type codes to include (default: judgments
                       only — CJ/TJ/FJ). Use DOC_TYPE_ALL_JUDICIAL for everything
                       or pass a custom list like ["CJ", "CO", "CC"].

        Returns:
            DataFrame with columns: celex, ecli, date, court, resource_type,
            formation, judge_rapporteur, advocate_general, procedure_type,
            originating_country, procedure_language, case_year,
            defendant_agent, applicant_agent, referring_court,
            treaty_basis, date_lodged, procjur, published_ecr,
            authentic_lang, eea_relevant
        """
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        current_offset = offset

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            filters = []
            if court:
                filters.append(f'FILTER(?court_code = "{court}")')
            if resource_type:
                filters.append(f'FILTER(?resource_type = "{resource_type}")')
            if formation:
                filters.append(f'FILTER(CONTAINS(?formation_code, "{formation}"))')
            if judge:
                filters.append(f'FILTER(CONTAINS(LCASE(?judge_rapporteur), LCASE("{judge}")))')
            if advocate_general:
                filters.append(f'FILTER(CONTAINS(LCASE(?advocate_general), LCASE("{advocate_general}")))')
            if date_from:
                filters.append(f'FILTER(?date >= "{date_from}"^^xsd:date)')
            if date_to:
                filters.append(f'FILTER(?date <= "{date_to}"^^xsd:date)')
            
            filter_clause = "\n    ".join(filters)
            
            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?ecli ?date ?court_code ?resource_type
       ?formation_code ?judge_rapporteur ?advocate_general
       ?procedure_type ?orig_country ?proc_lang ?case_year
       ?defendant_agent ?applicant_agent ?referring_court
       ?treaty_basis ?date_lodged ?procjur ?published_ecr
       ?authentic_lang ?eea_relevant ?natural_celex
WHERE {{
    ?work cdm:resource_legal_id_celex ?celex .
    {self._celex_filter(doc_types)}
    ?work cdm:work_date_document ?date .
    OPTIONAL {{ ?work cdm:case-law_ecli ?ecli }}
    OPTIONAL {{ ?work cdm:resource_legal_type ?court_code }}
    OPTIONAL {{ ?work cdm:work_has_resource-type ?restype .
               BIND(REPLACE(STR(?restype), "^.*resource-type/", "") AS ?resource_type) }}
    OPTIONAL {{ ?work cdm:case-law_delivered_by_court-formation ?form .
               BIND(REPLACE(STR(?form), "^.*formjug/", "") AS ?formation_code) }}
    OPTIONAL {{ ?work cdm:case-law_delivered_by_judge ?judgeUri .
               ?judgeUri cdm:agent_name ?judge_rapporteur }}
    OPTIONAL {{ ?work cdm:case-law_delivered_by_advocate-general ?agUri .
               ?agUri cdm:agent_name ?advocate_general }}
    OPTIONAL {{ ?work cdm:case-law_has_type_procedure_concept_type_procedure ?procUri .
               BIND(REPLACE(STR(?procUri), "^.*fd_100/", "") AS ?procedure_type) }}
    OPTIONAL {{ ?work cdm:case-law_originates_in_country ?countryUri .
               BIND(REPLACE(STR(?countryUri), "^.*country/", "") AS ?orig_country) }}
    OPTIONAL {{ ?work cdm:case-law_uses_procedure_language ?langUri .
               BIND(REPLACE(STR(?langUri), "^.*language/", "") AS ?proc_lang) }}
    OPTIONAL {{ ?work cdm:resource_legal_year ?case_year }}
    OPTIONAL {{ ?work cdm:case-law_defended_by_agent ?defUri .
               BIND(REPLACE(STR(?defUri), "^.*corporate-body/", "") AS ?defendant_agent) }}
    OPTIONAL {{ ?work cdm:case-law_requested_by_agent ?reqUri .
               BIND(REPLACE(STR(?reqUri), "^.*corporate-body/", "") AS ?applicant_agent) }}
    OPTIONAL {{ ?work cdm:case-law_delivered_by_court_national ?natCourtUri .
               BIND(REPLACE(STR(?natCourtUri), "^.*/", "") AS ?referring_court) }}
    OPTIONAL {{ ?work cdm:resource_legal_based_on_concept_treaty ?treatyUri .
               BIND(REPLACE(STR(?treatyUri), "^.*treaty/", "") AS ?treaty_basis) }}
    OPTIONAL {{ ?work cdm:resource_legal_date_request_opinion ?date_lodged }}
    OPTIONAL {{ ?work cdm:case-law_has_procjur ?procjurUri .
               BIND(REPLACE(STR(?procjurUri), "^.*procjur/", "") AS ?procjur) }}
    OPTIONAL {{ ?work cdm:case-law_published_in_erecueil ?published_ecr }}
    OPTIONAL {{ ?work cdm:resource_legal_uses_originally_language ?authLangUri .
               BIND(REPLACE(STR(?authLangUri), "^.*language/", "") AS ?authentic_lang) }}
    OPTIONAL {{ ?work cdm:resource_legal_eea ?eea_relevant }}
    OPTIONAL {{ ?work cdm:resource_legal_number_natural_celex ?natural_celex }}
    {filter_clause}
}}
ORDER BY ?date
OFFSET {current_offset}
LIMIT {limit}
"""
            logger.info(f"Fetching decisions: offset={current_offset}, limit={limit}")
            rows = self._query(query)
            
            if not rows:
                break
            
            all_rows.extend(rows)
            current_offset += len(rows)
            
            logger.info(f"  → Got {len(rows)} rows (total: {len(all_rows)})")
            
            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break
        
        if not all_rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_rows)
        # Deduplicate (SPARQL may return duplicates due to OPTIONAL fan-out)
        if "celex" in df.columns:
            df = df.drop_duplicates(subset=["celex"], keep="first")
        
        logger.info(f"Fetched {len(df)} unique decisions from CELLAR")
        return df

    def fetch_cited_metadata(
        self,
        celex_list: List[str],
        batch_size: int = 500,
    ) -> pd.DataFrame:
        """Fetch basic metadata for external cited cases.

        Queries CELLAR for ECLI, date, court, formation, and resource type
        for a list of CELEX numbers. Used by enrich-network to fill in
        metadata for cases cited by the downloaded decision set.

        Args:
            celex_list: CELEX numbers to look up
            batch_size: Max CELEX values per SPARQL query (VALUES clause limit)

        Returns:
            DataFrame with columns: celex, ecli, date, court_code,
            resource_type, formation_code
        """
        all_rows = []

        for i in range(0, len(celex_list), batch_size):
            batch = celex_list[i:i + batch_size]
            values = " ".join(f'"{c}"^^xsd:string' for c in batch)

            query = f"""
PREFIX cdm: <{CDM}>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?celex ?ecli ?date ?court_code ?resource_type ?formation_code
WHERE {{
    VALUES ?celex {{ {values} }}
    ?work cdm:resource_legal_id_celex ?celex .
    OPTIONAL {{ ?work cdm:work_date_document ?date }}
    OPTIONAL {{ ?work cdm:case-law_ecli ?ecli }}
    OPTIONAL {{ ?work cdm:resource_legal_type ?court_code }}
    OPTIONAL {{ ?work cdm:work_has_resource-type ?restype .
               BIND(REPLACE(STR(?restype), "^.*resource-type/", "") AS ?resource_type) }}
    OPTIONAL {{ ?work cdm:case-law_delivered_by_court-formation ?form .
               BIND(REPLACE(STR(?form), "^.*formjug/", "") AS ?formation_code) }}
}}
"""
            logger.info(f"Fetching cited metadata: batch {i // batch_size + 1} "
                        f"({len(batch)} cases)")
            rows = self._query(query)
            all_rows.extend(rows)

            if rows:
                logger.info(f"  → Got {len(rows)} rows")

        if not all_rows:
            return pd.DataFrame(columns=["celex", "ecli", "date",
                                          "court_code", "resource_type",
                                          "formation_code"])

        df = pd.DataFrame(all_rows)
        if "celex" in df.columns:
            df = df.drop_duplicates(subset=["celex"], keep="first")

        logger.info(f"Fetched metadata for {len(df)} cited cases")
        return df

    def save_cited_metadata(self, df: pd.DataFrame, output_dir: str = None):
        """Save cited metadata to Parquet."""
        output_dir = output_dir or config.RAW_CELLAR_DIR
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, "cited_metadata.parquet")
        df.to_parquet(path, index=False)
        logger.info(f"Saved metadata for {len(df)} cited cases to {path}")
        return path

    # ── Case names ─────────────────────────────────────────────────────────

    def fetch_case_names(
        self,
        celex_list: List[str] = None,
        max_items: int = None,
        doc_types: List[str] = None,
    ) -> pd.DataFrame:
        """Fetch case names from expression-level metadata.

        Combines two mutually exclusive CELLAR fields:
        - expression_title_alternative: short popular name (older cases, ~1954-2015)
        - expression_case-law_parties: full "X v Y" (newer cases, ~1988-2025)

        Returns:
            DataFrame with columns: celex, case_name, case_id
        """
        celex_f = self._celex_filter(doc_types)
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        offset = 0

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            celex_filter = ""
            if celex_list:
                values = " ".join(f'"{c}"' for c in celex_list)
                celex_filter = f"VALUES ?celex {{ {values} }}"

            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?parties ?titleAlt ?caseId
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    {celex_f}
    ?expr cdm:expression_belongs_to_work ?work .
    ?expr cdm:expression_uses_language
          <http://publications.europa.eu/resource/authority/language/ENG> .
    OPTIONAL {{ ?expr cdm:expression_case-law_parties ?parties . }}
    OPTIONAL {{ ?expr cdm:expression_title_alternative ?titleAlt . }}
    OPTIONAL {{ ?expr cdm:expression_case-law_identifier_case ?caseId . }}
}}
OFFSET {offset}
LIMIT {limit}
"""
            logger.info(f"Fetching case names: offset={offset}, limit={limit}")
            rows = self._query(query)

            if not rows:
                break

            all_rows.extend(rows)
            offset += len(rows)

            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["celex", "parties", "titleAlt", "caseId"])

        # Build unified case_name: prefer titleAlt (short), fall back to parties
        if not df.empty:
            df["case_name"] = df["titleAlt"].fillna(df["parties"])
            df["case_id"] = df.get("caseId", pd.Series(dtype=str))
            df = df[["celex", "case_name", "case_id"]].drop_duplicates(
                subset=["celex"], keep="first")

        logger.info(f"Fetched case names for {len(df)} cases")
        return df

    def save_case_names(self, df: pd.DataFrame, output_dir: str = None):
        """Save case names to Parquet."""
        output_dir = output_dir or config.RAW_CELLAR_DIR
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, "case_names.parquet")
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} case names to {path}")
        return path

    # ── Citations ─────────────────────────────────────────────────────────

    def fetch_citations(
        self,
        celex_list: List[str] = None,
        max_items: int = None,
        doc_types: List[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch the citation network: which cases cite which other cases.

        Args:
            celex_list: Optional list of citing CELEX numbers to filter
            max_items: Maximum citation pairs to fetch
            doc_types: CELEX document type codes (default: judgments only)

        Returns:
            DataFrame with columns: citing_celex, cited_celex
        """
        celex_f = self._celex_filter(doc_types, var="citing_celex")
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        offset = 0

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            celex_filter = ""
            if celex_list:
                values = " ".join(f'"{c}"' for c in celex_list)
                celex_filter = f"VALUES ?citing_celex {{ {values} }}"

            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?citing_celex ?cited_celex
WHERE {{
    {celex_filter}
    ?citing_work cdm:resource_legal_id_celex ?citing_celex .
    {celex_f}
    ?citing_work cdm:work_cites_work ?cited_work .
    ?cited_work cdm:resource_legal_id_celex ?cited_celex .
}}
OFFSET {offset}
LIMIT {limit}
"""
            logger.info(f"Fetching citations: offset={offset}, limit={limit}")
            rows = self._query(query)
            
            if not rows:
                break
            
            all_rows.extend(rows)
            offset += len(rows)
            
            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break
        
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=["citing_celex", "cited_celex"])
        logger.info(f"Fetched {len(df)} citation pairs from CELLAR")
        return df

    # ── Subject matter ────────────────────────────────────────────────────

    def fetch_subject_matter(
        self,
        celex_list: List[str] = None,
        max_items: int = None,
    ) -> pd.DataFrame:
        """
        Fetch subject-matter codes for decisions from four CELLAR taxonomies.

        Sources:
            - eurovoc: 260 broad thematic categories (flat)
            - case_law_subject: older case-law subject matter classification
            - case_law_directory: hierarchical directory (fd_578, ~3,800 codes
              with dotted numbering, e.g. 4.14.01.01)
            - case_law_directory_old: older directory (fd_577, ~27,000 entries)

        Returns:
            DataFrame with columns: celex, subject_code, subject_label, subject_source
        """
        all_rows = []

        celex_filter = ""
        if celex_list:
            values = " ".join(f'"{c}"' for c in celex_list)
            celex_filter = f"VALUES ?celex {{ {values} }}"

        sources = [
            ("eurovoc",
             "resource_legal_is_about_subject-matter",
             "subject-matter/"),
            ("case_law_subject",
             "case-law_is-about_case-law-subject-matter",
             "case-law-subject-matter/"),
            ("case_law_directory",
             "case-law_is_about_concept_new_case-law",
             "fd_578/"),
            ("case_law_directory_old",
             "case-law_is_about_concept_case-law",
             "fd_577/"),
        ]

        for source_name, cdm_prop, uri_suffix in sources:
            offset = 0
            batch_size = config.SPARQL_BATCH_SIZE

            while True:
                limit = (min(batch_size, max_items - len(all_rows))
                         if max_items else batch_size)

                query = f"""
PREFIX cdm: <{CDM}>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?celex ?subject_code ?subject_label
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    ?work cdm:resource_legal_id_sector "6" .
    ?work cdm:{cdm_prop} ?subj .
    BIND(REPLACE(STR(?subj), "^.*{uri_suffix}", "") AS ?subject_code)
    OPTIONAL {{ ?subj skos:prefLabel ?subject_label .
                FILTER(LANG(?subject_label) = "en") }}
}}
OFFSET {offset}
LIMIT {limit}
"""
                logger.info(f"Fetching {source_name} subjects: offset={offset}")
                rows = self._query(query)
                if not rows:
                    break
                for r in rows:
                    r["subject_source"] = source_name
                all_rows.extend(rows)
                offset += len(rows)
                logger.info(f"  → Got {len(rows)} rows (total {source_name}: {offset})")

                if max_items and len(all_rows) >= max_items:
                    break
                if len(rows) < limit:
                    break

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["celex", "subject_code", "subject_label", "subject_source"]
        )
        logger.info(f"Fetched {len(df)} subject entries from CELLAR "
                     f"({df['subject_source'].value_counts().to_dict() if len(df) else {}})")
        return df

    # ── Procedural links (multi-valued, separate queries) ────────────────

    def _fetch_pairs(
        self,
        cdm_property: str,
        target_col: str,
        celex_list: List[str] = None,
        max_items: int = None,
        extract_celex: bool = False,
        doc_types: List[str] = None,
    ) -> pd.DataFrame:
        """Generic fetcher for 1-to-many CELLAR relationships.

        Args:
            cdm_property: CDM predicate (e.g. 'case-law_joins_case_court')
            target_col: Name for the target column in output
            celex_list: Optional filter on citing CELEX
            max_items: Max rows
            extract_celex: If True, resolve target URI to its CELEX number
            doc_types: CELEX document type codes (default: judgments only)
        """
        celex_f = self._celex_filter(doc_types)
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        offset = 0

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            celex_filter = ""
            if celex_list:
                values = " ".join(f'"{c}"' for c in celex_list)
                celex_filter = f"VALUES ?celex {{ {values} }}"

            if extract_celex:
                select = f"?celex ?{target_col}"
                body = f"""
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    {celex_f}
    ?work cdm:{cdm_property} ?targetWork .
    ?targetWork cdm:resource_legal_id_celex ?{target_col} ."""
            else:
                select = f"?celex ?{target_col}"
                body = f"""
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    {celex_f}
    ?work cdm:{cdm_property} ?targetUri .
    BIND(REPLACE(STR(?targetUri), "^.*/", "") AS ?{target_col})"""

            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT {select}
WHERE {{{body}
}}
OFFSET {offset}
LIMIT {limit}
"""
            rows = self._query(query)
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break

        cols = ["celex", target_col]
        return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=cols)

    def fetch_joined_cases(self, celex_list: List[str] = None, max_items: int = None,
                           doc_types: List[str] = None) -> pd.DataFrame:
        """Fetch joined-case links. Returns celex → joined_celex pairs."""
        df = self._fetch_pairs("case-law_joins_case_court", "joined_celex",
                               celex_list, max_items, doc_types=doc_types)
        logger.info(f"Fetched {len(df)} joined-case links from CELLAR")
        return df

    def fetch_appeals(self, celex_list: List[str] = None, max_items: int = None,
                      doc_types: List[str] = None) -> pd.DataFrame:
        """Fetch appeal links (case → appeal case). Returns celex → appeal_celex pairs."""
        df = self._fetch_pairs("case-law_subject_to_appeal_in_case_court", "appeal_celex",
                               celex_list, max_items, doc_types=doc_types)
        logger.info(f"Fetched {len(df)} appeal links from CELLAR")
        return df

    def fetch_annulled_acts(self, celex_list: List[str] = None, max_items: int = None,
                            doc_types: List[str] = None) -> pd.DataFrame:
        """Fetch acts declared void by a decision. Returns celex → annulled_celex pairs."""
        df = self._fetch_pairs("case-law_declares_void_resource_legal", "annulled_celex",
                               celex_list, max_items, extract_celex=True, doc_types=doc_types)
        logger.info(f"Fetched {len(df)} annulled-act links from CELLAR")
        return df

    def fetch_interveners(self, celex_list: List[str] = None, max_items: int = None,
                          doc_types: List[str] = None) -> pd.DataFrame:
        """Fetch intervener/observer agent names. Returns celex → agent_name pairs."""
        celex_f = self._celex_filter(doc_types)
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        offset = 0

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            celex_filter = ""
            if celex_list:
                values = " ".join(f'"{c}"' for c in celex_list)
                celex_filter = f"VALUES ?celex {{ {values} }}"

            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?agent_name
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    {celex_f}
    ?work cdm:case-law_commented_by_agent ?agentUri .
    ?agentUri cdm:agent_name ?agent_name .
}}
OFFSET {offset}
LIMIT {limit}
"""
            rows = self._query(query)
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=["celex", "agent_name"])
        logger.info(f"Fetched {len(df)} intervener entries from CELLAR")
        return df

    # ── AG opinion links ───────────────────────────────────────────────────

    def fetch_ag_opinions(self, celex_list: List[str] = None, max_items: int = None,
                          doc_types: List[str] = None) -> pd.DataFrame:
        """Fetch judgment → AG opinion links. Returns celex → ag_opinion_celex pairs."""
        celex_f = self._celex_filter(doc_types)
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        offset = 0

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            celex_filter = ""
            if celex_list:
                values = " ".join(f'"{c}"' for c in celex_list)
                celex_filter = f"VALUES ?celex {{ {values} }}"

            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?ag_opinion_celex
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    {celex_f}
    ?work cdm:case-law_has_conclusions_opinion_advocate-general ?agWork .
    ?agWork cdm:resource_legal_id_celex ?ag_opinion_celex .
}}
OFFSET {offset}
LIMIT {limit}
"""
            rows = self._query(query)
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["celex", "ag_opinion_celex"])
        logger.info(f"Fetched {len(df)} AG opinion links from CELLAR")
        return df

    # ── Legislation links ──────────────────────────────────────────────────

    # CDM properties mapping case-law to legislation, grouped by tier
    LEGISLATION_LINK_TYPES_HIGH = [
        ("interprets", "case-law_interpretes_resource_legal"),
        ("confirms", "case-law_confirms_resource_legal"),
        ("requests_interpretation", "case-law_requests_interpretation_resource_legal"),
        ("requests_annulment", "case-law_requests_annulment_of_resource_legal"),
        ("states_failure", "case-law_states_failure_concerning_resource_legal"),
        ("amends", "case-law_amends_resource_legal"),
        ("declares_valid", "case-law_declares_valid_resource_legal"),
        ("declares_void_preliminary", "case-law_declares_void_by_preliminary_ruling_resource_legal"),
        ("declares_incidentally_valid", "case-law_declares_incidentally_valid_resource_legal"),
    ]
    LEGISLATION_LINK_TYPES_LOW = [
        ("suspends", "case-law_suspends_application_of_resource_legal"),
        ("corrects_judgment", "case-law_corrects_judgement_resource_legal"),
        ("incidentally_declares_void", "case-law_incidentally_declares_void_resource_legal"),
        ("interprets_judgment", "case-law_interpretes_judgement_resource_legal"),
        ("partially_annuls", "case-law_partially_annuls_resource_legal"),
        ("immediately_enforces", "case-law_immediately_enforces_resource_legal"),
        ("reviews_judgment", "case-law_reviews_judgement_resource_legal"),
        ("reexamined_by", "case-law_reexamined_by_case_court"),
    ]

    def fetch_legislation_links(
        self,
        celex_list: List[str] = None,
        max_items: int = None,
        include_low: bool = False,
        doc_types: List[str] = None,
    ) -> pd.DataFrame:
        """Fetch case-to-legislation links (interprets, confirms, amends, etc.).

        Returns DataFrame with columns: celex, legislation_celex, link_type.
        Set include_low=True to also fetch rare link types (<100 entries each).
        """
        celex_f = self._celex_filter(doc_types)
        link_types = list(self.LEGISLATION_LINK_TYPES_HIGH)
        if include_low:
            link_types.extend(self.LEGISLATION_LINK_TYPES_LOW)

        all_rows = []
        for link_type, cdm_prop in link_types:
            offset = 0
            batch_size = config.SPARQL_BATCH_SIZE

            while True:
                limit = (min(batch_size, max_items - len(all_rows))
                         if max_items else batch_size)

                celex_filter = ""
                if celex_list:
                    values = " ".join(f'"{c}"' for c in celex_list)
                    celex_filter = f"VALUES ?celex {{ {values} }}"

                query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?legislation_celex
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    {celex_f}
    ?work cdm:{cdm_prop} ?legWork .
    ?legWork cdm:resource_legal_id_celex ?legislation_celex .
}}
OFFSET {offset}
LIMIT {limit}
"""
                rows = self._query(query)
                if not rows:
                    break
                for r in rows:
                    r["link_type"] = link_type
                all_rows.extend(rows)
                offset += len(rows)
                if max_items and len(all_rows) >= max_items:
                    break
                if len(rows) < limit:
                    break

            if all_rows:
                count = sum(1 for r in all_rows if r["link_type"] == link_type)
                if count:
                    logger.info(f"  {link_type}: {count} links")

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["celex", "legislation_celex", "link_type"])
        logger.info(f"Fetched {len(df)} legislation links from CELLAR")
        return df

    # ── Academic citations ─────────────────────────────────────────────────

    def fetch_academic_citations(
        self,
        celex_list: List[str] = None,
        max_items: int = None,
    ) -> pd.DataFrame:
        """Fetch academic journal citations related to each case.

        Returns DataFrame with columns: celex, citation_text.
        """
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        offset = 0

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            celex_filter = ""
            if celex_list:
                values = " ".join(f'"{c}"' for c in celex_list)
                celex_filter = f"VALUES ?celex {{ {values} }}"

            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?citation_text
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    ?work cdm:resource_legal_id_sector "6" .
    ?work cdm:case-law_article_journal_related ?citation_text .
}}
OFFSET {offset}
LIMIT {limit}
"""
            rows = self._query(query)
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["celex", "citation_text"])
        logger.info(f"Fetched {len(df)} academic citation entries from CELLAR")
        return df

    # ── Referring national judgments ───────────────────────────────────────

    def fetch_referring_judgments(
        self,
        celex_list: List[str] = None,
        max_items: int = None,
    ) -> pd.DataFrame:
        """Fetch referring national judgment details (for preliminary rulings).

        Returns DataFrame with columns: celex, national_judgment.
        The national_judgment field contains structured text with court name,
        decision type, date, and reference number.
        """
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        offset = 0

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            celex_filter = ""
            if celex_list:
                values = " ".join(f'"{c}"' for c in celex_list)
                celex_filter = f"VALUES ?celex {{ {values} }}"

            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?national_judgment
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    ?work cdm:resource_legal_id_sector "6" .
    ?work cdm:case-law_national-judgement ?national_judgment .
}}
OFFSET {offset}
LIMIT {limit}
"""
            rows = self._query(query)
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["celex", "national_judgment"])
        logger.info(f"Fetched {len(df)} referring judgment entries from CELLAR")
        return df

    # ── Exhaustive-tier metadata ─────────────────────────────────────────

    def fetch_dossiers(self, celex_list: List[str] = None, max_items: int = None,
                       doc_types: List[str] = None) -> pd.DataFrame:
        """Fetch dossier groupings for cases.

        Returns DataFrame with columns: celex, dossier_uri.
        """
        return self._fetch_pairs(
            "work_part_of_dossier", "dossier_uri",
            celex_list, max_items, doc_types=doc_types)

    def fetch_summaries(
        self,
        celex_list: List[str] = None,
        max_items: int = None,
    ) -> pd.DataFrame:
        """Fetch links to case summaries and information notes.

        Returns DataFrame with columns: celex, summary_celex.
        """
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        offset = 0

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            celex_filter = ""
            if celex_list:
                values = " ".join(f'"{c}"' for c in celex_list)
                celex_filter = f"VALUES ?celex {{ {values} }}"

            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?summary_celex
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    ?work cdm:resource_legal_id_sector "6" .
    ?work cdm:summary_case-law_id_celex ?summary_celex .
}}
OFFSET {offset}
LIMIT {limit}
"""
            rows = self._query(query)
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["celex", "summary_celex"])
        logger.info(f"Fetched {len(df)} summary links from CELLAR")
        return df

    def fetch_misc_info(
        self,
        celex_list: List[str] = None,
        max_items: int = None,
    ) -> pd.DataFrame:
        """Fetch miscellaneous information fields (often appeal case numbers).

        Returns DataFrame with columns: celex, info_text.
        """
        all_rows = []
        batch_size = config.SPARQL_BATCH_SIZE
        offset = 0

        while True:
            limit = min(batch_size, max_items - len(all_rows)) if max_items else batch_size

            celex_filter = ""
            if celex_list:
                values = " ".join(f'"{c}"' for c in celex_list)
                celex_filter = f"VALUES ?celex {{ {values} }}"

            query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?info_text
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    ?work cdm:resource_legal_id_sector "6" .
    ?work cdm:resource_legal_information_miscellaneous ?info_text .
}}
OFFSET {offset}
LIMIT {limit}
"""
            rows = self._query(query)
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if max_items and len(all_rows) >= max_items:
                break
            if len(rows) < limit:
                break

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["celex", "info_text"])
        logger.info(f"Fetched {len(df)} misc info entries from CELLAR")
        return df

    def fetch_successors(self, celex_list: List[str] = None, max_items: int = None,
                         doc_types: List[str] = None) -> pd.DataFrame:
        """Fetch case succession chains (logical successor relationships).

        Returns DataFrame with columns: celex, successor_celex.
        """
        return self._fetch_pairs(
            "work_is_logical_successor_of_work", "successor_celex",
            celex_list, max_items, extract_celex=True, doc_types=doc_types)

    def fetch_incorporates(self, celex_list: List[str] = None, max_items: int = None,
                           doc_types: List[str] = None) -> pd.DataFrame:
        """Fetch legislative incorporation links.

        Returns DataFrame with columns: celex, incorporated_celex.
        """
        return self._fetch_pairs(
            "resource_legal_incorporates_resource_legal", "incorporated_celex",
            celex_list, max_items, extract_celex=True, doc_types=doc_types)

    # ── Kitchen-sink tier: all remaining CDM properties ──────────────────

    ADMIN_PROPERTIES = [
        ("country_role", "case-law_originates_in_country_role-qualifier"),
        ("created_by", "work_created_by_agent"),
        ("seq_celex", "resource_legal_number_sequence_celex"),
        ("collection", "work_part_of_collection_document"),
        ("event", "work_part_of_event"),
        ("complex_work", "work_is_member_of_complex_work"),
        ("version", "work_version"),
        ("embargo", "work_embargo"),
        ("date_creation", "work_date_creation"),
        ("date_creation_legacy", "work_date_creation_legacy"),
        ("datetime_transmission", "work_datetime_transmission"),
        ("date_creation_legacy_2", "date_creation_legacy"),
        ("datetime_negotiation", "datetime_negotiation"),
        ("obsolete_doc", "resource_legal_id_obsolete_document"),
        ("obsolete_notice", "work_id_obsolete_notice"),
        ("comment_internal", "resource_legal_comment_internal"),
        ("do_not_index", "do_not_index"),
        ("document_id", "work_id_document"),
    ]

    def fetch_admin_metadata(
        self,
        celex_list: List[str] = None,
        max_items: int = None,
        doc_types: List[str] = None,
    ) -> pd.DataFrame:
        """Fetch all remaining CDM properties as long-format triples.

        Returns DataFrame with columns: celex, property, value.
        """
        celex_f = self._celex_filter(doc_types)
        all_rows = []

        for prop_name, cdm_prop in self.ADMIN_PROPERTIES:
            offset = 0
            batch_size = config.SPARQL_BATCH_SIZE

            while True:
                limit = (min(batch_size, max_items - len(all_rows))
                         if max_items else batch_size)

                celex_filter = ""
                if celex_list:
                    values = " ".join(f'"{c}"' for c in celex_list)
                    celex_filter = f"VALUES ?celex {{ {values} }}"

                query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?celex ?value
WHERE {{
    {celex_filter}
    ?work cdm:resource_legal_id_celex ?celex .
    {celex_f}
    ?work cdm:{cdm_prop} ?value .
}}
OFFSET {offset}
LIMIT {limit}
"""
                rows = self._query(query)
                if not rows:
                    break
                for r in rows:
                    r["property"] = prop_name
                    # Shorten URIs to readable suffixes
                    val = r.get("value", "")
                    if val.startswith("http://"):
                        val = val.rsplit("/", 1)[-1]
                    r["value"] = val
                all_rows.extend(rows)
                offset += len(rows)
                if max_items and len(all_rows) >= max_items:
                    break
                if len(rows) < limit:
                    break

            count = sum(1 for r in all_rows if r.get("property") == prop_name)
            if count:
                logger.info(f"  {prop_name}: {count} entries")

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["celex", "property", "value"])
        logger.info(f"Fetched {len(df)} admin metadata entries from CELLAR")
        return df

    # ── Save to disk ──────────────────────────────────────────────────────

    def save_decisions(self, df: pd.DataFrame, path: str = None):
        """Save decisions DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} decisions to {path}")

    def save_citations(self, df: pd.DataFrame, path: str = None):
        """Save citations DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_citations.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} citations to {path}")

    def save_subject_matter(self, df: pd.DataFrame, path: str = None):
        """Save subject matter DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_subjects.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} subject matter entries to {path}")

    # ── Subject taxonomy (standalone) ─────────────────────────────────────

    def fetch_subject_taxonomy(self) -> pd.DataFrame:
        """Fetch the CELLAR subject-matter taxonomy (codes + labels only).

        Downloads the concept hierarchy from the same four CELLAR taxonomies
        used by fetch_subject_matter(), but without linking to individual
        cases.  This gives a complete reference table of all available
        subject codes and their English labels.

        Returns:
            DataFrame with columns: code, label, source
        """
        all_rows = []
        sources = [
            ("eurovoc",
             "http://eurovoc.europa.eu/",
             """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?code ?label WHERE {{
    ?concept a skos:Concept .
    FILTER(STRSTARTS(STR(?concept), "http://eurovoc.europa.eu/"))
    ?concept skos:prefLabel ?label .
    FILTER(LANG(?label) = "en")
    BIND(REPLACE(STR(?concept), "http://eurovoc.europa.eu/", "") AS ?code)
}}
ORDER BY ?code
OFFSET {offset} LIMIT {limit}
"""),
            ("case_law_subject",
             "http://publications.europa.eu/resource/authority/case-law-subject-matter/",
             """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?code ?label WHERE {{
    ?concept a skos:Concept .
    FILTER(STRSTARTS(STR(?concept),
           "http://publications.europa.eu/resource/authority/case-law-subject-matter/"))
    ?concept skos:prefLabel ?label .
    FILTER(LANG(?label) = "en")
    BIND(REPLACE(STR(?concept),
         "http://publications.europa.eu/resource/authority/case-law-subject-matter/", "")
         AS ?code)
}}
ORDER BY ?code
OFFSET {offset} LIMIT {limit}
"""),
            ("case_law_directory",
             "http://publications.europa.eu/resource/authority/fd_578/",
             """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?code ?label WHERE {{
    ?concept a skos:Concept .
    FILTER(STRSTARTS(STR(?concept),
           "http://publications.europa.eu/resource/authority/fd_578/"))
    ?concept skos:prefLabel ?label .
    FILTER(LANG(?label) = "en")
    BIND(REPLACE(STR(?concept),
         "http://publications.europa.eu/resource/authority/fd_578/", "")
         AS ?code)
}}
ORDER BY ?code
OFFSET {offset} LIMIT {limit}
"""),
            ("case_law_directory_old",
             "http://publications.europa.eu/resource/authority/fd_577/",
             """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?code ?label WHERE {{
    ?concept a skos:Concept .
    FILTER(STRSTARTS(STR(?concept),
           "http://publications.europa.eu/resource/authority/fd_577/"))
    ?concept skos:prefLabel ?label .
    FILTER(LANG(?label) = "en")
    BIND(REPLACE(STR(?concept),
         "http://publications.europa.eu/resource/authority/fd_577/", "")
         AS ?code)
}}
ORDER BY ?code
OFFSET {offset} LIMIT {limit}
"""),
        ]

        for source_name, _uri, query_tpl in sources:
            offset = 0
            batch_size = config.SPARQL_BATCH_SIZE
            while True:
                query = query_tpl.format(offset=offset, limit=batch_size)
                logger.info(f"Fetching {source_name} taxonomy: offset={offset}")
                rows = self._query(query)
                if not rows:
                    break
                for r in rows:
                    r["source"] = source_name
                all_rows.extend(rows)
                offset += len(rows)
                logger.info(f"  → Got {len(rows)} concepts (total {source_name}: {offset})")
                if len(rows) < batch_size:
                    break

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["code", "label", "source"]
        )
        df = df.drop_duplicates(subset=["code", "source"], keep="first")
        logger.info(f"Fetched {len(df)} taxonomy entries "
                     f"({df['source'].value_counts().to_dict() if len(df) else {}})")
        return df

    def save_subject_taxonomy(self, df: pd.DataFrame, path: str = None):
        """Save subject taxonomy as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "subject_taxonomy.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} taxonomy entries to {path}")
        return path

    def save_joined_cases(self, df: pd.DataFrame, path: str = None):
        """Save joined-cases DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_joined_cases.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} joined-case links to {path}")

    def save_appeals(self, df: pd.DataFrame, path: str = None):
        """Save appeals DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_appeals.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} appeal links to {path}")

    def save_annulled_acts(self, df: pd.DataFrame, path: str = None):
        """Save annulled-acts DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_annulled_acts.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} annulled-act links to {path}")

    def save_interveners(self, df: pd.DataFrame, path: str = None):
        """Save interveners DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_interveners.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} intervener entries to {path}")

    def save_ag_opinions(self, df: pd.DataFrame, path: str = None):
        """Save AG opinion links DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_ag_opinions.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} AG opinion links to {path}")

    def save_legislation_links(self, df: pd.DataFrame, path: str = None):
        """Save legislation links DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_legislation_links.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} legislation links to {path}")

    def save_academic_citations(self, df: pd.DataFrame, path: str = None):
        """Save academic citations DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_academic_citations.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} academic citation entries to {path}")

    def save_referring_judgments(self, df: pd.DataFrame, path: str = None):
        """Save referring national judgments DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_referring_judgments.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} referring judgment entries to {path}")

    def save_dossiers(self, df: pd.DataFrame, path: str = None):
        """Save dossier groupings DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_dossiers.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} dossier links to {path}")

    def save_summaries(self, df: pd.DataFrame, path: str = None):
        """Save summary links DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_summaries.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} summary links to {path}")

    def save_misc_info(self, df: pd.DataFrame, path: str = None):
        """Save misc info DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_misc_info.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} misc info entries to {path}")

    def save_successors(self, df: pd.DataFrame, path: str = None):
        """Save successor links DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_successors.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} successor links to {path}")

    def save_incorporates(self, df: pd.DataFrame, path: str = None):
        """Save incorporation links DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_incorporates.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} incorporation links to {path}")

    def save_admin_metadata(self, df: pd.DataFrame, path: str = None):
        """Save admin metadata DataFrame as Parquet."""
        path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_admin_metadata.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} admin metadata entries to {path}")
