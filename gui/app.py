"""
cjeu-py Streamlit GUI — browser-based interface for the CJEU research toolkit.

Launch:  streamlit run gui/app.py
"""
import io
import json
import logging
import os
import sys
import tempfile

import pandas as pd
import streamlit as st

# Ensure the package is importable from the repo root
_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _repo not in sys.path:
    sys.path.insert(0, _repo)

from cjeu_py import config

# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="cjeu-py",
    page_icon="https://raw.githubusercontent.com/niccoloridi/cjeu-py/main/docs/logo.svg",
    layout="wide",
)

# ── Logging handler that writes to a Streamlit status container ──────────────


class _StreamlitLogHandler(logging.Handler):
    """Forward log records to a streamlit status container."""

    def __init__(self, container):
        super().__init__()
        self.container = container
        self.lines: list[str] = []

    def emit(self, record):
        msg = self.format(record)
        self.lines.append(msg)
        # Keep only last 50 lines to avoid overflow
        if len(self.lines) > 50:
            self.lines = self.lines[-50:]
        try:
            self.container.text("\n".join(self.lines[-15:]))
        except Exception:
            pass


def _attach_logger(container):
    """Attach a Streamlit handler to the cjeu-py logger and return it."""
    handler = _StreamlitLogHandler(container)
    handler.setFormatter(logging.Formatter("%(levelname)-8s %(message)s"))
    root = logging.getLogger("cjeu-py")
    root.addHandler(handler)
    # Also capture cellar_client logs
    for name in ("cjeu_py.data_collection.cellar_client",
                 "cjeu_py.data_collection.text_fetcher",
                 "cjeu_py.network_export",
                 "cjeu_py.search"):
        logging.getLogger(name).addHandler(handler)
    return handler


def _detach_logger(handler):
    """Remove the Streamlit handler from all loggers."""
    for name in ("cjeu-py",
                 "cjeu_py.data_collection.cellar_client",
                 "cjeu_py.data_collection.text_fetcher",
                 "cjeu_py.network_export",
                 "cjeu_py.search"):
        logging.getLogger(name).removeHandler(handler)


# ── Sidebar: data directory + file status ────────────────────────────────────

st.sidebar.image(
    "https://raw.githubusercontent.com/niccoloridi/cjeu-py/main/docs/logo.svg",
    width=180,
)
st.sidebar.markdown("---")

data_dir = st.sidebar.text_input(
    "Data directory",
    value=os.environ.get("CJEU_DATA_DIR", config.DATA_ROOT),
    help="Override the default data directory (~/.cjeu-py/data/)",
)

# Ensure config paths follow the sidebar setting
config.DATA_ROOT = data_dir
config.RAW_CELLAR_DIR = os.path.join(data_dir, "raw", "cellar")
config.RAW_TEXTS_DIR = os.path.join(data_dir, "raw", "texts")
config.PROCESSED_DIR = os.path.join(data_dir, "processed")
config.CLASSIFIED_DIR = os.path.join(data_dir, "classified")

st.sidebar.markdown("---")
st.sidebar.subheader("Data status")


def _scan_files(directory, extensions=(".parquet", ".jsonl")):
    """Scan a directory for data files and return (name, rows) pairs."""
    import pyarrow.parquet as pq

    results = []
    if not os.path.isdir(directory):
        return results
    for fname in sorted(os.listdir(directory)):
        fpath = os.path.join(directory, fname)
        if not os.path.isfile(fpath):
            continue
        if fname.endswith(".parquet"):
            try:
                n = pq.read_metadata(fpath).num_rows
                results.append((fname, n))
            except Exception:
                results.append((fname, "?"))
        elif fname.endswith(".jsonl"):
            try:
                with open(fpath) as f:
                    n = sum(1 for _ in f)
                results.append((fname, n))
            except Exception:
                results.append((fname, "?"))
    return results


def _show_data_status():
    dirs = [
        ("raw/cellar", config.RAW_CELLAR_DIR),
        ("raw/texts", config.RAW_TEXTS_DIR),
        ("processed", config.PROCESSED_DIR),
        ("classified", config.CLASSIFIED_DIR),
    ]
    any_found = False
    for label, path in dirs:
        files = _scan_files(path)
        if files:
            any_found = True
            st.sidebar.markdown(f"**{label}/**")
            for fname, n in files:
                st.sidebar.text(f"  {fname}  ({n} rows)")
    if not any_found:
        st.sidebar.info("No data files found. Use the Download tab to get started.")


_show_data_status()

if st.sidebar.button("Refresh status"):
    st.rerun()

# ── Main tabs ────────────────────────────────────────────────────────────────

tab_download, tab_browse, tab_search, tab_ontology, tab_network = st.tabs(
    ["Download", "Browse Data", "Search", "Ontology & Headnotes", "Network"]
)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 1 — DOWNLOAD                                                          ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_download:
    st.header("Download CJEU data from CELLAR")

    # ── 1a. Base metadata ────────────────────────────────────────────────
    st.subheader("1. Base metadata")
    st.caption(
        "Download decisions, citations, and subject matter codes from CELLAR SPARQL. "
        "Leave all filters blank to download the entire CJEU corpus."
    )

    COURT_OPTIONS = {
        None: "All courts",
        "CJ": "CJ — Court of Justice",
        "TJ": "TJ — General Court",
        "FJ": "FJ — Civil Service Tribunal (dissolved 2016)",
    }

    # Document type groups for the multiselect
    DOC_TYPE_GROUPS = {
        "Judgments (CJ/TJ/FJ)": ["CJ", "TJ", "FJ"],
        "Orders (CO/TO/FO)": ["CO", "TO", "FO"],
        "AG Opinions (CC/TC)": ["CC", "TC"],
        "Other judicial (CV/CP/CD/CX)": ["CV", "CP", "CD", "CX"],
        "Communications": ["CN", "CA", "CB", "CU", "CG", "TN", "TA", "TB", "FN", "FA", "FB"],
        "Procedural (CS/CT/TT/FT)": ["CS", "CT", "TT", "FT"],
    }

    col1, col2, col3 = st.columns(3)
    with col1:
        dl_court = st.selectbox("Court", list(COURT_OPTIONS.keys()),
                                format_func=lambda x: COURT_OPTIONS[x],
                                key="dl_court")
    with col2:
        dl_date_from = st.text_input("Date from (YYYY-MM-DD)", key="dl_date_from")
        dl_date_from = dl_date_from.strip() or None
    with col3:
        dl_date_to = st.text_input("Date to (YYYY-MM-DD)", key="dl_date_to")
        dl_date_to = dl_date_to.strip() or None

    # Document types (CELEX codes)
    dl_doc_groups = st.multiselect(
        "Document types",
        list(DOC_TYPE_GROUPS.keys()),
        default=["Judgments (CJ/TJ/FJ)"],
        help="Select which types of judicial acts to download. "
             "Default: judgments only. Add Orders and AG Opinions to get the full picture.",
        key="dl_doc_groups",
    )
    # Flatten selected groups into a list of CELEX codes
    dl_doc_types = []
    for group in dl_doc_groups:
        dl_doc_types.extend(DOC_TYPE_GROUPS[group])
    dl_doc_types = dl_doc_types or None  # None → default (judgments)

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        dl_formation = st.text_input(
            "Formation (substring)",
            placeholder="e.g. GRAND_CH, CHAMBER_3",
            help="Filter by court formation. Common values: GRAND_CH, CHAMBER_1 through CHAMBER_10, FULL_CT",
            key="dl_formation",
        )
        dl_formation = dl_formation.strip() or None
    with col_f2:
        dl_judge = st.text_input(
            "Judge-rapporteur (name)",
            placeholder="e.g. Lenaerts",
            help="Case-insensitive substring match on judge-rapporteur name",
            key="dl_judge",
        )
        dl_judge = dl_judge.strip() or None
    with col_f3:
        dl_ag = st.text_input(
            "Advocate General (name)",
            placeholder="e.g. Bobek",
            help="Case-insensitive substring match on AG name",
            key="dl_ag",
        )
        dl_ag = dl_ag.strip() or None

    col4, col5, col6 = st.columns(3)
    with col4:
        dl_resource = st.selectbox(
            "CDM resource type (optional)",
            [None, "JUDG", "INFO_JUR", "SUM_JUR", "ABSTRACT_JUR", "JUDG_EXTRACT"],
            format_func=lambda x: {
                None: "All",
                "JUDG": "JUDG — Judgment",
                "INFO_JUR": "INFO_JUR — Info juridique",
                "SUM_JUR": "SUM_JUR — Summary",
                "ABSTRACT_JUR": "ABSTRACT_JUR — Abstract",
                "JUDG_EXTRACT": "JUDG_EXTRACT — Extract",
            }[x],
            help="Additional filter on CELLAR resource type (CDM metadata). "
                 "Most users should leave this as 'All'.",
            key="dl_resource",
        )
    with col5:
        dl_max = st.number_input("Max decisions (0 = all)", min_value=0, value=0,
                                 key="dl_max")
        dl_max = dl_max or None
    with col6:
        dl_force = st.checkbox("Force re-download", key="dl_force")

    if st.button("Download base metadata", type="primary", key="btn_base"):
        from cjeu_py.data_collection.cellar_client import CellarClient

        os.makedirs(config.RAW_CELLAR_DIR, exist_ok=True)
        client = CellarClient()

        with st.status("Downloading base metadata...", expanded=True) as status:
            log_area = st.empty()
            handler = _attach_logger(log_area)
            try:
                # Decisions
                dec_path = os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet")
                if not dl_force and os.path.exists(dec_path):
                    decisions = pd.read_parquet(dec_path)
                    st.write(f"Cached: {len(decisions)} decisions")
                else:
                    st.write("Fetching decisions...")
                    decisions = client.fetch_decisions(
                        court=dl_court,
                        resource_type=dl_resource,
                        formation=dl_formation,
                        judge=dl_judge,
                        advocate_general=dl_ag,
                        date_from=dl_date_from,
                        date_to=dl_date_to,
                        max_items=dl_max,
                        doc_types=dl_doc_types,
                    )
                    client.save_decisions(decisions)
                    st.write(f"Downloaded {len(decisions)} decisions")

                # Citations
                cit_path = os.path.join(config.RAW_CELLAR_DIR, "gc_citations.parquet")
                if not dl_force and os.path.exists(cit_path):
                    st.write(f"Cached: citations")
                else:
                    st.write("Fetching citations...")
                    celex_list = decisions["celex"].tolist() if not decisions.empty else None
                    citations = client.fetch_citations(
                        celex_list=celex_list if dl_max else None,
                        max_items=dl_max * 10 if dl_max else None,
                        doc_types=dl_doc_types,
                    )
                    client.save_citations(citations)
                    st.write(f"Downloaded {len(citations)} citation pairs")

                # Subjects
                sub_path = os.path.join(config.RAW_CELLAR_DIR, "gc_subjects.parquet")
                if not dl_force and os.path.exists(sub_path):
                    st.write(f"Cached: subjects")
                else:
                    st.write("Fetching subjects...")
                    celex_list = decisions["celex"].tolist() if not decisions.empty else None
                    subjects = client.fetch_subject_matter(
                        celex_list=celex_list if dl_max else None,
                        max_items=dl_max * 5 if dl_max else None,
                    )
                    client.save_subject_matter(subjects)
                    st.write(f"Downloaded {len(subjects)} subject codes")

                status.update(label="Base metadata complete", state="complete")
            except Exception as e:
                status.update(label=f"Error: {e}", state="error")
            finally:
                _detach_logger(handler)

    st.markdown("---")

    # ── 1b. Extended metadata ────────────────────────────────────────────
    st.subheader("2. Extended metadata")
    st.caption("Joins, appeals, interveners, annulled acts, legislation links, case names, AG opinions, and more.")

    detail_level = st.selectbox(
        "Detail level",
        ["high", "medium", "all", "exhaustive", "kitchen_sink"],
        index=1,
        help=(
            "high: procedural links + legislation + AG opinions + case names. "
            "medium: + academic citations + referring judgments. "
            "all: + rare legislation link types. "
            "exhaustive: + dossiers, summaries, misc info, successors. "
            "kitchen_sink: every remaining CDM property."
        ),
    )
    ext_max = st.number_input("Max items per query (0 = all)", min_value=0, value=0,
                              key="ext_max")
    ext_max = ext_max or None
    ext_force = st.checkbox("Force re-download", key="ext_force")

    if st.button("Download extended metadata", type="primary", key="btn_ext"):
        from cjeu_py.data_collection.cellar_client import CellarClient

        os.makedirs(config.RAW_CELLAR_DIR, exist_ok=True)
        client = CellarClient()

        with st.status("Downloading extended metadata...", expanded=True) as status:
            log_area = st.empty()
            handler = _attach_logger(log_area)
            try:
                # Load CELEX list from decisions
                celex_list = None
                dec_path = os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet")
                if os.path.exists(dec_path):
                    decisions = pd.read_parquet(dec_path)
                    if not decisions.empty:
                        celex_list = decisions["celex"].dropna().unique().tolist()
                        st.write(f"Filtering to {len(celex_list)} decisions")

                # HIGH tasks
                high_tasks = [
                    ("joined cases", client.fetch_joined_cases, client.save_joined_cases,
                     "gc_joined_cases.parquet"),
                    ("appeals", client.fetch_appeals, client.save_appeals,
                     "gc_appeals.parquet"),
                    ("interveners", client.fetch_interveners, client.save_interveners,
                     "gc_interveners.parquet"),
                    ("annulled acts", client.fetch_annulled_acts, client.save_annulled_acts,
                     "gc_annulled_acts.parquet"),
                    ("case names", client.fetch_case_names, client.save_case_names,
                     "case_names.parquet"),
                ]

                for name, fetch_fn, save_fn, filename in high_tasks:
                    path = os.path.join(config.RAW_CELLAR_DIR, filename)
                    if not ext_force and os.path.exists(path):
                        st.write(f"Cached: {name}")
                        continue
                    st.write(f"Downloading {name}...")
                    df = fetch_fn(celex_list=celex_list, max_items=ext_max,
                                  doc_types=dl_doc_types)
                    save_fn(df)
                    st.write(f"Downloaded {len(df)} {name}")

                # AG opinions
                ag_path = os.path.join(config.RAW_CELLAR_DIR, "gc_ag_opinions.parquet")
                if not ext_force and os.path.exists(ag_path):
                    st.write("Cached: AG opinions")
                else:
                    st.write("Downloading AG opinion links...")
                    ag = client.fetch_ag_opinions(celex_list=celex_list, max_items=ext_max,
                                                  doc_types=dl_doc_types)
                    client.save_ag_opinions(ag)
                    st.write(f"Downloaded {len(ag)} AG opinion links")

                # Legislation links
                leg_path = os.path.join(config.RAW_CELLAR_DIR, "gc_legislation_links.parquet")
                include_low = detail_level in ("all", "exhaustive", "kitchen_sink")
                if not ext_force and os.path.exists(leg_path) and not include_low:
                    st.write("Cached: legislation links")
                else:
                    st.write(f"Downloading legislation links (include_low={include_low})...")
                    leg = client.fetch_legislation_links(
                        celex_list=celex_list, max_items=ext_max, include_low=include_low,
                        doc_types=dl_doc_types)
                    client.save_legislation_links(leg)
                    st.write(f"Downloaded {len(leg)} legislation links")

                if detail_level == "high":
                    status.update(label="Extended metadata (high) complete", state="complete")
                    _detach_logger(handler)
                    st.stop()

                # MEDIUM: academic + referring
                acad_path = os.path.join(config.RAW_CELLAR_DIR, "gc_academic_citations.parquet")
                if not ext_force and os.path.exists(acad_path):
                    st.write("Cached: academic citations")
                else:
                    st.write("Downloading academic citations...")
                    acad = client.fetch_academic_citations(celex_list=celex_list, max_items=ext_max)
                    client.save_academic_citations(acad)
                    st.write(f"Downloaded {len(acad)} academic citations")

                ref_path = os.path.join(config.RAW_CELLAR_DIR, "gc_referring_judgments.parquet")
                if not ext_force and os.path.exists(ref_path):
                    st.write("Cached: referring judgments")
                else:
                    st.write("Downloading referring judgments...")
                    ref = client.fetch_referring_judgments(celex_list=celex_list, max_items=ext_max)
                    client.save_referring_judgments(ref)
                    st.write(f"Downloaded {len(ref)} referring judgments")

                if detail_level in ("medium", "all"):
                    status.update(label=f"Extended metadata ({detail_level}) complete",
                                  state="complete")
                    _detach_logger(handler)
                    st.stop()

                # EXHAUSTIVE
                exhaustive_tasks = [
                    ("dossiers", client.fetch_dossiers, client.save_dossiers,
                     "gc_dossiers.parquet"),
                    ("summaries", client.fetch_summaries, client.save_summaries,
                     "gc_summaries.parquet"),
                    ("misc info", client.fetch_misc_info, client.save_misc_info,
                     "gc_misc_info.parquet"),
                    ("successors", client.fetch_successors, client.save_successors,
                     "gc_successors.parquet"),
                    ("incorporates", client.fetch_incorporates, client.save_incorporates,
                     "gc_incorporates.parquet"),
                ]

                for name, fetch_fn, save_fn, filename in exhaustive_tasks:
                    path = os.path.join(config.RAW_CELLAR_DIR, filename)
                    if not ext_force and os.path.exists(path):
                        st.write(f"Cached: {name}")
                        continue
                    st.write(f"Downloading {name}...")
                    df = fetch_fn(celex_list=celex_list, max_items=ext_max,
                                  doc_types=dl_doc_types)
                    save_fn(df)
                    st.write(f"Downloaded {len(df)} {name}")

                if detail_level == "exhaustive":
                    status.update(label="Extended metadata (exhaustive) complete",
                                  state="complete")
                    _detach_logger(handler)
                    st.stop()

                # KITCHEN_SINK
                admin_path = os.path.join(config.RAW_CELLAR_DIR, "gc_admin_metadata.parquet")
                if not ext_force and os.path.exists(admin_path):
                    st.write("Cached: admin metadata")
                else:
                    st.write("Downloading admin metadata (all remaining CDM properties)...")
                    admin = client.fetch_admin_metadata(celex_list=celex_list, max_items=ext_max,
                                                         doc_types=dl_doc_types)
                    client.save_admin_metadata(admin)
                    st.write(f"Downloaded {len(admin)} admin metadata entries")

                status.update(label="Extended metadata (kitchen_sink) complete",
                              state="complete")
            except Exception as e:
                status.update(label=f"Error: {e}", state="error")
            finally:
                _detach_logger(handler)

    st.markdown("---")

    # ── 1c. Enrich network ───────────────────────────────────────────────
    st.subheader("3. Enrich network metadata")
    st.caption(
        "Fetch ECLI, date, court, and formation for external cited cases "
        "(cases cited by your decisions but not in your downloaded set)."
    )

    dec_exists = os.path.exists(os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet"))
    cit_exists = os.path.exists(os.path.join(config.RAW_CELLAR_DIR, "gc_citations.parquet"))

    if not (dec_exists and cit_exists):
        st.warning("Download base metadata first (decisions + citations).")
    else:
        enrich_force = st.checkbox("Force re-download", key="enrich_force")

        if st.button("Enrich external cited cases", type="primary", key="btn_enrich"):
            from cjeu_py.data_collection.cellar_client import CellarClient

            with st.status("Enriching network...", expanded=True) as status:
                log_area = st.empty()
                handler = _attach_logger(log_area)
                try:
                    out_path = os.path.join(config.RAW_CELLAR_DIR, "cited_metadata.parquet")

                    if not enrich_force and os.path.exists(out_path):
                        existing = pd.read_parquet(out_path)
                        st.write(f"Cached: {len(existing)} cited metadata rows")
                        status.update(label="Already enriched (use force to re-download)",
                                      state="complete")
                    else:
                        decisions = pd.read_parquet(
                            os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet"))
                        citations = pd.read_parquet(
                            os.path.join(config.RAW_CELLAR_DIR, "gc_citations.parquet"))

                        decision_celex = set(decisions["celex"])
                        cited_celex = set(citations["cited_celex"])
                        external = sorted(
                            c for c in (cited_celex - decision_celex)
                            if isinstance(c, str) and c.startswith("6")
                        )
                        st.write(f"Found {len(external)} external cited cases")

                        if external:
                            client = CellarClient()
                            df = client.fetch_cited_metadata(external)
                            client.save_cited_metadata(df, output_dir=config.RAW_CELLAR_DIR)
                            st.write(f"Enriched metadata for {len(df)} cases")
                        status.update(label="Enrichment complete", state="complete")
                except Exception as e:
                    status.update(label=f"Error: {e}", state="error")
                finally:
                    _detach_logger(handler)

    st.markdown("---")

    # ── 1d. Fetch texts ──────────────────────────────────────────────────
    st.subheader("4. Fetch judgment texts")
    st.caption("Download full judgment XHTML from CELLAR REST API.")

    if not dec_exists:
        st.warning("Download base metadata first (decisions).")
    else:
        col_lang, col_conc = st.columns(2)
        with col_lang:
            txt_lang = st.text_input("Languages (comma-separated ISO 639-2/B)",
                                     value="eng",
                                     help="e.g. eng,fra,deu — tries each in order",
                                     key="txt_lang")
        with col_conc:
            txt_concurrency = st.slider("Concurrency", 1, 20, 10, key="txt_conc")

        txt_max = st.number_input("Max texts (0 = all)", min_value=0, value=0,
                                  key="txt_max")
        txt_max = txt_max or None

        if st.button("Fetch texts", type="primary", key="btn_texts"):
            from cjeu_py.data_collection.text_fetcher import fetch_texts

            os.makedirs(config.RAW_TEXTS_DIR, exist_ok=True)

            with st.status("Fetching judgment texts...", expanded=True) as status:
                log_area = st.empty()
                handler = _attach_logger(log_area)
                try:
                    decisions = pd.read_parquet(
                        os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet"))
                    celex_list = decisions["celex"].dropna().unique().tolist()
                    st.write(f"Fetching texts for {len(celex_list)} decisions...")

                    languages = tuple(
                        lang.strip() for lang in txt_lang.split(",") if lang.strip()
                    )
                    fetch_texts(
                        celex_list,
                        max_items=txt_max,
                        concurrency=txt_concurrency,
                        languages=languages,
                    )
                    status.update(label="Text fetching complete", state="complete")
                except Exception as e:
                    status.update(label=f"Error: {e}", state="error")
                finally:
                    _detach_logger(handler)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 2 — BROWSE DATA                                                       ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_browse:
    st.header("Browse downloaded data")

    # Collect available Parquet files
    available_tables = {}
    for label, dirpath in [
        ("raw/cellar", config.RAW_CELLAR_DIR),
        ("processed", config.PROCESSED_DIR),
        ("classified", config.CLASSIFIED_DIR),
    ]:
        if os.path.isdir(dirpath):
            for fname in sorted(os.listdir(dirpath)):
                if fname.endswith(".parquet"):
                    available_tables[f"{label}/{fname}"] = os.path.join(dirpath, fname)

    if not available_tables:
        st.info("No data files found. Use the Download tab first.")
    else:
        selected_table = st.selectbox("Select table", list(available_tables.keys()))
        table_path = available_tables[selected_table]

        @st.cache_data
        def _load_parquet(path, _mtime):
            return pd.read_parquet(path)

        mtime = os.path.getmtime(table_path)
        df = _load_parquet(table_path, mtime)

        st.write(f"**{len(df)} rows, {len(df.columns)} columns**")
        st.dataframe(df, width="stretch", height=400)

        # Single-table download: CSV + Excel
        dl_col1, dl_col2 = st.columns(2)
        base_name = os.path.basename(table_path).replace(".parquet", "")
        with dl_col1:
            csv_buf = io.StringIO()
            df.to_csv(csv_buf, index=False)
            st.download_button(
                "Download as CSV",
                csv_buf.getvalue(),
                file_name=f"{base_name}.csv",
                mime="text/csv",
                key="dl_table_csv",
            )
        with dl_col2:
            xlsx_buf = io.BytesIO()
            df.to_excel(xlsx_buf, index=False)
            st.download_button(
                "Download as Excel",
                xlsx_buf.getvalue(),
                file_name=f"{base_name}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_table_xlsx",
            )

        st.markdown("---")

        # ── Descriptive stats ────────────────────────────────────────────
        if "gc_decisions" in selected_table and "date" in df.columns:
            st.subheader("Descriptive statistics")

            col_a, col_b = st.columns(2)

            with col_a:
                st.markdown("**Decisions per year**")
                df_dated = df.dropna(subset=["date"]).copy()
                df_dated["year"] = pd.to_datetime(df_dated["date"], errors="coerce").dt.year
                year_counts = df_dated["year"].value_counts().sort_index()
                st.bar_chart(year_counts)

            with col_b:
                if "court" in df.columns:
                    st.markdown("**Decisions by court**")
                    court_counts = df["court"].value_counts()
                    st.bar_chart(court_counts)

                if "formation" in df.columns:
                    st.markdown("**Top 10 formations**")
                    form_counts = df["formation"].value_counts().head(10)
                    st.bar_chart(form_counts)

    st.markdown("---")

    # ── Text viewer ──────────────────────────────────────────────────────
    st.subheader("Text viewer")

    texts_path = os.path.join(config.RAW_TEXTS_DIR, "gc_texts.jsonl")
    if not os.path.exists(texts_path):
        st.info("No texts downloaded. Use the Download tab to fetch judgment texts.")
    else:
        celex_query = st.text_input("CELEX number", placeholder="e.g. 62019CJ0311",
                                    key="text_viewer_celex")
        if celex_query:
            celex_query = celex_query.strip()
            found = None
            with open(texts_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    doc = json.loads(line)
                    if doc.get("celex") == celex_query:
                        found = doc
                        break

            if found is None:
                st.warning(f"CELEX {celex_query} not found in downloaded texts.")
            elif found.get("status") != "ok":
                st.warning(f"Text not available (status: {found.get('status')})")
            else:
                st.write(f"**{found['celex']}** — {found.get('paragraph_count', '?')} paragraphs, "
                         f"{found.get('char_count', '?')} characters")
                paras = found.get("paragraphs", [])
                nums = found.get("paragraph_nums", [])
                text_lines = []
                for i, p in enumerate(paras):
                    num = nums[i] if i < len(nums) else i + 1
                    text_lines.append(f"**[{num}]** {p}")
                st.markdown("\n\n".join(text_lines))

    st.markdown("---")

    # ── Bulk export ──────────────────────────────────────────────────────
    st.subheader("Bulk export all tables")
    st.caption("Export every available pipeline table as CSV or Excel files (zipped).")

    export_fmt = st.radio("Format", ["csv", "xlsx"], horizontal=True, key="bulk_fmt")

    if st.button("Export all tables", type="primary", key="btn_bulk_export"):
        from cjeu_py.export import export_data
        import zipfile

        with st.status("Exporting all tables...", expanded=True) as status:
            try:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    exported = export_data(
                        data_dir=data_dir,
                        output_dir=tmp_dir,
                        fmt=export_fmt,
                    )

                    if not exported:
                        st.warning("No tables found to export.")
                        status.update(label="Nothing to export", state="error")
                    else:
                        for name, (path, n) in exported.items():
                            st.write(f"{name}: {n} rows")

                        # Zip all exported files
                        zip_buf = io.BytesIO()
                        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                            for name, (path, _) in exported.items():
                                zf.write(path, os.path.basename(path))
                        zip_buf.seek(0)

                        st.download_button(
                            f"Download all ({len(exported)} tables, .zip)",
                            zip_buf.getvalue(),
                            file_name=f"cjeu_py_export.zip",
                            mime="application/zip",
                            key="dl_bulk_zip",
                        )
                        status.update(
                            label=f"Exported {len(exported)} tables",
                            state="complete",
                        )
            except Exception as e:
                status.update(label=f"Error: {e}", state="error")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 3 — SEARCH                                                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_search:
    st.header("Search case-law data")

    search_modes = {
        "text": "Full-text search (local texts)",
        "headnote": "Headnote search (live CELLAR query)",
        "party": "Party / case name search",
        "citing": "Cases cited by a given CELEX",
        "cited-by": "Cases that cite a given CELEX",
        "topic": "Subject matter / topic search",
        "legislation": "Legislation links for a CELEX",
        "list": "List categories (topics, judges, AGs, formations, procedures)",
    }

    col_mode, col_query = st.columns([1, 2])
    with col_mode:
        search_mode = st.selectbox("Mode", list(search_modes.keys()),
                                   format_func=lambda k: f"{k} — {search_modes[k]}")
    with col_query:
        if search_mode == "list":
            search_query = st.selectbox(
                "Category",
                ["topics", "judges", "ags", "formations", "procedures"],
                key="search_query_list",
            )
        else:
            placeholder = {
                "text": "search terms...",
                "headnote": "search terms...",
                "party": "party name or case name...",
                "citing": "CELEX number (e.g. 62019CJ0311)",
                "cited-by": "CELEX number",
                "topic": "topic code or label...",
                "legislation": "CELEX number",
            }.get(search_mode, "")
            search_query = st.text_input("Query", placeholder=placeholder,
                                         key="search_query_text")

    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        search_limit = st.slider("Max results", 5, 200, 25, key="search_limit")
    with col_s2:
        search_date_from = st.text_input("Date from", key="search_date_from")
        search_date_from = search_date_from.strip() or None
    with col_s3:
        search_date_to = st.text_input("Date to", key="search_date_to")
        search_date_to = search_date_to.strip() or None

    search_court = st.selectbox("Court filter", list(COURT_OPTIONS.keys()),
                                format_func=lambda x: COURT_OPTIONS[x],
                                key="search_court")

    if st.button("Search", type="primary", key="btn_search"):
        if not search_query and search_mode != "list":
            st.warning("Enter a search query.")
        else:
            from cjeu_py.search import run_search

            with st.spinner("Searching..."):
                try:
                    raw_output = run_search(
                        data_dir=data_dir,
                        mode=search_mode,
                        query=search_query,
                        limit=search_limit,
                        fmt="json",
                        date_from=search_date_from,
                        date_to=search_date_to,
                        court=search_court,
                    )

                    try:
                        results = json.loads(raw_output)
                    except json.JSONDecodeError:
                        # Might be a plain text message
                        st.info(raw_output)
                        results = None

                    if results:
                        if isinstance(results, list):
                            results_df = pd.DataFrame(results)
                        elif isinstance(results, dict):
                            # Some modes return {"results": [...]} or similar
                            for key in ("results", "data", "rows"):
                                if key in results and isinstance(results[key], list):
                                    results_df = pd.DataFrame(results[key])
                                    break
                            else:
                                results_df = pd.DataFrame([results])
                        else:
                            results_df = pd.DataFrame()

                        if not results_df.empty:
                            st.write(f"**{len(results_df)} results**")
                            st.dataframe(results_df, width="stretch")

                            csv_buf = io.StringIO()
                            results_df.to_csv(csv_buf, index=False)
                            st.download_button(
                                "Download results as CSV",
                                csv_buf.getvalue(),
                                file_name=f"search_{search_mode}.csv",
                                mime="text/csv",
                                key="dl_search_csv",
                            )
                        else:
                            st.info("No results found.")
                except Exception as e:
                    st.error(f"Search error: {e}")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 4 — ONTOLOGY & HEADNOTES                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_ontology:
    st.header("Subject-matter taxonomy & headnote search")

    # ── Taxonomy browser ─────────────────────────────────────────────────
    st.subheader("Subject-matter taxonomy")
    st.caption(
        "Browse the CELLAR subject-matter classification systems: EuroVoc, "
        "case-law subject matter, and the case-law directory (fd_578/fd_577). "
        "Download the taxonomy first, then search or filter by keyword."
    )

    taxonomy_path = os.path.join(config.RAW_CELLAR_DIR, "subject_taxonomy.parquet")

    tax_col1, tax_col2 = st.columns([3, 1])
    with tax_col2:
        tax_force = st.checkbox("Force re-download", key="tax_force")

    if st.button("Download taxonomy from CELLAR", type="primary", key="btn_tax_dl"):
        from cjeu_py.data_collection.cellar_client import CellarClient

        os.makedirs(config.RAW_CELLAR_DIR, exist_ok=True)

        with st.status("Downloading taxonomy...", expanded=True) as status:
            log_area = st.empty()
            handler = _attach_logger(log_area)
            try:
                if not tax_force and os.path.exists(taxonomy_path):
                    st.write("Already downloaded (tick 'Force re-download' to refresh)")
                    status.update(label="Taxonomy already cached", state="complete")
                else:
                    client = CellarClient()
                    df = client.fetch_subject_taxonomy()
                    client.save_subject_taxonomy(df)
                    st.write(f"Downloaded {len(df)} taxonomy entries")
                    status.update(label="Taxonomy download complete", state="complete")
            except Exception as e:
                status.update(label=f"Error: {e}", state="error")
            finally:
                _detach_logger(handler)

    if os.path.exists(taxonomy_path):
        @st.cache_data
        def _load_taxonomy(path, _mtime):
            return pd.read_parquet(path)

        tax_mtime = os.path.getmtime(taxonomy_path)
        tax_df = _load_taxonomy(taxonomy_path, tax_mtime)

        st.write(f"**{len(tax_df)} concepts** across "
                 f"{tax_df['source'].nunique()} taxonomies")

        # Source filter
        tax_sources = ["All"] + sorted(tax_df["source"].unique().tolist())
        tax_source_filter = st.selectbox("Taxonomy source", tax_sources,
                                         key="tax_source")

        # Keyword search
        tax_keyword = st.text_input("Search taxonomy (keyword in code or label)",
                                     placeholder="e.g. environment, competition, CHDF...",
                                     key="tax_keyword")

        filtered = tax_df.copy()
        if tax_source_filter != "All":
            filtered = filtered[filtered["source"] == tax_source_filter]
        if tax_keyword:
            kw = tax_keyword.lower()
            mask = (
                filtered["code"].str.lower().str.contains(kw, na=False) |
                filtered["label"].str.lower().str.contains(kw, na=False)
            )
            filtered = filtered[mask]

        st.write(f"**{len(filtered)} matching concepts**")
        st.dataframe(filtered, width="stretch", height=400)

        # Download
        dl_c1, dl_c2 = st.columns(2)
        with dl_c1:
            csv_buf = io.StringIO()
            filtered.to_csv(csv_buf, index=False)
            st.download_button(
                "Download as CSV",
                csv_buf.getvalue(),
                file_name="subject_taxonomy.csv",
                mime="text/csv",
                key="dl_tax_csv",
            )
        with dl_c2:
            xlsx_buf = io.BytesIO()
            filtered.to_excel(xlsx_buf, index=False)
            st.download_button(
                "Download as Excel",
                xlsx_buf.getvalue(),
                file_name="subject_taxonomy.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_tax_xlsx",
            )
    else:
        st.info("Click the button above to download the taxonomy from CELLAR.")

    st.markdown("---")

    # ── Headnote search ──────────────────────────────────────────────────
    st.subheader("Headnote search")
    st.caption(
        "Search CELLAR headnotes and titles across the entire CJEU corpus "
        "(live query — no local data needed)."
    )

    hn_query = st.text_input("Search headnotes", placeholder="e.g. proportionality, "
                             "fundamental rights, free movement...",
                             key="hn_query")
    hn_limit = st.slider("Max results", 5, 200, 25, key="hn_limit")

    if st.button("Search headnotes", type="primary", key="btn_hn"):
        if not hn_query.strip():
            st.warning("Enter a search term.")
        else:
            from cjeu_py.search import search_headnote

            with st.spinner("Querying CELLAR..."):
                try:
                    raw = search_headnote(hn_query.strip(), limit=hn_limit, fmt="json")
                    try:
                        results = json.loads(raw)
                    except json.JSONDecodeError:
                        st.info(raw)
                        results = None

                    if results and isinstance(results, list):
                        hn_df = pd.DataFrame(results)
                        st.write(f"**{len(hn_df)} results** (live CELLAR query)")
                        st.dataframe(hn_df, width="stretch")

                        csv_buf = io.StringIO()
                        hn_df.to_csv(csv_buf, index=False)
                        st.download_button(
                            "Download results as CSV",
                            csv_buf.getvalue(),
                            file_name="headnote_search.csv",
                            mime="text/csv",
                            key="dl_hn_csv",
                        )
                    elif results:
                        st.info(str(results))
                except Exception as e:
                    st.error(f"Headnote search error: {e}")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  TAB 5 — NETWORK                                                           ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

with tab_network:
    st.header("Citation network export")

    has_data = os.path.exists(
        os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet")
    ) and os.path.exists(
        os.path.join(config.RAW_CELLAR_DIR, "gc_citations.parquet")
    )

    if not has_data:
        st.warning("Download base metadata first (decisions + citations).")
    else:
        st.subheader("Filters")

        col_n1, col_n2, col_n3 = st.columns(3)
        with col_n1:
            net_topic = st.text_input("Topic (substring)", key="net_topic")
            net_topic = net_topic.strip() or None
        with col_n2:
            net_formation = st.text_input("Formation (e.g. GRAND_CH)", key="net_formation")
            net_formation = net_formation.strip() or None
        with col_n3:
            net_court = st.selectbox("Court", list(COURT_OPTIONS.keys()),
                                     format_func=lambda x: COURT_OPTIONS[x],
                                     key="net_court")

        col_n4, col_n5 = st.columns(2)
        with col_n4:
            net_date_from = st.text_input("Date from", key="net_date_from")
            net_date_from = net_date_from.strip() or None
        with col_n5:
            net_date_to = st.text_input("Date to", key="net_date_to")
            net_date_to = net_date_to.strip() or None

        col_n6, col_n7 = st.columns(2)
        with col_n6:
            net_max_nodes = st.number_input("Max nodes (0 = no limit)", min_value=0,
                                            value=0, key="net_max_nodes")
            net_max_nodes = net_max_nodes or None
        with col_n7:
            net_internal = st.checkbox("Internal nodes only", key="net_internal",
                                       help="Exclude external cited cases (only show decisions "
                                            "in your downloaded set)")

        net_include_leg = st.checkbox("Include legislation links", key="net_include_leg")

        st.markdown("---")
        st.subheader("Export")

        col_btn1, col_btn2 = st.columns(2)

        with col_btn1:
            if st.button("Export GEXF (Gephi)", type="primary", key="btn_gexf"):
                from cjeu_py.network_export import export_network

                with st.status("Building network (GEXF)...", expanded=True) as status:
                    log_area = st.empty()
                    handler = _attach_logger(log_area)
                    try:
                        with tempfile.NamedTemporaryFile(suffix=".gexf", delete=False) as tmp:
                            tmp_path = tmp.name

                        path = export_network(
                            data_dir=data_dir,
                            output_path=tmp_path,
                            fmt="gexf",
                            topic=net_topic,
                            formation=net_formation,
                            court=net_court,
                            date_from=net_date_from,
                            date_to=net_date_to,
                            include_legislation=net_include_leg,
                            max_nodes=net_max_nodes,
                            internal_only=net_internal,
                        )
                        if path:
                            with open(path, "rb") as f:
                                gexf_data = f.read()
                            st.download_button(
                                "Download GEXF file",
                                gexf_data,
                                file_name="citation_network.gexf",
                                mime="application/gexf+xml",
                                key="dl_gexf",
                            )
                            status.update(label="GEXF export complete", state="complete")
                        else:
                            status.update(label="No network produced", state="error")
                    except Exception as e:
                        status.update(label=f"Error: {e}", state="error")
                    finally:
                        _detach_logger(handler)

        with col_btn2:
            if st.button("Export HTML (interactive)", type="primary", key="btn_html"):
                from cjeu_py.network_export import export_network

                with st.status("Building network (HTML)...", expanded=True) as status:
                    log_area = st.empty()
                    handler = _attach_logger(log_area)
                    try:
                        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as tmp:
                            tmp_path = tmp.name

                        path = export_network(
                            data_dir=data_dir,
                            output_path=tmp_path,
                            fmt="html",
                            topic=net_topic,
                            formation=net_formation,
                            court=net_court,
                            date_from=net_date_from,
                            date_to=net_date_to,
                            include_legislation=net_include_leg,
                            max_nodes=net_max_nodes,
                            internal_only=net_internal,
                        )
                        if path:
                            with open(path, "r") as f:
                                html_content = f.read()

                            st.download_button(
                                "Download HTML file",
                                html_content,
                                file_name="citation_network.html",
                                mime="text/html",
                                key="dl_html",
                            )

                            st.markdown("---")
                            st.subheader("Preview")
                            st.components.v1.html(html_content, height=700, scrolling=True)

                            status.update(label="HTML export complete", state="complete")
                        else:
                            status.update(label="No network produced", state="error")
                    except Exception as e:
                        status.update(label=f"Error: {e}", state="error")
                    finally:
                        _detach_logger(handler)
