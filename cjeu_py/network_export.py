"""
Export CJEU citation networks as GEXF (Gephi), D3.js JSON, or
self-contained interactive HTML.

Builds a directed citation graph from cached pipeline data, computes
centrality metrics, and writes the network in a format ready for
interactive exploration in Gephi Lite, Gephi desktop, or a browser.

Node attributes include: date, year, court formation, procedure type,
case name, PageRank, betweenness centrality, in-degree, out-degree.

Edge direction: citing → cited.
"""
import json
import logging
import os
from typing import Optional

import networkx as nx
import pandas as pd

logger = logging.getLogger(__name__)

# GEXF does not support None or complex types.
# These helpers sanitise attributes before export.

NODE_WARN_THRESHOLD = 5_000
NODE_HARD_WARN = 10_000

# Case-law subject matter code → human-readable label
SUBJECT_LABELS = {
    "RAPL": "Rapprochement of legislation",
    "ETAB": "Freedom of establishment",
    "CHDF": "Charter of Fundamental Rights",
    "PRIN": "General principles of EU law",
    "LPSE": "Free movement of persons/services",
    "DRIN": "Fundamental rights",
    "PSOC": "Social policy",
    "AIDE": "State aid",
    "LCT": "Free movement of capital/taxation",
    "ASIL": "Asylum / AFSJ",
    "PDON": "Data protection",
    "CITU": "EU citizenship",
    "DDES": "Non-discrimination",
    "ENVI": "Environment",
    "COJP": "Judicial cooperation",
    "TVA": "VAT / Taxation",
    "DADV": "Legal profession",
    "NAT": "Nature / environment",
    "DGEN": "General provisions",
    "RPRO": "Procedural rules",
    "PEM": "Free movement of workers",
    "JUAI": "Judicial assistance",
    "EFJS": "AFSJ / JHA",
    "MARC": "Internal market / free movement of goods",
    "CONC": "Competition",
    "TRAN": "Transport",
    "AGRI": "Agriculture",
    "PCIV": "Civil procedure",
    "BREV": "Intellectual property",
    "CPEN": "Criminal law",
    "PECH": "Fisheries",
    "INST": "Institutional law",
    "REXT": "External relations",
    "ELEC": "Energy / electricity",
    "DMIG": "Immigration",
    "PRRE": "Public procurement",
}


def _load_pipeline_data(data_dir: str):
    """Load decisions, citations, subjects, and case names from cached Parquet."""
    from cjeu_py import config

    cellar_dir = os.path.join(data_dir, "raw", "cellar")

    dec_path = os.path.join(cellar_dir, "gc_decisions.parquet")
    cit_path = os.path.join(cellar_dir, "gc_citations.parquet")
    sub_path = os.path.join(cellar_dir, "gc_subjects.parquet")
    names_path = os.path.join(cellar_dir, "case_names.parquet")

    if not os.path.exists(dec_path):
        raise FileNotFoundError(
            f"No decisions data at {dec_path}. Run: cjeu-py download-cellar"
        )
    if not os.path.exists(cit_path):
        raise FileNotFoundError(
            f"No citations data at {cit_path}. Run: cjeu-py download-cellar"
        )

    decisions = pd.read_parquet(dec_path)
    citations = pd.read_parquet(cit_path)

    subjects = None
    if os.path.exists(sub_path):
        subjects = pd.read_parquet(sub_path)

    case_names = None
    if os.path.exists(names_path):
        case_names = pd.read_parquet(names_path)

    return decisions, citations, subjects, case_names


def _filter_decisions(decisions, citations, subjects=None,
                      topic=None, formation=None,
                      date_from=None, date_to=None):
    """Apply filters to the decision set and matching citations."""
    mask = pd.Series(True, index=decisions.index)

    if formation:
        if "formation_code" in decisions.columns:
            mask &= decisions["formation_code"].str.contains(
                formation, case=False, na=False)

    if "date" in decisions.columns:
        decisions = decisions.copy()
        decisions["date"] = pd.to_datetime(decisions["date"], errors="coerce")
        if date_from:
            mask &= decisions["date"] >= pd.Timestamp(date_from)
        if date_to:
            mask &= decisions["date"] <= pd.Timestamp(date_to)

    if topic and subjects is not None:
        # Filter by subject matter label (substring match)
        topic_lower = topic.lower()
        matching_celex = set()
        for col in subjects.columns:
            if col == "celex":
                continue
            hits = subjects[subjects[col].astype(str).str.lower().str.contains(
                topic_lower, na=False)]["celex"]
            matching_celex.update(hits.tolist())
        if matching_celex:
            mask &= decisions["celex"].isin(matching_celex)
            logger.info(f"Topic filter '{topic}': {len(matching_celex)} matching decisions")
        else:
            logger.warning(f"Topic filter '{topic}' matched no decisions")

    filtered = decisions[mask]
    celex_set = set(filtered["celex"])

    # Keep citations where the citing case is in our filtered set
    filtered_cit = citations[citations["citing_celex"].isin(celex_set)]

    return filtered, filtered_cit


def _build_graph(decisions, citations, case_names=None, subjects=None,
                  case_law_only=True):
    """Build a directed citation graph with node attributes."""
    G = nx.DiGraph()
    decision_set = set(decisions["celex"].tolist())

    # Parse dates
    decisions = decisions.copy()
    if "date" in decisions.columns:
        decisions["date"] = pd.to_datetime(decisions["date"], errors="coerce")
        decisions["year"] = decisions["date"].dt.year

    # Case name lookup
    name_lookup = {}
    if case_names is not None and not case_names.empty:
        for _, row in case_names.iterrows():
            celex = row.get("celex")
            name = row.get("case_name")
            cid = row.get("case_id")
            if celex and pd.notna(name):
                name_lookup[celex] = {
                    "case_name": str(name),
                    "case_id": str(cid) if pd.notna(cid) else "",
                }

    # Subject lookup: celex → list of subject codes
    subject_lookup = {}
    if subjects is not None and not subjects.empty:
        for celex, grp in subjects.groupby("celex"):
            codes = grp["subject_code"].dropna().unique().tolist()
            subject_lookup[celex] = codes

    # Add decision nodes with metadata
    for _, row in decisions.iterrows():
        celex = row["celex"]
        attrs = {}
        if "ecli" in row and pd.notna(row["ecli"]):
            attrs["ecli"] = str(row["ecli"])
        if "date" in row and pd.notna(row["date"]):
            attrs["date"] = str(row["date"].date()) if hasattr(row["date"], "date") else str(row["date"])
        if "year" in row and pd.notna(row["year"]):
            attrs["year"] = int(row["year"])
        if "resource_type" in row and pd.notna(row["resource_type"]):
            attrs["resource_type"] = str(row["resource_type"])
        if "formation_code" in row and pd.notna(row["formation_code"]):
            attrs["formation"] = str(row["formation_code"])
        if "procedure_type" in row and pd.notna(row["procedure_type"]):
            attrs["procedure"] = str(row["procedure_type"])
        if "judge_rapporteur" in row and pd.notna(row["judge_rapporteur"]):
            attrs["judge_rapporteur"] = str(row["judge_rapporteur"])
        if "advocate_general" in row and pd.notna(row["advocate_general"]):
            attrs["advocate_general"] = str(row["advocate_general"])

        # Case name
        if celex in name_lookup:
            attrs["case_name"] = name_lookup[celex]["case_name"]
            if name_lookup[celex]["case_id"]:
                attrs["case_id"] = name_lookup[celex]["case_id"]

        # Subjects
        if celex in subject_lookup:
            attrs["subjects"] = subject_lookup[celex]

        G.add_node(celex, **attrs)

    # Add citation edges
    for _, row in citations.iterrows():
        citing = row["citing_celex"]
        cited = row["cited_celex"]

        if case_law_only and not (isinstance(cited, str) and cited.startswith("6")):
            continue

        # Add cited node if external
        if cited not in decision_set and not G.has_node(cited):
            attrs = {"external": True}
            if cited in name_lookup:
                attrs["case_name"] = name_lookup[cited]["case_name"]
                if name_lookup[cited]["case_id"]:
                    attrs["case_id"] = name_lookup[cited]["case_id"]
            G.add_node(cited, **attrs)

        if G.has_node(citing):
            G.add_edge(citing, cited)

    return G


def _compute_centrality(G):
    """Compute centrality metrics and set as node attributes."""
    n = G.number_of_nodes()
    if n == 0:
        return

    logger.info(f"Computing centrality metrics for {n} nodes...")

    in_deg = dict(G.in_degree())
    out_deg = dict(G.out_degree())
    nx.set_node_attributes(G, in_deg, "in_degree")
    nx.set_node_attributes(G, out_deg, "out_degree")

    logger.info("  PageRank...")
    try:
        pagerank = nx.pagerank(G, alpha=0.85, max_iter=200)
    except nx.PowerIterationFailedConvergence:
        pagerank = {node: 1.0 / n for node in G.nodes()}
    nx.set_node_attributes(G, pagerank, "pagerank")

    logger.info("  Betweenness centrality...")
    betweenness = nx.betweenness_centrality(G, k=min(500, n))
    nx.set_node_attributes(G, betweenness, "betweenness")

    # Community detection (on undirected projection)
    logger.info("  Community detection (Louvain)...")
    try:
        communities = list(nx.community.louvain_communities(
            G.to_undirected(), seed=42))
        communities = sorted(communities, key=len, reverse=True)
        community_map = {}
        for i, comm in enumerate(communities):
            for node in comm:
                community_map[node] = i
        nx.set_node_attributes(G, community_map, "community")
        logger.info(f"  {len(communities)} communities detected")
    except Exception:
        pass


def _sanitise_for_gexf(G):
    """Return a copy of G with GEXF-safe attributes (no None, no complex types)."""
    G_clean = G.copy()
    for node in G_clean.nodes():
        attrs = G_clean.nodes[node]
        to_remove = [k for k, v in attrs.items() if v is None]
        for k in to_remove:
            del attrs[k]
        # Convert lists to semicolon-separated strings (GEXF doesn't support lists)
        if "subjects" in attrs and isinstance(attrs["subjects"], list):
            attrs["subjects"] = ";".join(attrs["subjects"])
        # Normalise non-breaking hyphens (Gephi font compatibility)
        for k in list(attrs.keys()):
            if isinstance(attrs[k], str):
                attrs[k] = attrs[k].replace("\u2011", "-").replace("\u2010", "-")
    return G_clean


def _to_d3_json(G):
    """Convert graph to D3.js force-directed JSON format."""
    nodes = []
    for n, attrs in G.nodes(data=True):
        node_data = {"id": n}
        for k, v in attrs.items():
            if v is not None:
                if isinstance(v, str):
                    node_data[k] = v.replace("\u2011", "-").replace("\u2010", "-")
                elif isinstance(v, float):
                    node_data[k] = round(v, 6)
                else:
                    node_data[k] = v
        nodes.append(node_data)

    links = []
    for u, v in G.edges():
        links.append({"source": u, "target": v})

    # Global metadata for visualisation controls
    years = [attrs.get("year") for _, attrs in G.nodes(data=True)
             if attrs.get("year") is not None]

    meta = {
        "nodeCount": G.number_of_nodes(),
        "edgeCount": G.number_of_edges(),
    }
    if years:
        meta["yearMin"] = int(min(years))
        meta["yearMax"] = int(max(years))

    return {"meta": meta, "nodes": nodes, "links": links}


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>CJEU Citation Network</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body { font-family: sans-serif; margin: 0; overflow: hidden; }
        #controls {
            position: absolute; top: 10px; left: 10px;
            background: rgba(255,255,255,0.95); padding: 15px;
            border-radius: 5px; box-shadow: 0 0 5px rgba(0,0,0,0.2);
            width: 280px; max-height: 90vh; overflow-y: auto; z-index: 100;
        }
        #sidebar {
            position: absolute; top: 0; right: -420px; width: 400px;
            height: 100vh; background: #fff;
            box-shadow: -2px 0 10px rgba(0,0,0,0.2);
            transition: right 0.3s ease; z-index: 200; overflow-y: auto;
        }
        #sidebar.open { right: 0; }
        #sidebar-header {
            background: #2c3e50; color: #fff; padding: 15px;
            position: sticky; top: 0; z-index: 1;
        }
        #sidebar-header h3 { margin: 0 0 5px 0; font-size: 16px; }
        #sidebar-header .close-btn {
            position: absolute; top: 10px; right: 15px;
            cursor: pointer; font-size: 24px; color: #fff;
        }
        #sidebar-header .close-btn:hover { color: #e74c3c; }
        #sidebar-stats {
            padding: 12px 15px; background: #ecf0f1;
            font-size: 13px; border-bottom: 1px solid #bdc3c7;
        }
        #sidebar-stats div { margin-bottom: 4px; }
        #sidebar-citations { padding: 15px; }
        .cite-item {
            padding: 8px 10px; margin-bottom: 6px; border-radius: 4px;
            border-left: 3px solid #3498db; background: rgba(52,152,219,0.06);
            font-size: 13px;
        }
        .cite-item.outgoing { border-left-color: #e74c3c; background: rgba(231,76,60,0.06); }
        .cite-item a { color: #2980b9; text-decoration: none; }
        .cite-item a:hover { text-decoration: underline; }
        .subject-chip {
            display: inline-block; padding: 2px 7px; margin: 2px 3px 2px 0;
            border-radius: 10px; font-size: 10px; background: #dfe6e9; color: #2d3436;
        }
        svg { width: 100vw; height: 100vh; }
        .link { stroke-opacity: 0.25; }
        .node circle { stroke: #fff; stroke-width: 1.5px; cursor: pointer; }
        .node circle:hover { stroke: #333; stroke-width: 2px; }
        .tooltip {
            position: absolute; background: #333; color: #fff;
            padding: 8px 12px; border-radius: 4px; font-size: 12px;
            pointer-events: none; opacity: 0; max-width: 350px; z-index: 1000;
        }
        label { display: block; margin-bottom: 5px; cursor: pointer; }
        select, input[type="range"] { width: 100%; margin-bottom: 10px; }
        h3 { margin-top: 0; font-size: 16px; }
        .section { margin-bottom: 12px; border-bottom: 1px solid #ccc; padding-bottom: 8px; }
        .section-title { font-weight: bold; margin-bottom: 6px; font-size: 13px; }
        .stats-box {
            background: #f5f5f5; padding: 8px; border-radius: 3px; font-size: 12px;
        }
        .stats-box div { margin-bottom: 3px; }
        .filter-list {
            max-height: 140px; overflow-y: auto; border: 1px solid #ddd;
            padding: 5px; background: #fafafa; margin-bottom: 8px;
        }
        .filter-list label { display: flex; align-items: center; margin-bottom: 2px; font-size: 12px; }
        .filter-list input[type="checkbox"] { margin-right: 6px; }
        .filter-controls { display: flex; gap: 5px; margin-bottom: 5px; }
        .filter-controls button { flex: 1; padding: 3px 8px; font-size: 11px; cursor: pointer; }
    </style>
</head>
<body>
<div id="controls">
    <h3>CJEU Citation Network</h3>

    <div class="section">
        <div class="section-title">Node Size:</div>
        <select id="size-metric">
            <option value="pagerank">PageRank</option>
            <option value="in_degree">In-Degree (times cited)</option>
            <option value="out_degree">Out-Degree (citations made)</option>
            <option value="betweenness">Betweenness Centrality</option>
        </select>
        <strong style="font-size:12px">Scale: <span id="node-scale-value">1.0</span>x</strong>
        <input type="range" id="node-scale" min="0.2" max="3" step="0.1" value="1">
    </div>

    <div class="section">
        <div class="section-title">Node Color:</div>
        <select id="color-metric">
            <option value="community">Community (Louvain)</option>
            <option value="procedure">Procedure Type</option>
            <option value="year">Year</option>
        </select>
    </div>

    <div class="section">
        <div class="section-title">Edge Thickness:</div>
        <strong style="font-size:12px">Scale: <span id="edge-scale-value">1.0</span>x</strong>
        <input type="range" id="edge-scale" min="0.1" max="5" step="0.1" value="1">
    </div>

    <div class="section">
        <div class="section-title">Year Range:</div>
        <div style="font-size:12px; margin-bottom:4px;">
            <span id="yr-from-val">1950</span> &ndash; <span id="yr-to-val">2025</span>
        </div>
        <input type="range" id="yr-from" min="1950" max="2025" step="1" value="1950">
        <input type="range" id="yr-to" min="1950" max="2025" step="1" value="2025">
    </div>

    <div class="section">
        <div class="section-title">Filter by Subject:</div>
        <div class="filter-controls">
            <button onclick="selectAll('subj')">All</button>
            <button onclick="selectNone('subj')">None</button>
        </div>
        <div class="filter-list" id="subj-filters"></div>
    </div>

    <div class="section">
        <div class="section-title">Filter by Procedure:</div>
        <div class="filter-controls">
            <button onclick="selectAll('proc')">All</button>
            <button onclick="selectNone('proc')">None</button>
        </div>
        <div class="filter-list" id="proc-filters"></div>
    </div>

    <div class="stats-box">
        <div>Visible nodes: <strong id="vis-nodes">0</strong></div>
        <div>Visible edges: <strong id="vis-edges">0</strong></div>
        <div>Total: <strong id="total-nodes">0</strong> nodes, <strong id="total-edges">0</strong> edges</div>
    </div>
</div>

<div id="sidebar">
    <div id="sidebar-header">
        <span class="close-btn" onclick="closeSidebar()">&times;</span>
        <h3 id="sb-name">Case</h3>
        <div id="sb-celex" style="font-size:12px; opacity:0.8;"></div>
    </div>
    <div id="sidebar-stats"></div>
    <div id="sidebar-citations"></div>
</div>

<div class="tooltip" id="tooltip"></div>
<svg></svg>
<script>
const data = __DATA_PLACEHOLDER__;
const EURLEX = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:";
const SUBJ_LABELS = __SUBJECT_LABELS__;

const allNodes = data.nodes;
const allLinks = data.links;
const meta = data.meta || {};

document.getElementById("total-nodes").innerText = meta.nodeCount || allNodes.length;
document.getElementById("total-edges").innerText = meta.edgeCount || allLinks.length;

// Year range
const yearMin = meta.yearMin || 1950;
const yearMax = meta.yearMax || 2025;
const yrFrom = document.getElementById("yr-from");
const yrTo = document.getElementById("yr-to");
yrFrom.min = yearMin; yrFrom.max = yearMax; yrFrom.value = yearMin;
yrTo.min = yearMin; yrTo.max = yearMax; yrTo.value = yearMax;
document.getElementById("yr-from-val").innerText = yearMin;
document.getElementById("yr-to-val").innerText = yearMax;

// Subject matter filters (sorted by frequency)
const subjCounts = {};
allNodes.forEach(n => (n.subjects || []).forEach(s => { subjCounts[s] = (subjCounts[s]||0) + 1; }));
const subjTypes = Object.entries(subjCounts).sort((a,b) => b[1]-a[1]).map(e => e[0]);
const subjDiv = document.getElementById("subj-filters");
subjTypes.forEach(s => {
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox"; cb.checked = true; cb.dataset.subj = s;
    cb.addEventListener("change", update);
    label.appendChild(cb);
    label.appendChild(document.createTextNode((SUBJ_LABELS[s]||s) + " (" + subjCounts[s] + ")"));
    subjDiv.appendChild(label);
});

// Procedure type filters
const procTypes = [...new Set(allNodes.map(n => n.procedure).filter(Boolean))].sort();
const procDiv = document.getElementById("proc-filters");
const PROC_LABELS = {
    "PREJ": "Preliminary ruling", "ANNU": "Annulment",
    "CONS%3DOB": "Failure to fulfil (Comm.)", "ANNU%3DRF": "Annulment (referred)",
    "PVOI%3DRF": "Appeal (referred)", "REFER_PREL_URG": "Urgent preliminary",
    "ANNU%3DOB": "Annulment (obligatory)", "CONS%3DRF": "Failure to fulfil (ref.)",
    "PREJ%3DRI": "Prelim. (re-interp.)", "ANNU%3DRI": "Annulment (re-interp.)",
};
const RTYPE_LABELS = { "JUDG": "Judgment", "ORDER": "Order", "OPIN_AG": "AG Opinion" };
procTypes.forEach(p => {
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox"; cb.checked = true; cb.dataset.proc = p;
    cb.addEventListener("change", update);
    label.appendChild(cb);
    label.appendChild(document.createTextNode(PROC_LABELS[p] || p.replace(/%3D/g,"=").replace(/%3A/g,":")));
    procDiv.appendChild(label);
});

function selectAll(kind) {
    document.querySelectorAll(`#${kind}-filters input`).forEach(cb => cb.checked = true);
    update();
}
function selectNone(kind) {
    document.querySelectorAll(`#${kind}-filters input`).forEach(cb => cb.checked = false);
    update();
}

// Colors
const COMM_COLORS = [
    "#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00",
    "#a65628","#f781bf","#66c2a5","#fc8d62","#8da0cb",
    "#e78ac3","#a6d854","#ffd92f","#e5c494","#b3b3b3",
];
const procColor = d3.scaleOrdinal(d3.schemeTableau10);
const yearColor = d3.scaleSequential(d3.interpolateViridis).domain([yearMin, yearMax]);

function getColor(node) {
    const mode = document.getElementById("color-metric").value;
    if (mode === "procedure") return procColor(node.procedure || "unknown");
    if (mode === "year") return node.year ? yearColor(node.year) : "#ccc";
    return COMM_COLORS[(node.community || 0) % COMM_COLORS.length];
}

// Centrality maxima
const maxVals = {
    pagerank: Math.max(...allNodes.map(n => n.pagerank || 0)) || 1e-6,
    in_degree: Math.max(...allNodes.map(n => n.in_degree || 0)) || 1,
    out_degree: Math.max(...allNodes.map(n => n.out_degree || 0)) || 1,
    betweenness: Math.max(...allNodes.map(n => n.betweenness || 0)) || 1e-6,
};

function getNodeSize(node) {
    const metric = document.getElementById("size-metric").value;
    const scale = parseFloat(document.getElementById("node-scale").value);
    const val = node[metric] || 0;
    return (3 + Math.sqrt(val / maxVals[metric]) * 25) * scale;
}

function getEdgeWidth() {
    return parseFloat(document.getElementById("edge-scale").value) * 0.7;
}

// SVG + simulation
const width = window.innerWidth;
const height = window.innerHeight;

const svg = d3.select("svg").attr("width", width).attr("height", height)
    .call(d3.zoom().on("zoom", e => g.attr("transform", e.transform)));
const g = svg.append("g");

svg.append("defs").append("marker")
    .attr("id", "arrow").attr("viewBox", "0 0 10 6")
    .attr("refX", 15).attr("refY", 3)
    .attr("markerWidth", 8).attr("markerHeight", 6)
    .attr("orient", "auto")
    .append("path").attr("d", "M0,0L10,3L0,6").attr("fill", "#999");

const linkG = g.append("g").attr("class", "links");
const nodeG = g.append("g").attr("class", "nodes");

const simulation = d3.forceSimulation()
    .force("link", d3.forceLink().id(d => d.id).distance(80))
    .force("charge", d3.forceManyBody().strength(-200))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collide", d3.forceCollide().radius(d => getNodeSize(d) + 2));

// Sidebar
function openSidebar(node) {
    const sb = document.getElementById("sidebar");
    const name = node.case_name || node.id;
    document.getElementById("sb-name").innerText = name.length > 80 ? name.slice(0,77)+"..." : name;
    document.getElementById("sb-celex").innerText = node.id + (node.case_id ? " (" + node.case_id + ")" : "");

    // Subject chips
    const subjs = (node.subjects || []).map(s =>
        `<span class="subject-chip">${SUBJ_LABELS[s] || s}</span>`
    ).join("");

    const statsDiv = document.getElementById("sidebar-stats");
    statsDiv.innerHTML = `
        <div><strong>CELEX:</strong> <a href="${EURLEX}${node.id}" target="_blank" style="color:#2980b9">${node.id}</a></div>
        ${node.ecli ? `<div><strong>ECLI:</strong> ${node.ecli}</div>` : ""}
        <div><strong>Date:</strong> ${node.date || "?"}</div>
        ${node.resource_type ? `<div><strong>Type:</strong> ${RTYPE_LABELS[node.resource_type] || node.resource_type}</div>` : ""}
        <div><strong>Formation:</strong> ${node.formation || "?"}</div>
        <div><strong>Procedure:</strong> ${PROC_LABELS[node.procedure] || node.procedure || "?"}</div>
        <div><strong>Judge-Rapporteur:</strong> ${node.judge_rapporteur || "?"}</div>
        <div><strong>Advocate General:</strong> ${node.advocate_general || "?"}</div>
        ${subjs ? `<div style="margin-top:6px"><strong>Subjects:</strong><br>${subjs}</div>` : ""}
        <hr style="margin:8px 0;border-color:#ddd">
        <div><strong>PageRank:</strong> ${(node.pagerank || 0).toFixed(6)}</div>
        <div><strong>Betweenness:</strong> ${(node.betweenness || 0).toFixed(5)}</div>
        <div><strong>In-Degree:</strong> ${node.in_degree || 0} (cited by this many cases)</div>
        <div><strong>Out-Degree:</strong> ${node.out_degree || 0} (cites this many cases)</div>
        <div><strong>Community:</strong> ${node.community !== undefined ? node.community : "?"}</div>
    `;

    // Citation lists
    const citDiv = document.getElementById("sidebar-citations");
    const nodeMap = Object.fromEntries(allNodes.map(n => [n.id, n]));

    const citing = allLinks.filter(l => (l.target.id||l.target) === node.id)
        .map(l => nodeMap[l.source.id||l.source]).filter(Boolean)
        .sort((a,b) => (b.year||0)-(a.year||0));

    const cited = allLinks.filter(l => (l.source.id||l.source) === node.id)
        .map(l => nodeMap[l.target.id||l.target]).filter(Boolean)
        .sort((a,b) => (b.year||0)-(a.year||0));

    let html = `<h4 style="margin:10px 0 8px 0">Cited by (${citing.length})</h4>`;
    citing.slice(0,50).forEach(c => {
        const nm = c.case_name ? c.case_name.slice(0,60) : c.id;
        html += `<div class="cite-item"><a href="${EURLEX}${c.id}" target="_blank">${nm}</a> <span style="color:#999;font-size:11px">${c.year||""}</span></div>`;
    });
    if (citing.length > 50) html += `<div style="font-size:12px;color:#999;padding:8px">... and ${citing.length-50} more</div>`;

    html += `<h4 style="margin:15px 0 8px 0">Cites (${cited.length})</h4>`;
    cited.slice(0,50).forEach(c => {
        const nm = c.case_name ? c.case_name.slice(0,60) : c.id;
        html += `<div class="cite-item outgoing"><a href="${EURLEX}${c.id}" target="_blank">${nm}</a> <span style="color:#999;font-size:11px">${c.year||""}</span></div>`;
    });
    if (cited.length > 50) html += `<div style="font-size:12px;color:#999;padding:8px">... and ${cited.length-50} more</div>`;

    citDiv.innerHTML = html;
    sb.classList.add("open");
}

function closeSidebar() { document.getElementById("sidebar").classList.remove("open"); }

// Filter
function filterData() {
    const yFrom = parseInt(yrFrom.value);
    const yTo = parseInt(yrTo.value);
    const selProc = new Set();
    document.querySelectorAll("#proc-filters input:checked").forEach(cb => selProc.add(cb.dataset.proc));
    const selSubj = new Set();
    document.querySelectorAll("#subj-filters input:checked").forEach(cb => selSubj.add(cb.dataset.subj));
    const allSubjChecked = selSubj.size === subjTypes.length;

    let nodes = allNodes.filter(n => {
        if (n.year && (n.year < yFrom || n.year > yTo)) return false;
        if (n.procedure && selProc.size > 0 && !selProc.has(n.procedure)) return false;
        // Subject filter: node must have at least one selected subject (or no subjects = pass)
        if (!allSubjChecked && n.subjects && n.subjects.length > 0) {
            if (!n.subjects.some(s => selSubj.has(s))) return false;
        }
        return true;
    });

    const ids = new Set(nodes.map(n => n.id));
    const links = allLinks.filter(l => {
        return ids.has(l.source.id||l.source) && ids.has(l.target.id||l.target);
    });

    return { nodes, links };
}

function update() {
    const { nodes, links } = filterData();
    document.getElementById("vis-nodes").innerText = nodes.length;
    document.getElementById("vis-edges").innerText = links.length;

    let node = nodeG.selectAll(".node").data(nodes, d => d.id);
    node.exit().remove();
    const nodeEnter = node.enter().append("g").attr("class","node")
        .call(d3.drag().on("start", ds).on("drag", dd).on("end", de));
    nodeEnter.append("circle");
    nodeEnter.on("click", (e,d) => { e.stopPropagation(); openSidebar(d); });
    nodeEnter.on("mouseover", (e,d) => {
        const nm = d.case_name ? d.case_name.slice(0,60) : d.id;
        d3.select("#tooltip").style("opacity",1)
          .html(`<strong>${nm}</strong><br>${d.id}<br>Cited ${d.in_degree||0}x &middot; Cites ${d.out_degree||0}<br><em>Click for details</em>`)
          .style("left",(e.pageX+12)+"px").style("top",(e.pageY-20)+"px");
    }).on("mouseout", () => d3.select("#tooltip").style("opacity",0));
    node = nodeEnter.merge(node);
    node.select("circle").attr("r", d => getNodeSize(d)).attr("fill", d => getColor(d));

    let link = linkG.selectAll(".link").data(links, d =>
        (d.source.id||d.source)+"-"+(d.target.id||d.target));
    link.exit().remove();
    const linkEnter = link.enter().append("line").attr("class","link")
        .attr("stroke","#999").attr("marker-end","url(#arrow)");
    link = linkEnter.merge(link);
    link.attr("stroke-width", getEdgeWidth());

    simulation.nodes(nodes);
    simulation.force("link").links(links);
    simulation.alpha(1).restart();
    simulation.on("tick", () => {
        link.attr("x1",d=>d.source.x).attr("y1",d=>d.source.y)
            .attr("x2",d=>d.target.x).attr("y2",d=>d.target.y);
        node.attr("transform",d=>`translate(${d.x},${d.y})`);
    });
}

svg.on("click", closeSidebar);

yrFrom.addEventListener("input", e => { document.getElementById("yr-from-val").innerText = e.target.value; update(); });
yrTo.addEventListener("input", e => { document.getElementById("yr-to-val").innerText = e.target.value; update(); });
document.getElementById("size-metric").addEventListener("change", () => updateSizes());
document.getElementById("color-metric").addEventListener("change", () => {
    nodeG.selectAll(".node circle").attr("fill", d => getColor(d));
});
document.getElementById("node-scale").addEventListener("input", e => {
    document.getElementById("node-scale-value").innerText = parseFloat(e.target.value).toFixed(1);
    updateSizes();
});
document.getElementById("edge-scale").addEventListener("input", e => {
    document.getElementById("edge-scale-value").innerText = parseFloat(e.target.value).toFixed(1);
    linkG.selectAll(".link").attr("stroke-width", getEdgeWidth());
});

function updateSizes() { nodeG.selectAll(".node circle").attr("r", d => getNodeSize(d)); }
function ds(e,d) { if(!e.active) simulation.alphaTarget(0.3).restart(); d.fx=d.x; d.fy=d.y; }
function dd(e,d) { d.fx=e.x; d.fy=e.y; }
function de(e,d) { if(!e.active) simulation.alphaTarget(0); d.fx=null; d.fy=null; }

update();
</script>
</body>
</html>"""


def _to_html(G):
    """Generate a self-contained HTML file with embedded D3.js visualization."""
    d3_data = _to_d3_json(G)
    json_str = json.dumps(d3_data, ensure_ascii=False)
    subj_json = json.dumps(SUBJECT_LABELS, ensure_ascii=False)
    html = _HTML_TEMPLATE.replace("__DATA_PLACEHOLDER__", json_str)
    html = html.replace("__SUBJECT_LABELS__", subj_json)
    return html


def export_network(
    data_dir: str,
    output_path: str,
    fmt: str = "gexf",
    topic: Optional[str] = None,
    formation: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    include_legislation: bool = False,
    max_nodes: Optional[int] = None,
) -> str:
    """Build and export the citation network.

    Args:
        data_dir: Root data directory (usually 'data/')
        output_path: Where to write the output file
        fmt: Output format ('gexf', 'd3', or 'html')
        topic: Subject matter filter (substring match)
        formation: Court formation filter (e.g. 'GRAND_CH')
        date_from: Earliest decision date (YYYY-MM-DD)
        date_to: Latest decision date (YYYY-MM-DD)
        include_legislation: Include citations to legislation/treaties
        max_nodes: Limit node count (takes top N by PageRank)

    Returns:
        Path to the exported file
    """
    decisions, citations, subjects, case_names = _load_pipeline_data(data_dir)
    logger.info(f"Loaded {len(decisions)} decisions, {len(citations)} citation pairs")

    # Apply filters
    has_filters = any([topic, formation, date_from, date_to])
    if has_filters:
        decisions, citations = _filter_decisions(
            decisions, citations, subjects,
            topic=topic, formation=formation,
            date_from=date_from, date_to=date_to,
        )
        logger.info(f"After filters: {len(decisions)} decisions, {len(citations)} citations")

    if decisions.empty:
        logger.error("No decisions match the given filters.")
        return ""

    # Build graph
    case_law_only = not include_legislation
    G = _build_graph(decisions, citations, case_names, subjects,
                     case_law_only=case_law_only)
    logger.info(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    # Compute centrality
    _compute_centrality(G)

    # Node limit
    if max_nodes and G.number_of_nodes() > max_nodes:
        pagerank = nx.get_node_attributes(G, "pagerank")
        top_nodes = sorted(pagerank, key=pagerank.get, reverse=True)[:max_nodes]
        G = G.subgraph(top_nodes).copy()
        logger.info(f"Trimmed to top {max_nodes} nodes by PageRank")

    # Warnings for large networks
    n_nodes = G.number_of_nodes()
    n_edges = G.number_of_edges()
    if n_nodes > NODE_HARD_WARN:
        logger.warning(
            f"Large network: {n_nodes:,} nodes, {n_edges:,} edges. "
            f"Gephi Lite and D3.js may become unresponsive. "
            f"Consider using --max-nodes {NODE_WARN_THRESHOLD} or adding filters."
        )
    elif n_nodes > NODE_WARN_THRESHOLD:
        logger.warning(
            f"Network has {n_nodes:,} nodes, {n_edges:,} edges. "
            f"D3.js browser rendering may be slow. Gephi desktop handles this fine."
        )

    # Export
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    if fmt == "gexf":
        G_clean = _sanitise_for_gexf(G)
        nx.write_gexf(G_clean, output_path)
    elif fmt == "d3":
        data = _to_d3_json(G)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    elif fmt == "html":
        html_content = _to_html(G)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
    else:
        raise ValueError(f"Unknown format: {fmt}. Use 'gexf', 'd3', or 'html'.")

    size_kb = os.path.getsize(output_path) / 1024
    logger.info(f"Exported {fmt.upper()}: {n_nodes:,} nodes, {n_edges:,} edges "
                f"({size_kb:.0f} KB) → {output_path}")

    return output_path
