"""
Tests for the judgment header parser.

Uses inline XHTML snippets from three known case formats:
- Grand Chamber CJ judgment (C-16/19): preliminary ruling, observation-table reps
- Grand Chamber CJ judgment (C-660/13): old format, applicant/defendant/interveners
- AG opinion (C-53/19 P & C-65/19 P): different header structure
"""
import pytest
from cjeu_py.data_collection.judgment_header import (
    parse_judgment_header,
    flatten_assignments,
    derive_case_names,
    extract_operative_part,
    _parse_date,
    _parse_case_numbers,
    _parse_composition,
    _parse_representatives,
)


# ── Minimal XHTML fixtures ─────────────────────────────────────────────

XHTML_JUDGMENT_NEW = """<html><body>
<p class="coj-sum-title-1"><span class="coj-bold">JUDGMENT OF THE COURT (Grand Chamber)</span></p>
<p class="coj-sum-title-1">26 January 2021 (*1)</p>
<p class="coj-normal">In Case C\u201116/19,</p>
<p class="coj-normal"><span class="coj-bold">VL</span></p>
<p class="coj-pnormal">v</p>
<p class="coj-normal"><span class="coj-bold">Szpital Kliniczny,</span></p>
<p class="coj-normal">THE COURT (Grand Chamber),</p>
<p class="coj-normal">composed of K. Lenaerts, President, R. Silva de Lapuerta, Vice-President, A. Prechal, M. Vilaras, E. Regan and M. Ile\u0161i\u010d, Presidents of Chambers, E. Juh\u00e1sz, T. von Danwitz (Rapporteur), S. Rodin, F. Biltgen, K. J\u00fcrim\u00e4e, C. Lycourgos and N. J\u00e4\u00e4skinen, Judges,</p>
<p class="coj-normal">Advocate General: G. Pitruzzella,</p>
<p class="coj-normal">Registrar: M. Aleksejev, Head of Unit,</p>
<p class="coj-normal">having regard to the written procedure and further to the hearing on 10 March 2020,</p>
<p class="coj-normal">after considering the observations submitted on behalf of:</p>
<table><tr><td><p class="coj-count">\u2013</p></td><td>
<p class="coj-normal">VL, by M. Podskalna and A.M. Nizankowska-Horodecka, adwokaci,</p>
</td></tr></table>
<table><tr><td><p class="coj-count">\u2013</p></td><td>
<p class="coj-normal">the Polish Government, by B. Majczyna and A. Siwek, acting as Agents,</p>
</td></tr></table>
<p class="coj-normal">after hearing the Opinion of the Advocate General at the sitting on 18 June 2020,</p>
<p class="coj-normal">gives the following</p>
<p class="coj-sum-title-1"><span class="coj-bold">Judgment</span></p>
<table><tr><td><p class="coj-count">1</p></td><td><p class="coj-normal">This request concerns ...</p></td></tr></table>
</body></html>"""

XHTML_JUDGMENT_OLD = """<html><body>
<p class="sum-title-1"><span class="bold">JUDGMENT OF THE COURT (Grand Chamber)</span></p>
<p class="sum-title-1">28 July 2016 (*)</p>
<p class="normal">In Case C\u2011660/13,</p>
<p class="normal"><span class="bold">Council of the European Union,</span></p>
<p class="pstatus">applicant,</p>
<p class="normal">supported by:</p>
<p class="normal"><span class="bold">Czech Republic,</span></p>
<p class="pstatus">intervener,</p>
<p class="pnormal">v</p>
<p class="normal"><span class="bold">European Commission,</span></p>
<p class="pstatus">defendant,</p>
<p class="normal">THE COURT (Grand Chamber),</p>
<p class="normal">composed of K. Lenaerts, President, M. Ile\u0161i\u010d and L. Bay Larsen, Presidents of Chambers, A. Rosas (Rapporteur), E. Juh\u00e1sz and M. Berger, Judges,</p>
<p class="normal">Advocate General: E. Sharpston,</p>
<p class="normal">Registrar: I. Illessy, Administrator,</p>
<p class="normal">having regard to the written procedure and further to the hearing on 2 June 2015,</p>
<p class="normal">after hearing the Opinion of the Advocate General at the sitting on 26 November 2015,</p>
<p class="sum-title-1"><span class="bold">Judgment</span></p>
</body></html>"""

XHTML_AG_OPINION = """<html><body>
<p class="coj-sum-title-1"><span class="coj-bold">OPINION OF ADVOCATE GENERAL</span></p>
<p class="coj-sum-title-1"><span class="coj-bold">PITRUZZELLA</span></p>
<p class="coj-sum-title-1">delivered on 21 January 2021 (1)</p>
<p class="coj-subtitle">Joined Cases C\u201153/19 P and C\u201165/19 P</p>
<p class="coj-subtitle">Banco Santander, SA</p>
<p class="coj-subtitle">and Santusa Holding, SL</p>
<p class="coj-subtitle">v</p>
<p class="coj-subtitle">European Commission (C\u201153/19 P)</p>
<p class="coj-subtitle">and</p>
<p class="coj-subtitle">Kingdom of Spain</p>
<p class="coj-subtitle">v</p>
<p class="coj-subtitle">Banco Santander, SA,</p>
<p class="coj-subtitle">Santusa Holding, SL,</p>
<p class="coj-subtitle">European Commission (C\u201165/19 P)</p>
<table><tr><td><p class="coj-count">1.</p></td><td><p class="coj-normal">The present joined cases...</p></td></tr></table>
</body></html>"""


# ── Helper function tests ───────────────────────────────────────────────

class TestParseDate:
    def test_standard(self):
        assert _parse_date("26 January 2021") == "2021-01-26"

    def test_single_digit_day(self):
        assert _parse_date("2 June 2015") == "2015-06-02"

    def test_no_date(self):
        assert _parse_date("some random text") is None


class TestParseCaseNumbers:
    def test_single(self):
        assert _parse_case_numbers("In Case C\u201116/19,") == ["C-16/19"]

    def test_joined(self):
        nums = _parse_case_numbers("In Joined Cases C\u201153/19 P and C\u201165/19 P")
        assert "C-53/19" in nums
        assert "C-65/19" in nums

    def test_general_court(self):
        assert _parse_case_numbers("Case T\u2011344/19") == ["T-344/19"]

    def test_no_case(self):
        assert _parse_case_numbers("no case numbers here") == []


class TestParseComposition:
    COMP_TEXT = (
        "composed of K. Lenaerts, President, R. Silva de Lapuerta, Vice-President, "
        "A. Prechal, M. Vilaras, E. Regan and M. Ile\u0161i\u010d, Presidents of Chambers, "
        "E. Juh\u00e1sz, T. von Danwitz (Rapporteur), S. Rodin, F. Biltgen, "
        "K. J\u00fcrim\u00e4e, C. Lycourgos and N. J\u00e4\u00e4skinen, Judges,"
    )

    def test_judge_count(self):
        judges = _parse_composition(self.COMP_TEXT)
        assert len(judges) == 13

    def test_president(self):
        judges = _parse_composition(self.COMP_TEXT)
        president = [j for j in judges if j["role"] == "President"]
        assert len(president) == 1
        assert president[0]["name"] == "K. Lenaerts"

    def test_rapporteur(self):
        judges = _parse_composition(self.COMP_TEXT)
        rapporteur = [j for j in judges if j["role"] == "Rapporteur"]
        assert len(rapporteur) == 1
        assert "von Danwitz" in rapporteur[0]["name"]

    def test_vice_president(self):
        judges = _parse_composition(self.COMP_TEXT)
        vp = [j for j in judges if j["role"] == "Vice-President"]
        assert len(vp) == 1

    def test_smaller_panel(self):
        text = "composed of M. Ile\u0161i\u010d, President of the Chamber, A. Rosas (Rapporteur) and E. Juh\u00e1sz, Judges,"
        judges = _parse_composition(text)
        assert len(judges) == 3


class TestParseRepresentatives:
    def test_adwokaci(self):
        reps = _parse_representatives("VL, by M. Podskalna and A.M. Nizankowska, adwokaci,")
        assert len(reps) == 2
        assert reps[0]["name"] == "M. Podskalna"
        assert reps[1]["title"] == "adwokaci"

    def test_acting_as_agents(self):
        reps = _parse_representatives(
            "the Polish Government, by B. Majczyna and A. Siwek, acting as Agents,"
        )
        assert len(reps) == 2
        assert reps[0]["name"] == "B. Majczyna"

    def test_no_by(self):
        reps = _parse_representatives("some text without representatives")
        assert reps == []


# ── Full parser tests ───────────────────────────────────────────────────

class TestParseJudgmentNew:
    """Test parser on new-format (coj-) Grand Chamber judgment."""

    @pytest.fixture
    def meta(self):
        return parse_judgment_header(XHTML_JUDGMENT_NEW)

    def test_doc_type(self, meta):
        assert meta["doc_type"] == "judgment"

    def test_date(self, meta):
        assert meta["date"] == "2021-01-26"

    def test_case_numbers(self, meta):
        assert meta["case_numbers"] == ["C-16/19"]

    def test_formation(self, meta):
        assert meta["formation"] == "Grand Chamber"

    def test_applicant(self, meta):
        assert "VL" in meta["parties"]["applicants"]

    def test_defendant(self, meta):
        assert any("Szpital" in d for d in meta["parties"]["defendants"])

    def test_composition_count(self, meta):
        assert len(meta["composition"]) == 13

    def test_advocate_general(self, meta):
        assert "Pitruzzella" in meta["advocate_general"]

    def test_registrar(self, meta):
        assert "Aleksejev" in meta["registrar"]

    def test_representatives(self, meta):
        assert len(meta["representatives"]) >= 2
        vl_reps = [r for r in meta["representatives"] if r["party"] == "VL"]
        assert len(vl_reps) == 1
        assert len(vl_reps[0]["representatives"]) == 2

    def test_hearing_date(self, meta):
        assert meta["hearing_date"] == "2020-03-10"

    def test_ag_opinion_date(self, meta):
        assert meta["ag_opinion_date"] == "2020-06-18"


class TestParseJudgmentOld:
    """Test parser on old-format (no coj- prefix) judgment."""

    @pytest.fixture
    def meta(self):
        return parse_judgment_header(XHTML_JUDGMENT_OLD)

    def test_doc_type(self, meta):
        assert meta["doc_type"] == "judgment"

    def test_date(self, meta):
        assert meta["date"] == "2016-07-28"

    def test_case_numbers(self, meta):
        assert meta["case_numbers"] == ["C-660/13"]

    def test_applicant(self, meta):
        assert any("Council" in a for a in meta["parties"]["applicants"])

    def test_defendant(self, meta):
        assert any("Commission" in d for d in meta["parties"]["defendants"])

    def test_interveners(self, meta):
        assert any("Czech" in i for i in meta["parties"]["interveners"])

    def test_composition(self, meta):
        assert len(meta["composition"]) == 6

    def test_registrar(self, meta):
        assert "Illessy" in meta["registrar"]

    def test_hearing_date(self, meta):
        assert meta["hearing_date"] == "2015-06-02"

    def test_ag_opinion_date(self, meta):
        assert meta["ag_opinion_date"] == "2015-11-26"


class TestParseAgOpinion:
    """Test parser on AG opinion (different header structure)."""

    @pytest.fixture
    def meta(self):
        return parse_judgment_header(XHTML_AG_OPINION)

    def test_doc_type(self, meta):
        assert meta["doc_type"] == "ag_opinion"

    def test_date(self, meta):
        assert meta["date"] == "2021-01-21"

    def test_case_numbers(self, meta):
        assert "C-53/19" in meta["case_numbers"]
        assert "C-65/19" in meta["case_numbers"]

    def test_advocate_general(self, meta):
        assert meta["advocate_general"] == "Pitruzzella"

    def test_no_composition(self, meta):
        assert meta["composition"] == []

    def test_no_registrar(self, meta):
        assert meta["registrar"] is None

    def test_parties(self, meta):
        assert "Banco Santander, SA" in meta["parties"]["applicants"]
        assert "Kingdom of Spain" in meta["parties"]["defendants"]


# ── Derived table tests ─────────────────────────────────────────────────

class TestFlattenAssignments:
    def test_basic(self):
        headers = [
            {
                "celex": "62019CJ0016",
                "composition": [
                    {"name": "K. Lenaerts", "role": "President"},
                    {"name": "T. von Danwitz", "role": "Rapporteur"},
                    {"name": "S. Rodin", "role": "Judges"},
                ],
            }
        ]
        df = flatten_assignments(headers)
        assert len(df) == 3
        assert df[df.is_rapporteur].iloc[0]["judge_name"] == "T. von Danwitz"
        assert df["is_rapporteur"].sum() == 1

    def test_empty_composition(self):
        headers = [{"celex": "62019CC0053", "composition": []}]
        df = flatten_assignments(headers)
        assert len(df) == 0


class TestDeriveCaseNames:
    def test_standard(self):
        headers = [
            {
                "celex": "62013CJ0660",
                "parties": {
                    "applicants": ["Council"],
                    "defendants": ["Commission"],
                    "interveners": [],
                },
            }
        ]
        df = derive_case_names(headers)
        assert df.iloc[0]["case_name"] == "Council v Commission"

    def test_no_defendant(self):
        headers = [
            {
                "celex": "62019CJ0016",
                "parties": {
                    "applicants": ["VL"],
                    "defendants": [],
                    "interveners": [],
                },
            }
        ]
        df = derive_case_names(headers)
        assert df.iloc[0]["case_name"] == "VL"


XHTML_OPERATIVE = """<html><body>
<p class="coj-normal">Some reasoning paragraph.</p>
<p class="coj-normal">On those grounds, the Court (Grand Chamber) hereby:</p>
<table><tr><td><p class="coj-count">1</p></td><td>
<p class="coj-normal">Dismisses the appeal;</p>
</td></tr></table>
<table><tr><td><p class="coj-count">2</p></td><td>
<p class="coj-normal">Orders the applicant to pay the costs.</p>
</td></tr></table>
<p class="coj-normal">[Signatures]</p>
</body></html>"""


class TestExtractOperativePart:
    def test_basic(self):
        op = extract_operative_part(XHTML_OPERATIVE)
        assert op is not None
        assert "Dismisses the appeal" in op
        assert "pay the costs" in op

    def test_no_signatures_leaking(self):
        op = extract_operative_part(XHTML_OPERATIVE)
        assert "[Signatures]" not in op

    def test_ag_opinion_has_none(self):
        op = extract_operative_part(XHTML_AG_OPINION)
        assert op is None
