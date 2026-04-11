"""
Rule-based skill extraction for Silver (tags + title + description).
No ML: allowlist, aliases, stoplist, phrase and token matching.
"""

from __future__ import annotations

import re
from typing import Iterable

# Canonical skill strings (lowercase). Multi-word phrases included.
SKILL_ALLOWLIST: frozenset[str] = frozenset(
    {
        "python",
        "java",
        "javascript",
        "typescript",
        "kotlin",
        "swift",
        "php",
        "ruby",
        "rust",
        "scala",
        "sql",
        "html",
        "css",
        "bash",
        "powershell",
        "golang",
        "react",
        "angular",
        "vue",
        "node",
        "django",
        "flask",
        "spring",
        "laravel",
        "rails",
        ".net",
        "dotnet",
        "graphql",
        "rest",
        "kafka",
        "redis",
        "elasticsearch",
        "mongodb",
        "postgresql",
        "mysql",
        "sqlite",
        "pandas",
        "numpy",
        "tensorflow",
        "pytorch",
        "spark",
        "hadoop",
        "airflow",
        "dbt",
        "aws",
        "azure",
        "gcp",
        "kubernetes",
        "docker",
        "terraform",
        "ansible",
        "jenkins",
        "gitlab",
        "github",
        "linux",
        "windows",
        "vmware",
        "etl",
        "excel",
        "tableau",
        "looker",
        "snowflake",
        "databricks",
        "bigquery",
        "synapse",
        "sap",
        "erp",
        "crm",
        "salesforce",
        "power bi",
        "business intelligence",
        "data engineering",
        "data science",
        "machine learning",
        "deep learning",
        "nlp",
        "computer vision",
        "statistics",
        "security",
        "cybersecurity",
        "penetration testing",
        "compliance",
        "gdpr",
        "iso 27001",
        "devops",
        "sre",
        "mlops",
        "agile",
        "scrum",
        "kanban",
        "ci/cd",
        "microservices",
        "api",
        "ui",
        "ux",
        "seo",
        "sem",
        "recruiting",
        "digital marketing",
        "network administration",
        "systems administration",
        "information systems",
        "automation",
        "blockchain",
        "iot",
        "software development",
        "data processing",
        "english",
        "german",
        "french",
        "sales",
        "accounting",
        "procurement",
        "logistics",
    }
)

# Keys must be lowercase stripped. Values are canonical allowlist names.
SKILL_ALIAS_MAP: dict[str, tuple[str, ...]] = {
    "js": ("javascript",),
    "ts": ("typescript",),
    "react.js": ("react",),
    "reactjs": ("react",),
    "node.js": ("node",),
    "nodejs": ("node",),
    "vue.js": ("vue",),
    "angularjs": ("angular",),
    "c#": (".net",),
    "c sharp": (".net",),
    "csharp": (".net",),
    "dot net": (".net",),
    ".net core": (".net",),
    "asp.net": (".net",),
    "k8s": ("kubernetes",),
    "kubernetess": ("kubernetes",),
    "tf": ("tensorflow",),
    "postgres": ("postgresql",),
    "psql": ("postgresql",),
    "ms sql": ("sql",),
    "mssql": ("sql",),
    "ms excel": ("excel",),
    "powerbi": ("power bi",),
    "power-bi": ("power bi",),
    "ml": ("machine learning",),
    "ai": ("machine learning",),
    "bi": ("business intelligence",),
    "ds": ("data science",),
    "sap/erp consulting": ("sap", "erp"),
    "system and network administration": ("network administration", "systems administration"),
    "online marketing": ("digital marketing",),
    "recruitment and selection": ("recruiting",),
    "data engineer": ("data engineering",),
    "automation engineering": ("automation",),
    "vertrieb": ("sales",),
    "einkauf": ("procurement",),
    "buchhaltung": ("accounting",),
    "buchhalter": ("accounting",),
    "buchhalterin": ("accounting",),
    "lager": ("logistics",),
    "logistik": ("logistics",),
}

# Tokens / phrases never promoted to skills (too generic or noisy).
SKILL_STOPLIST: frozenset[str] = frozenset(
    {
        "a",
        "an",
        "the",
        "and",
        "or",
        "for",
        "with",
        "from",
        "our",
        "your",
        "we",
        "you",
        "all",
        "new",
        "top",
        "best",
        "team",
        "work",
        "job",
        "jobs",
        "role",
        "position",
        "senior",
        "junior",
        "lead",
        "principal",
        "staff",
        "intern",
        "internship",
        "trainee",
        "mwd",
        "mw",
        "w",
        "d",
        "m",
        "f",
        "hr",
        "it",
        "ceo",
        "cto",
        "cfo",
        "management",
        "director",
        "head",
        "chief",
        "office",
        "remote",
        "hybrid",
        "onsite",
        "full",
        "time",
        "part",
        "temporary",
        "permanent",
        "gmbh",
        "ag",
        "ltd",
        "inc",
        "plc",
        "llc",
        "experience",
        "years",
        "year",
        "skills",
        "requirements",
        "nice",
        "have",
        "must",
        "will",
        "can",
        "please",
        "apply",
        "click",
        "here",
        "www",
        "com",
        "de",
        "en",
        "und",
        "der",
        "die",
        "das",
        "im",
        "in",
        "am",
        "zur",
        "bei",
        "zu",
        "auf",
        "als",
        "wir",
        "sie",
        "ihre",
        "uns",
        "über",
        "suchen",
        "bieten",
        "ab",
        "bis",
        "abteilung",
        "standort",
        "bereich",
        "entwicklung",
        "kunden",
        "projekt",
        "projekte",
        "aufgaben",
        "profil",
        "voraussetzungen",
        "wünschenswert",
        "benefits",
        "home",
        "office",
        "flexible",
        "working",
        "hours",
        "salary",
        "pay",
        "bonus",
        "company",
        "firm",
        "group",
        "division",
        "department",
        "service",
        "services",
        "solutions",
        "solution",
        "global",
        "international",
        "national",
        "local",
        "europe",
        "germany",
        "berlin",
        "munich",
        "frankfurt",
        "hamburg",
        "cologne",
        "consulting",
        "consultant",
        "advisor",
        "support",
        "customer",
        "client",
        "sales",
        "marketing",
        "communication",
        "finance",
        "legal",
        "operations",
        "strategy",
        "innovation",
        "digital",
        "social",
        "media",
        "content",
        "design",
        "product",
        "project",
        "process",
        "quality",
        "test",
        "testing",
        "engineer",
        "engineering",
        "developer",
        "development",
        "software",
        "hardware",
        "technical",
        "technology",
        "system",
        "systems",
        "application",
        "applications",
        "platform",
        "tools",
        "tool",
        "building",
        "asset",
        "supply",
        "accounts",
        "receivable",
        "controlling",
        "directors",
        "executives",
        "banking",
        "private",
        "fonds",
        "safety",
        "leader",
        "manager",
        "management",
        "administration",
        "administrator",
        "admin",
        "assistant",
        "associate",
        "specialist",
        "analyst",
        "architect",
        "scientist",
        "researcher",
        "officer",
        "coordinator",
        "representative",
        "executive",
        "vice",
        "president",
    }
)

_WS = re.compile(r"\s+")
_NON_WORD = re.compile(r"[^\w/+.#\-]+")
_TOKEN_SPLIT = re.compile(r"[^\w/+.#\-]+")

# Longest phrases first to prefer "machine learning" over "learning" (learning not in allowlist anyway)
_ALLOWLIST_BY_LEN: tuple[str, ...] = tuple(sorted(SKILL_ALLOWLIST, key=lambda s: (-len(s), s)))


def _normalize_blob(*parts: str) -> str:
    text = " ".join(p for p in parts if p).lower()
    text = _NON_WORD.sub(" ", text)
    return _WS.sub(" ", text).strip()


def _phrase_in_blob(phrase: str, blob: str) -> bool:
    if not phrase or not blob:
        return False
    p = phrase.strip().lower()
    b = f" {blob} "
    return f" {p} " in b


def _add_from_alias_key(key: str, found: set[str]) -> None:
    k = key.strip().lower()
    if not k:
        return
    if k in SKILL_STOPLIST:
        return
    if k in SKILL_ALIAS_MAP:
        for c in SKILL_ALIAS_MAP[k]:
            if c in SKILL_ALLOWLIST:
                found.add(c)
        return
    if k in SKILL_ALLOWLIST:
        found.add(k)


def extract_silver_skills(
    tags: Iterable[str] | None,
    title_raw: str,
    description_text: str,
) -> list[str]:
    """Return sorted unique canonical skills derived from tags, title, and description."""
    found: set[str] = set()
    tag_list = [str(t or "").strip() for t in (tags or []) if str(t or "").strip()]

    for tag in tag_list:
        tl = tag.lower()
        _add_from_alias_key(tl, found)
        if tl in SKILL_ALLOWLIST and tl not in SKILL_STOPLIST:
            found.add(tl)
        # Tag as free text: scan for embedded allowlist phrases
        tag_blob = _normalize_blob(tag)
        for phrase in _ALLOWLIST_BY_LEN:
            if phrase in SKILL_STOPLIST:
                continue
            if _phrase_in_blob(phrase, tag_blob):
                found.add(phrase)

    blob = _normalize_blob(title_raw, description_text, " ".join(tag_list))
    if not blob:
        return sorted(found)

    for phrase in _ALLOWLIST_BY_LEN:
        if _phrase_in_blob(phrase, blob):
            found.add(phrase)

    for tok in _TOKEN_SPLIT.split(blob):
        tok = tok.strip(".#_/-")
        if len(tok) < 2:
            continue
        tl = tok.lower()
        if tl in SKILL_STOPLIST:
            continue
        _add_from_alias_key(tl, found)
        if tl in SKILL_ALLOWLIST:
            found.add(tl)

    return sorted(found)
