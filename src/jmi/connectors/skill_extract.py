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
        "social media",
        "customer service",
        "warehouse",
        "healthcare",
        "hospitality",
        "retail",
        "education",
        "construction",
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
        # Broader tech / data / cloud (Adzuna + India market coverage)
        "spring boot",
        "express",
        "nestjs",
        "hibernate",
        "grpc",
        "protobuf",
        "nginx",
        "selenium",
        "jira",
        "confluence",
        "figma",
        "microsoft fabric",
        "azure data factory",
        "azure synapse",
        "oracle",
        "pl/sql",
        "t-sql",
        "cassandra",
        "dynamodb",
        "quarkus",
        "opencv",
        "cuda",
        "mlflow",
        "delta lake",
        "apache spark",
        "react native",
        "flutter",
        "swiftui",
        "svelte",
        "webpack",
        "jest",
        "cypress",
        "postman",
        "openapi",
        "swagger",
        "sharepoint",
        "power apps",
        "power automate",
        "qlik",
        "informatica",
        "talend",
        "matlab",
        "solidity",
        "ansible tower",
        "prometheus",
        "grafana",
        "splunk",
        "elk stack",
        "frontend",
        "backend",
        "fullstack",
        "presales",
        "video editing",
        "vfx",
        "quality assurance",
        "financial services",
        "field sales",
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
    "bpo": ("customer service",),
    "telecalling": ("customer service",),
    "telecaller": ("customer service",),
    "einkauf": ("procurement",),
    "buchhaltung": ("accounting",),
    "buchhalter": ("accounting",),
    "buchhalterin": ("accounting",),
    "lager": ("logistics",),
    "logistik": ("logistics",),
    "pflege": ("healthcare",),
    "krankenpflege": ("healthcare",),
    "gastronomie": ("hospitality",),
    "einzelhandel": ("retail",),
    "lagerist": ("warehouse",),
    "call center": ("customer service",),
    "pl/sql": ("pl/sql",),
    "pl sql": ("pl/sql",),
    "springboot": ("spring boot",),
    "spring-boot": ("spring boot",),
    "ms fabric": ("microsoft fabric",),
    "fabric (microsoft)": ("microsoft fabric",),
    "azure fabric": ("microsoft fabric",),
    "next.js": ("react",),
    "nextjs": ("react",),
    "nestjs": ("node",),
    "go lang": ("golang",),
    "oracle pl/sql": ("oracle", "pl/sql"),
    "oracle sql": ("oracle", "sql"),
    "t sql": ("t-sql",),
    "tsql": ("t-sql",),
    "pyspark": ("apache spark",),
    "py torch": ("pytorch",),
    "scikit learn": ("machine learning",),
    "sklearn": ("machine learning",),
    "powerapps": ("power apps",),
    "influencer": ("social media", "digital marketing"),
    "full-stack": ("fullstack",),
    "full stack": ("fullstack",),
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


def _source_tag_fallback_skills(tag_list: list[str]) -> list[str]:
    """When allowlist extraction finds nothing, use cleaned Arbeitnow tags (source-native, not invented).

    Do not apply SKILL_STOPLIST here: stoplist is for title/description token noise; API tags like
    \"Consulting\" or \"IT\" are authoritative source labels even when generic as tokens.
    """
    out: set[str] = set()
    for tag in tag_list:
        t = _WS.sub(" ", str(tag or "").strip().lower())
        if len(t) < 2:
            continue
        if len(t) > 120:
            t = t[:120].rsplit(" ", 1)[0].strip()
            if len(t) < 2:
                continue
        out.add(t)
    return sorted(out)


def _normalize_tags_input(tags: object) -> list[str]:
    """Turn API `tags` into a list of strings. Strings must not be iterated char-by-char."""
    if tags is None:
        return []
    if isinstance(tags, str):
        s = tags.strip()
        return [s] if s else []
    if isinstance(tags, dict):
        out: list[str] = []
        for k, v in tags.items():
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
            elif isinstance(k, str) and k.strip():
                out.append(k.strip())
        return out
    out = []
    for t in tags:
        if t is None:
            continue
        s = str(t).strip()
        if s:
            out.append(s)
    return out


def extract_silver_skills(
    tags: Iterable[str] | None,
    title_raw: str,
    description_text: str,
    *,
    extra_context: str = "",
) -> list[str]:
    """Return sorted unique canonical skills derived from tags, title, and description.

    `extra_context` is optional text (e.g. Adzuna category label) folded into the match blob
    when tags are missing — improves recall without changing Bronze.
    """
    found: set[str] = set()
    tag_list = _normalize_tags_input(tags)

    for tag in tag_list:
        tl = tag.lower()
        _add_from_alias_key(tl, found)
        if tl in SKILL_ALLOWLIST and tl not in SKILL_STOPLIST:
            found.add(tl)
        tag_blob = _normalize_blob(tag)
        for phrase in _ALLOWLIST_BY_LEN:
            if phrase in SKILL_STOPLIST:
                continue
            if _phrase_in_blob(phrase, tag_blob):
                found.add(phrase)

    blob = _normalize_blob(title_raw, description_text, " ".join(tag_list), extra_context)
    if blob:
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

    if found:
        return sorted(found)
    return _source_tag_fallback_skills(tag_list)


def adzuna_enrich_weak_skills(
    skills: list[str],
    title_raw: str,
    description_text: str,
    *,
    extra_context: str = "",
) -> list[str]:
    """When Adzuna returns fewer than two skills, re-run extraction with category folded into the title blob.

    Title tokens are often blocked by SKILL_STOPLIST; synthetic title improves phrase matches without inventing tags.
    """
    if len(skills) >= 2:
        return sorted(set(skills))
    syn_title = f"{title_raw} {extra_context}".strip()
    merged = extract_silver_skills(None, syn_title, description_text, extra_context=extra_context)
    out = sorted(set(skills) | set(merged))
    return out if out else skills
