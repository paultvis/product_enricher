# enricher.py
import os
import json
import re
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime, timezone

FALLBACK_PREFIX = "[AI] Using FALLBACK enrichment:"
API_PREFIX = "[AI] Using OpenAI API for enrichment."

try:
    # OpenAI SDK v1+
    from openai import OpenAI
    _HAS_OPENAI = True
    _SDK_IMPORT_ERROR = None
except Exception as e:
    _HAS_OPENAI = False
    _SDK_IMPORT_ERROR = e

MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Patterns that mean the base content is useless
_BAD_DESC_PATTERNS = [
    r"^no detailed description is available", r"^n/?a$", r"^tbc$", r"^none$",
    r"^\s*$"
]

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _is_bad(text: str) -> bool:
    if not isinstance(text, str):
        return True
    s = text.strip()
    if not s:
        return True
    s_plain = re.sub(r"<[^>]+>", " ", s, flags=re.IGNORECASE)
    s_plain = re.sub(r"\s+", " ", s_plain).strip().lower()
    for pat in _BAD_DESC_PATTERNS:
        if re.match(pat, s_plain):
            return True
    return len(s_plain) < 15

def _strip_leading_brand(s: str, brand: str) -> str:
    if not s or not brand:
        return s.strip() if isinstance(s, str) else s
    pattern = r'^\s*(?:' + re.escape(brand) + r')\s+'
    out = s
    prev = None
    while prev != out:
        prev = out
        out = re.sub(pattern, '', out, flags=re.IGNORECASE)
    return out.strip()

def _normalize_title(brand: str, product_name: str, aititle: str) -> str:
    """
    Keep technical tokens (sizes, power, ranges). Only de-duplicate brand.
    """
    b = (brand or "").strip()
    pn = (product_name or "").strip()
    t = (aititle or "").strip()
    if not t:
        return pn or (f"{b} {pn}".strip())

    if b and pn and pn.lower().startswith(b.lower()):
        stripped = _strip_leading_brand(t, b)
        # if model doubled brand, prefer product_name (should already include tech tokens)
        if stripped.lower() == pn[len(b):].strip().lower() or t.lower().startswith((b + " " + b).lower()):
            return pn

    if b:
        stripped = _strip_leading_brand(t, b)
        if t.lower().startswith(b.lower()):
            return (f"{b} {stripped}").strip()
        return t
    return t

def _normalise_inches_feet(s: str) -> str:
    """
    Convert inch/foot marks to ' in' / ' ft' to reduce fragile quotes.
    Examples: 52" -> 52 in ; 30' -> 30 ft
    """
    if not isinstance(s, str) or not s:
        return s
    s = re.sub(r'(?<=\d)"', ' in', s)   # inches
    s = re.sub(r"(?<=\d)'", ' ft', s)   # feet
    return s

def _strip_disallowed_tags(html: str) -> str:
    """
    Keep only allowed tags: p, ul, li, strong, em, h3.
    Remove script/style/iframe/obj and attributes.
    Delete any <table>... entirely (too heavy for product copy).
    """
    if not isinstance(html, str):
        return html or ""

    # remove dangerous blocks first
    html = re.sub(r"(?is)<(script|style|iframe|object|embed|svg).*?>.*?</\1>", "", html)
    # remove tables entirely
    html = re.sub(r"(?is)<table.*?>.*?</table>", "", html)

    # strip all tags but whitelisted by converting others to text
    # 1) Remove attributes from allowed tags
    html = re.sub(r"(?is)<(p|ul|li|strong|em|h3)(\s+[^>]*)?>", r"<\1>", html)
    html = re.sub(r"(?is)</(p|ul|li|strong|em|h3)>", r"</\1>", html)

    # 2) Remove all other tags
    html = re.sub(r"(?is)</?(?!p|ul|li|strong|em|h3)\w+[^>]*>", "", html)

    # normalise whitespace
    html = re.sub(r"\s+\n", "\n", html)
    html = re.sub(r"\n{3,}", "\n\n", html)
    html = re.sub(r"\s{2,}", " ", html)
    return html.strip()

def _ensure_html_structure(s: str) -> str:
    """
    Ensure output is proper HTML:
    - Wrap stray lines into <p>
    - Convert leading bullet characters to <ul><li>
    - Collapse double blank lines
    """
    if not isinstance(s, str):
        return ""

    text = s.strip()

    # Convert common bullet symbols to <ul>
    lines = [ln.strip() for ln in re.split(r"\r?\n", text)]
    out = []
    ul_open = False

    def open_ul():
        nonlocal ul_open
        if not ul_open:
            out.append("<ul>")
            ul_open = True

    def close_ul():
        nonlocal ul_open
        if ul_open:
            out.append("</ul>")
            ul_open = False

    bullet_pat = re.compile(r"^(\*|-|•|\u2022)\s+")
    for ln in lines:
        if not ln:
            close_ul()
            continue
        if bullet_pat.match(ln):
            open_ul()
            ln = bullet_pat.sub("", ln)
            out.append(f"<li>{ln}</li>")
        else:
            close_ul()
            # if already a tag start, keep as-is; else wrap in <p>
            if re.match(r"^<(p|ul|li|strong|em|h3)>", ln, flags=re.IGNORECASE):
                out.append(ln)
            else:
                out.append(f"<p>{ln}</p>")
    close_ul()

    html = "\n".join(out)
    # Strip anything not in allowed list
    html = _strip_disallowed_tags(html)
    # final tidy
    html = re.sub(r"\n{3,}", "\n\n", html).strip()
    return html

# --------------------------------------------------------------------------- #
# Prompt builder
# --------------------------------------------------------------------------- #

def _build_messages(product: Dict[str, Any]):
    """
    Build prompt that preserves supplier detail but makes it buyer-friendly:
    - Benefit-led overview
    - De-duplicated, merged features
    - “In practice” usage guidance (no 'DIY'), kept light
    - Quick specs (summarised, but keep figures)
    - Add: What's in the box (if detected)
    """
    brand = product.get("brand") or ""
    mpn = product.get("mpn") or ""
    name = product.get("name") or ""
    category = product.get("category") or ""
    base = product.get("base") or {}

    short_intro = base.get("shortdescription2") or ""
    long_html = base.get("longDescription") or ""

    brand_html = (base.get("brandcontent") or "")
    mode = "rich"
    if _is_bad(brand_html) and _is_bad(short_intro) and _is_bad(long_html):
        mode = "sparse"

    # Strict JSON schema to force proper escaping and bounded fields
    json_schema = {
        "name": "seo_enrichment",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "aititle": {"type": "string", "maxLength": 90},
                "aiDesc": {"type": "string", "maxLength": 5000},  # HTML body
                "aBrandDesc": {"type": "string", "maxLength": 280},  # we will wrap it in <p>
                "metaName": {"type": "string", "maxLength": 60},
                "metadescription": {"type": "string", "maxLength": 160},
                "metakeywords": {"type": "string", "maxLength": 300},
                "specs": {"type": "object", "additionalProperties": {"type": "string", "maxLength": 300}},
                "useCases": {"type": "array", "items": {"type": "string", "maxLength": 200}},
                "quality": {"type": "string", "enum": ["rich", "sparse"]}
            },
            "required": ["aititle", "aiDesc", "metaName", "metadescription", "specs", "useCases", "quality"]
        }
    }

    schema_hint = {
        "aititle": (
            "string — Preserve model/size/power tokens from the product name; do not duplicate the brand."
        ),
        "aiDesc": (
            "string — HTML only using <p>, <ul>, <li>, <strong>, <em>, <h3>. DO NOT use <table>.\n"
            "STRUCTURE:\n"
            "1) <p>Benefit-led overview that extends supplier info. Plain English. No 'DIY'.</p>\n"
            "2) <ul>Buyer-facing bullets (max 7) that merge and de-duplicate supplier bullets; keep unique figures/claims.</ul>\n"
            "3) <p><strong>In practice:</strong> One short paragraph of buying/usage guidance. Keep light; assume deeper articles will be linked later.</p>\n"
            "4) <ul>Quick specs (5–10 bullets) with exact values.</ul>\n"
            "5) If source lists included items (e.g., 'Includes …', 'What's included'), add:\n"
            "   <p><strong>What’s in the box:</strong></p><ul>…items…</ul>\n"
            "6) Optional: <p><strong>Compatibility & fit:</strong></p> if clear dependencies (networks, voltages, shaft length, etc).\n"
            "7) Optional: <p><strong>Good to know:</strong></p> for warranty/limitations.\n"
            "8) <p><strong>Use it when:</strong></p> with 1–2 short scenarios (≤20 words each).\n"
            "RULES:\n"
            "- NEVER invent specs. Keep all figures found.\n"
            "- Convert inch/foot marks (\" and ') into ' in' / ' ft'.\n"
            "- Keep under ~350 words total.\n"
        ),
        "aBrandDesc": "string — One sentence about the brand’s relevance. (We will wrap it with <p>.)",
        "metaName": "string — ≤60 chars, include model/size/power tokens if useful; no brand duplication.",
        "metadescription": "string — 140–160 chars, factual, includes one differentiator.",
        "metakeywords": "string — short, comma-separated.",
        "specs": {"<Key>": "<Value>"},
        "useCases": ["string", "string"],
        "quality": "string — 'rich' or 'sparse'."
    }

     common_system = (
        "Goal: Rewrite the provided manufacturer, Vision Marine, and supplier text into a high-converting, emotionally engaging "
        "product description for boaters. Keep every factual detail intact (power ratings, weights, dimensions, screen types, "
        "materials, etc.), but transform dry feature lists into compelling copy that helps customers imagine how the product improves "
        "their boating life. Never invent or guess information.\n"
        "Writing Style:\n"
        "• Premium yet practical: aspirational tone merged with clear technical reassurance.\n"
        "• Informative but never dry: every line should add value to the reader.\n"
        "• Emotional + functional: highlight both benefits and the lifestyle improvements the product brings.\n"
        "• Neutral boat language: if product is specific to motor boats or sailing, keep references accurate.\n"
        "SEO Guidelines (On-Page SEO Best Practices 2025):\n"
        "• Use the exact product title (e.g., “Victron Blue Smart IP67 12V 25A Battery Charger” or “Braid on Braid Polyester Rope – 12mm”) once within the first 100–150 words of the description.\n"
        "• Include the target keyword(s) again naturally at least once more in the body text.\n"
        "• Use related keywords, synonyms, and variations relevant to the specific product type. For example:\n"
        "o For batteries: “12V lithium battery”, “LiFePO4 marine battery”, “deep cycle power storage”.\n"
        "o For electronics: “marine chartplotter”, “touchscreen display”, “GPS navigation system”.\n"
        "o For hardware: “stainless steel shackle”, “316 A4 grade”, “marine fastener”.\n"
        "o For rope: “mooring line”, “double braid polyester rope”, “marine cordage”.\n"
        "Always derive variations from the product title and product type. Never insert unrelated terms.\n"
        "• Use short, descriptive micro-headers that contain keywords where appropriate. Avoid vague headers.\n"
        "• Front-load key benefits and specifications early in the description so users and search engines quickly see what matters most.\n"
        "• Ensure the description is formatted for readability: short paragraphs, logical flow, and micro-headers for key features. Use bulleted lists only if they clearly improve comprehension (avoid over-use).\n"
        "• Always generate suggested meta title and meta description along with the product description:\n"
        "o Meta title: under 60 characters, includes the main keyword, communicates product type and brand.\n"
        "o Meta description: under 160 characters, includes the main keyword, highlights a unique benefit, encourages click-through.\n"
        "• Ensure content offers “information gain” compared to manufacturer text. Do not simply repeat feature lists. Expand on why features matter and what benefits they provide.\n"
        "• Address customer search intent by covering common concerns such as: lifespan, compatibility, reliability, safety, certifications, size, weight, and usage.\n"
        "• For images, always generate alt text suggestions: concise, descriptive, relevant, ideally including the main keyword naturally.\n"
        "• Keyword frequency should be natural. Avoid keyword stuffing, but mention the main keyword and variations several times throughout.\n"
        "• Ensure each product description is unique, original, and valuable.\n"
        "Source Priority & Use:\n"
        "• Brand source (brandcontent field if populated) = primary reference.\n"
        "• Vision Marine short description = always included; if it contains unique details, ensure these are integrated into the rewritten description.\n"
        "• Supplier source (description field passed in) = fallback for filling gaps not covered by brand or VM short description.\n"
        "• Combine these sources seamlessly, without duplication.\n"
        "• Any additional sources must be verified\n"
        "Structure:\n"
        "1. Begin with a bold, benefit-driven opening paragraph. Do not title this section “Introduction.”\n"
        "2. Present the main product features in engaging paragraphs. For important features, use short, benefit-driven micro headers (e.g., “Reliable Power Delivery,” “All-Weather Performance,” “Lightweight and Easy to Handle”). These should make the copy easy to scan but must not read like essay section titles.\n"
        "3. For each feature, describe the practical outcome or emotional payoff while also including all key technical details provided (e.g., charge/discharge limits, surge ratings, dimensions, weight, materials, certifications, cycle life, connection options). Do not omit these details, even if the description becomes longer. Present them naturally in sentences, not as a separate specs table.\n"
        "4. If “what’s in the box” information is provided, include it naturally in the flow. Never speculate or invent contents.\n"
        "5. Add a closing summary paragraph that reassures buyers about reliability and highlights the product’s key benefits. Do not title this section “Closing Points.”\n"
        "6. Always finish with an About [Brand] section. If the brand is:\n"
        "o A known manufacturer (e.g., Victron, Garmin): briefly state why the brand is respected in the marine world.\n"
        "o VM Marine (own brand): use a “trusted by us” reassurance message (e.g., “Hand-selected by our marine experts to deliver excellent value without compromise”).\n"
        "o AG / Aquafax Group: state it is part of the UK’s largest marine distributor, without embellishment.\n"
        "Do Not:\n"
        "• Use large essay-style section headers such as “Introduction,” “Main Features,” or “Closing Points.”\n"
        "• Invent features, usage scenarios, or box contents if they are not provided.\n"
        "• Pad out sparse product info — keep it concise if the source is minimal.\n"
        "Length Rules:\n"
        "• For complex products (inverters, chargers, electronics, autopilot kits): expand into detailed, flowing copy with multiple feature subsections and micro headers.\n"
        "• For simple hardware (shackles, pins, screws, rope): keep it short and direct — 1–2 concise paragraphs that highlight quality and uses.\n"
        "NLP-Optimized Writing Approach\n"
        "Create content strictly adhering to an NLP-friendly format, emphasizing clarity and simplicity in structure and language. Ensure sentences follow a straightforward subject-verb-object order, selecting words for their precision and avoiding any ambiguity. Exclude filler content, focusing on delivering information succinctly. Do not use complex or abstract terms such as 'meticulous,' 'navigating,' 'complexities,' 'realm,' 'bespoke,' 'tailored,' 'towards,' 'underpins,' 'ever-changing,' 'ever-evolving,' 'the world of,' 'not only,' 'seeking more than just,' 'designed to enhance,' 'it’s not merely,' 'our suite,' 'it is advisable,' 'daunting,' 'in the heart of,' 'when it comes to,' 'in the realm of,' 'amongst,' 'unlock the secrets,' 'unveil the secrets,' and 'robust.' This approach aims to streamline content production for enhanced NLP algorithm comprehension, ensuring the output is direct, accessible, and easily interpretable.\n"
        "While prioritizing NLP-friendly content creation (60%), also dedicate 40% of your focus to making the content engaging and enjoyable for readers, balancing technical NLP optimization with reader satisfaction to produce content that not only ranks well on search engines but is also compelling and valuable to a readership.\n"
    )


    if mode == "rich":
        system = common_system + (
            "- RICH MODE: Source includes supplier/manually improved content. Extend it, keep ALL concrete technical details. "
            "If shortdescription2 exists, you may incorporate it into the opening overview.\n"
        )
        baseContent = {
            "brandcontent": base.get("brandcontent") or "",
            "shortdescription2": short_intro,
            "longDescription": long_html,
            **{k: v for k, v in base.items() if k not in ("brandcontent", "shortdescription2", "longDescription")}
        }
    else:
        system = common_system + (
            "- SPARSE MODE: Input is minimal. Provide a concise, safe overview using only brand/name/MPN/category. "
            "Add 'Full technical specifications will be added soon.' before 'Use it when:'."
        )
        baseContent = {
            "shortdescription2": short_intro,
            "longDescription": long_html
        }

    user = {
        "mode": mode,
        "task": "Extend and structure the product content; keep technical detail; add helpful, light buyer context.",
        "brand": brand,
        "mpn": mpn,
        "title": name,
        "category": category,
        "baseContent": baseContent,
        "return_json_schema_hint": schema_hint
    }

    return mode, [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False)}
    ], json_schema

# --------------------------------------------------------------------------- #
# Fallback & JSON repair
# --------------------------------------------------------------------------- #

def _fallback(product: Dict[str, Any], reason: str, mode: str = "sparse") -> Dict[str, Any]:
    brand = product.get("brand") or ""
    name = product.get("name") or ""
    mpn = product.get("mpn") or ""
    category = product.get("category") or "marine"
    base = product.get("base") or {}
    short_intro = base.get("shortdescription2") or ""
    long_html = base.get("longDescription") or ""

    parts = []
    if mode == "rich":
        if short_intro and not _is_bad(short_intro):
            parts.append(f"<p>{_normalise_inches_feet(short_intro.strip())}</p>")
        if long_html and not _is_bad(long_html):
            # strip any tables and disallowed tags from supplier html
            cleaned = _strip_disallowed_tags(_normalise_inches_feet(long_html))
            parts.append(cleaned)
    else:
        parts.append(f"<p>{_normalise_inches_feet(f'{brand} {name} ({mpn}) for {category} applications.').strip()}</p>")
        parts.append("<p>Full technical specifications will be added soon.</p>")

    parts.append("<p><strong>Use it when:</strong> General onboard upgrades; off-grid and shore-power transitions.</p>")
    desc = _ensure_html_structure("\n".join(parts))

    print(f"{FALLBACK_PREFIX} {reason}")
    return {
        "_source": "fallback",
        "_error": reason,
        "aititle": _normalize_title(brand, name, f"{brand} {name}".strip()),
        "aiDesc": desc,
        "aBrandDesc": f"<p>{brand} is a trusted name in marine systems.</p>" if brand else "",
        "metaName": (f"{brand} {name}"[:60]).strip(),
        "metadescription": f"Buy {brand} {name} ({mpn}) for {category} systems. UK support."[:160],
        "metakeywords": ",".join(filter(None, [brand, name, mpn, "marine"])),
        "specs": {},
        "useCases": [
            "Onboard power upgrades",
            "Off-grid installs",
            "Generator/shore power backup"
        ],
        "quality": mode
    }

def _repair_and_load_json(s: str) -> Dict[str, Any]:
    """
    Coerce common non-JSON responses into JSON:
    - Strip Markdown code fences
    - Clamp to first '{'..last '}' if present
    """
    if not isinstance(s, str):
        raise ValueError("Non-string JSON content")
    s2 = re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$", "", s, flags=re.IGNORECASE | re.DOTALL)
    start = s2.find("{")
    end = s2.rfind("}")
    if start != -1 and end != -1 and end > start:
        s2 = s2[start:end + 1]
    return json.loads(s2)

def _dump_raw(content: str, raw_dump_dir: Optional[str], tag: str) -> None:
    if not raw_dump_dir:
        return
    try:
        p = Path(raw_dump_dir)
        p.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        fpath = p / f"raw_{tag}_{ts}.txt"
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content if isinstance(content, str) else repr(content))
    except Exception:
        pass

# --------------------------------------------------------------------------- #
# Enrichment
# --------------------------------------------------------------------------- #

def enrich_product(product: Dict[str, Any]) -> Dict[str, Any]:
    # 1) SDK present?
    if not _HAS_OPENAI:
        return _fallback(product, f"OpenAI SDK not available ({_SDK_IMPORT_ERROR})")

    # 2) Key present?
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return _fallback(product, "OPENAI_API_KEY is not set")

    raw_dump_dir = os.getenv("RAW_DUMP_DIR") or None

    # 3) Call API with strict JSON schema; retry once if parsing fails
    try:
        client = OpenAI()  # reads OPENAI_API_KEY from env
        mode, messages, json_schema = _build_messages(product)

        def _call_and_parse(strict_schema: bool):
            resp = client.chat.completions.create(
                model=MODEL_NAME,
                messages=messages,
                temperature=0.2,
                max_tokens=3200,  # more room for richer content
                response_format=(
                    {"type": "json_schema", "json_schema": json_schema}
                    if strict_schema else
                    {"type": "json_object"}
                )
            )
            print(API_PREFIX)
            content = resp.choices[0].message.content
            try:
                return json.loads(content)
            except Exception:
                _dump_raw(content, raw_dump_dir, "parse_fail")
                return _repair_and_load_json(content)

        # Prefer strict; fall back to json_object + repair if needed
        try:
            data = _call_and_parse(strict_schema=True)
        except Exception as e1:
            try:
                data = _call_and_parse(strict_schema=False)
            except Exception as e2:
                raise RuntimeError(f"JSON parse failure after retries: {e1} | {e2}")

        # Normalise required fields
        if "specs" not in data or not isinstance(data["specs"], dict):
            data["specs"] = {}
        if "useCases" not in data or not isinstance(data["useCases"], list):
            data["useCases"] = []
        if "quality" not in data or data["quality"] not in ("rich", "sparse"):
            data["quality"] = mode

        # De-duplicate brand in the title but KEEP technical tokens
        brand = product.get("brand") or ""
        pname = product.get("name") or ""
        if isinstance(data.get("aititle"), str):
            data["aititle"] = _normalize_title(brand, pname, data["aititle"])

        # Normalise inches/feet in aiDesc and specs to avoid fragile quotes
        if isinstance(data.get("aiDesc"), str):
            data["aiDesc"] = _normalise_inches_feet(data["aiDesc"])
            data["aiDesc"] = _ensure_html_structure(data["aiDesc"])  # enforce allowed HTML structure
        specs2 = {}
        for k, v in (data.get("specs") or {}).items():
            specs2[str(k)] = _normalise_inches_feet(str(v))
        data["specs"] = specs2

        # If rich and intro exists, prepend refined intro if missing
        base = product.get("base") or {}
        short_intro = base.get("shortdescription2") or ""
        if data["quality"] == "rich" and short_intro and isinstance(data.get("aiDesc"), str):
            first_160 = data["aiDesc"][:160].lower()
            if short_intro[:40].lower() not in first_160:
                pre = _ensure_html_structure(f"<p>{_normalise_inches_feet(short_intro.strip())}</p>")
                data["aiDesc"] = pre + "\n" + data["aiDesc"]

        # Wrap brand blurb as HTML paragraph if present
        if isinstance(data.get("aBrandDesc"), str) and data["aBrandDesc"].strip():
            ab = _normalise_inches_feet(data["aBrandDesc"].strip())
            if not re.search(r"^\s*<p>.*</p>\s*$", ab, flags=re.IGNORECASE | re.DOTALL):
                ab = f"<p>{ab}</p>"
            data["aBrandDesc"] = _strip_disallowed_tags(ab)

        # Enforce caps
        if isinstance(data.get("metaName"), str):
            data["metaName"] = data["metaName"][:60]
        if isinstance(data.get("metadescription"), str):
            data["metadescription"] = data["metadescription"][:160]
        if isinstance(data.get("aiDesc"), str) and len(data["aiDesc"]) > 5000:
            data["aiDesc"] = data["aiDesc"][:5000]

        data["_source"] = "openai"
        return data

    except Exception as e:
        # Show the exact API failure reason; choose mode based on base content
        mode = "sparse"
        base = product.get("base") or {}
        if not _is_bad(base.get("shortdescription2") or "") or not _is_bad(base.get("longDescription") or ""):
            mode = "rich"
        return _fallback(product, f"API error: {type(e).__name__}: {e}", mode=mode)
