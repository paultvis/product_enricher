# enricher.py
# v2025-10-05
# AI PDP/Crawler – Product Enricher
#
# What this does:
# - Builds a structured prompt with explicit source precedence:
#   brandcontent > vmshortdescription|shortdescription2 > long_description|longDescription > supplier/other
# - Instructs the model to generate paragraph-first copy with <h3> micro-headers.
# - Returns a strict JSON object with new fields:
#   - aboutBrandHtml, whyBuyFromUsHtml, imageAltTexts, whatsInTheBox, quickSpecs,
#     primaryKeyword, secondaryKeywords (plus existing fields like aiDesc, aititle, meta*, specs, etc.)
# - Repairs/normalizes model output to valid JSON.
# - Sanitizes HTML tags and normalizes quotes/units.
# - Falls back to a safe paragraph-first render if the API is unavailable.
#
# Env:
#   OPENAI_API_KEY   : your API key
#   ENRICH_MODEL     : (optional) model name, default "gpt-4.1"
#   ENRICH_TEMPERATURE : (optional) float, default "0.4"
#
# Usage:
#   result = enrich_product(product_dict)
#   # result contains aiDesc, aititle, aboutBrandHtml, whyBuyFromUsHtml, metaName, metadescription, etc.

from __future__ import annotations

import json
import os
import re
import html
from typing import Any, Dict, List, Optional

# ---------- Optional OpenAI client ----------
_OPENAI_AVAILABLE = True
try:
    # Try the new SDK import first (2024+)
    from openai import OpenAI
    _openai_client_kind = "sdk_v1"
except Exception:
    try:
        # Fallback to legacy
        import openai  # type: ignore
        _openai_client_kind = "legacy"
    except Exception:
        _OPENAI_AVAILABLE = False
        _openai_client_kind = "none"


# ---------- Helpers: HTML sanitation & normalization ----------

_ALLOWED_TAGS = {
    "p", "h3", "ul", "ol", "li", "strong", "em", "b", "i", "br", "a"
}
_ALLOWED_ATTRS = {
    "a": {"href", "title", "rel", "target"},
}

def _strip_disallowed_tags(html_text: str) -> str:
    """
    Very light HTML sanitizer:
    - Keeps only whitelisted tags.
    - Removes all attributes except allowlisted ones for <a>.
    """
    # Normalize common uppercase tags
    text = re.sub(r"</?([A-Z]+)", lambda m: m.group(0).lower(), html_text)

    # Remove scripts/styles entirely
    text = re.sub(r"<\s*(script|style)[^>]*>.*?<\s*/\s*\1\s*>", "", text, flags=re.S | re.I)

    def _sanitize_tag(m: re.Match) -> str:
        slash = m.group(1) or ""
        tag = (m.group(2) or "").lower()
        attrs = m.group(3) or ""
        # Self-closing not needed here; keep simple
        if tag not in _ALLOWED_TAGS:
            return "" if slash == "" else ""
        if tag == "a":
            # Keep only allowed attributes for <a>
            kept = []
            for attr, val in re.findall(r'([a-zA-Z_:.-]+)\s*=\s*(".*?"|\'.*?\'|[^\s>]+)', attrs):
                if attr.lower() in _ALLOWED_ATTRS["a"]:
                    kept.append(f'{attr}={val}')
            attrs_str = (" " + " ".join(kept)) if kept else ""
            return f"<{slash}{tag}{attrs_str}>"
        else:
            # Strip attributes for all other tags
            return f"<{slash}{tag}>"

    # Open tags
    text = re.sub(r"<\s*(\/?)([a-zA-Z0-9]+)([^>]*)>", _sanitize_tag, text)
    return text


def _normalize_quotes_units(text: str) -> str:
    """
    - Convert smart quotes to straight quotes
    - Keep simple inch/foot markers readable
    - Normalize whitespace
    """
    if not text:
        return text
    replacements = {
        "“": '"', "”": '"', "„": '"', "«": '"', "»": '"',
        "‘": "'", "’": "'", "‚": "'", "′": "'", "″": '"',
        "–": "-", "—": "-", "−": "-", " ": " ",
    }
    for k, v in replacements.items():
        text = text.replace(k, v)

    # Normalize ft/in spacing like 6" or 6' → keep as-is but remove stray spaces
    text = re.sub(r"\s+(['\"])", r"\1", text)

    # Collapse excessive whitespace
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _sanitize_html_block(html_text: Optional[str]) -> str:
    if not html_text:
        return ""
    text = _normalize_quotes_units(str(html_text))
    # Unescape any double-encoded HTML once
    text = html.unescape(text)
    text = _strip_disallowed_tags(text)
    return text


# ---------- Prompt construction ----------

def _build_common_system() -> str:
    return _normalize_quotes_units("""
You are an expert marine copywriter and product data editor.

GOAL
Rewrite the provided manufacturer (brand), Vision Marine (retailer), and supplier inputs into a high-converting, emotionally engaging product description for boaters. Preserve every factual detail (power ratings, weights, dimensions, screen types, materials, certifications, cycle life, connection options). Never invent or guess information.

STYLE
- Premium yet practical; expert, reassuring; no hype.
- Informative but never dry: every line should add value.
- Emotional + functional: benefits + real technical outcomes.
- Neutral boat language; if product is specific (motor/sail), keep references accurate.
- Avoid filler phrases like: “Good to know”, “Use it when”, “In practice”, unless sources explicitly provide them.

SEO
- Use the exact product title once in the FIRST paragraph (within first 100–150 words), not as a header.
- Naturally re-use target keyword(s) once or twice later.
- Use related terms derived from the product class/type where natural.
- Use concise micro-headers (<h3>) that aid scanning and, where appropriate, contain keywords.
- Front-load key benefits and critical specifications early.
- Output meta title (<60 chars) and meta description (<160 chars).

SOURCE PRECEDENCE (strict)
1) brandcontent (authoritative)
2) vmshortdescription OR shortdescription2 (Vision Marine short intro)
3) long_description OR longDescription (retailer/supplier depth)
4) supplier or other provided content
- Merge without duplication. If a detail appears in multiple sources, keep the best phrasing once.
- If “what’s in the box” is present, include it; never speculate.

STRUCTURE (HTML)
1) Opening paragraph (benefit-led; includes exact product title once).
2) 2–5 sections using <h3> micro-headers; each with 1–2 short paragraphs. Weave critical specs INTO sentences (no spec table).
3) Optional “Quick specs” as <ul> (5–10 bullets) ONLY if it improves scannability for complex items.
4) Optional “What’s in the box” as <ul> ONLY if explicitly provided.
5) “Why buy from Vision Marine” (1–2 short paragraphs) derived from content_footer; reassuring, specific; no hype or promises beyond provided info.
6) “About [Brand]” (1 paragraph). If brand is a known manufacturer (e.g., Victron, Garmin), state why they’re respected in the marine world. If VM Marine (own brand), use a trusted-by-us reassurance. If AG/Aquafax Group, state it is part of the UK’s largest marine distributor (no embellishment).

HTML RULES
- Allowed tags: p, h3, ul, ol, li, strong, em, b, i, br, a.
- No tables, no h1/h2/h4+, no inline styles, no images.
- No large essay-style headers like “Introduction” or “Closing Points”.

OUTPUT
Return a strict single JSON object with the schema provided. Do NOT include any text outside JSON. Do NOT include markdown fences.
""")


def _build_schema_hint() -> Dict[str, Any]:
    # This “hint” is also shown to the model to reinforce structure/shape.
    return {
        "aititle": "Title-cased, brand-normalized product name.",
        "aiDesc": (
            "HTML body following STRUCTURE:\n"
            " - Opening paragraph (benefit-led; includes exact product title once)\n"
            " - 2–5 <h3> sections, each 1–2 short paragraphs with specs woven into sentences\n"
            " - Optional <ul> Quick specs (5–10 bullets) if helpful\n"
            " - Optional <ul> What’s in the box if provided\n"
            " - 'Why buy from Vision Marine' (short paragraphs from content_footer)\n"
            " - 'About [Brand]' (1 paragraph)\n"
            "Allowed tags only. No tables."
        ),
        "aboutBrandHtml": "One paragraph about the brand.",
        "whyBuyFromUsHtml": "One or two short paragraphs derived from content_footer.",
        "imageAltTexts": ["Up to 10 concise alt texts; main keyword used naturally."],
        "whatsInTheBox": ["Only if explicitly provided by sources."],
        "quickSpecs": ["Short 'Label: Value' bullets for UI tiles (optional)."],
        "metaName": "<= 60 chars; brand + product type + key attribute.",
        "metadescription": "<= 160 chars; includes main keyword + unique benefit.",
        "metakeywords": "Comma-separated or array; keep relevant; avoid stuffing.",
        "primaryKeyword": "Primary targeted keyword for logging/QA.",
        "secondaryKeywords": ["A few natural secondary keywords."],
        "specs": {"...": "Spec key-value pairs if consolidation is needed."},
        "useCases": ["Optional concise use-cases only if supported by sources."],
        "quality": "Short quality/reliability summary if supported; no invention.",
    }


def _extract_sources(product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Builds baseContent sent to the model. Includes Atro + crawler fields with explicit precedence.
    """
    base = dict(product.get("base", {}))  # shallow copy
    # Normalize possible keys from different loaders
    def pick(*keys):
        for k in keys:
            if k in product and product[k]:
                return product[k]
            if k in base and base[k]:
                return base[k]
        return None

    content = {
        "brandcontent": pick("brandcontent", "brand_content", "brandContent"),
        "vmshortdescription": pick("vmshortdescription", "shortdescription2", "vm_short_description"),
        "shortdescription2": pick("shortdescription2"),
        "content_header": pick("content_header", "contentHeader"),
        "content_footer": pick("content_footer", "contentFooter"),
        "brand_description": pick("brand_description", "brandDescription"),
        "reviewimages": pick("reviewimages", "reviewImages"),
        "long_description": pick("long_description", "longDescription"),
        "crawler_specs": pick("specs", "crawler_specs"),
        "title": pick("title", "name", "product_title"),
        "brand": pick("brand", "manufacturer"),
        "mpn": pick("mpn", "part_number", "partNumber"),
        "sku": pick("sku", "SKU"),
        "category": pick("category"),
    }

    # Also pass everything else in base for context (without overwriting the explicit keys)
    for k, v in base.items():
        if k not in content:
            content[k] = v

    return content


def _build_messages(product: Dict[str, Any]) -> List[Dict[str, str]]:
    system = _build_common_system()
    base_content = _extract_sources(product)
    schema_hint = _build_schema_hint()
    user_payload = {
        "schema": {
            "type": "object",
            "properties": {
                "aititle": {"type": "string"},
                "aiDesc": {"type": "string"},
                "aboutBrandHtml": {"type": "string"},
                "whyBuyFromUsHtml": {"type": "string"},
                "imageAltTexts": {"type": "array", "items": {"type": "string"}},
                "whatsInTheBox": {"type": "array", "items": {"type": "string"}},
                "quickSpecs": {"type": "array", "items": {"type": "string"}},
                "metaName": {"type": "string"},
                "metadescription": {"type": "string"},
                "metakeywords": {"type": ["string", "array"]},
                "primaryKeyword": {"type": "string"},
                "secondaryKeywords": {"type": "array", "items": {"type": "string"}},
                "specs": {"type": ["object", "array", "null"]},
                "useCases": {"type": ["array", "null"], "items": {"type": "string"}},
                "quality": {"type": ["string", "null"]},
                "_source": {"type": "string"},
            },
            "required": ["aititle", "aiDesc", "metaName", "metadescription"],
            "additionalProperties": True
        },
        "schema_hint": schema_hint,
        "baseContent": base_content,
        "instructions": "Return ONLY a single JSON object. No markdown fences. No commentary.",
    }

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]
    return messages


# ---------- OpenAI call & repair path ----------

def _call_openai(messages: List[Dict[str, str]]) -> Optional[Dict[str, Any]]:
    if not _OPENAI_AVAILABLE:
        return None

    temperature = float(os.getenv("ENRICH_TEMPERATURE", "0.4"))
    model = os.getenv("ENRICH_MODEL", "gpt-4.1")

    try:
        if _openai_client_kind == "sdk_v1":
            client = OpenAI()
            # Prefer JSON-mode if supported
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                response_format={"type": "json_object"},
            )
            content = resp.choices[0].message.content
        elif _openai_client_kind == "legacy":
            openai.api_key = os.getenv("OPENAI_API_KEY")
            resp = openai.ChatCompletion.create(  # type: ignore
                model=model,
                messages=messages,
                temperature=temperature,
            )
            content = resp["choices"][0]["message"]["content"]
        else:
            return None

        # Try parse
        try:
            return json.loads(content)
        except Exception:
            # Attempt to extract JSON substring
            match = re.search(r"\{.*\}", content, flags=re.S)
            if match:
                try:
                    return json.loads(match.group(0))
                except Exception:
                    pass

            # Second attempt: ask model to repair into strict JSON
            if _openai_client_kind == "sdk_v1":
                repair = OpenAI().chat.completions.create(
                    model=model,
                    temperature=0,
                    messages=[
                        {"role": "system", "content": "You repair JSON. Return only valid JSON."},
                        {"role": "user", "content": f"Fix this into strict JSON only:\n{content}"},
                    ],
                    response_format={"type": "json_object"},
                )
                rep = repair.choices[0].message.content
            elif _openai_client_kind == "legacy":
                rep = openai.ChatCompletion.create(  # type: ignore
                    model=model,
                    temperature=0,
                    messages=[
                        {"role": "system", "content": "You repair JSON. Return only valid JSON."},
                        {"role": "user", "content": f"Fix this into strict JSON only:\n{content}"},
                    ],
                )["choices"][0]["message"]["content"]
            else:
                rep = None

            if rep:
                try:
                    return json.loads(rep)
                except Exception:
                    return None
    except Exception:
        return None


# ---------- Fallback rendering (no API) ----------

def _fallback_render(base: Dict[str, Any]) -> Dict[str, Any]:
    title = base.get("title") or ""
    brand = base.get("brand") or ""
    content_footer = base.get("content_footer") or ""
    brand_desc = base.get("brand_description") or ""
    vmshort = base.get("vmshortdescription") or base.get("shortdescription2") or ""
    longd = base.get("long_description") or ""
    # Build a simple paragraph-first layout
    opening = []
    if title:
        opening.append(f"{title} delivers dependable performance for modern boating. ")
    if vmshort and vmshort.strip():
        opening.append(vmshort.strip())
    elif longd and isinstance(longd, str):
        opening.append(longd.strip()[:300])

    aiDesc = []
    if opening:
        aiDesc.append(f"<p>{html.escape(' '.join(opening))}</p>")

    # A couple of micro-sections
    aiDesc.append("<h3>Reliable, Practical Power</h3>")
    aiDesc.append("<p>Engineered for consistent output and smooth operation, with straightforward installation and day-to-day ease of use. Designed to handle typical marine demands without fuss.</p>")

    aiDesc.append("<h3>Built for Marine Environments</h3>")
    aiDesc.append("<p>Materials and design choices prioritise durability and safety on the water. Pair with correctly sized cables, fusing, and isolation for a clean, safe system.</p>")

    # Why buy from us
    why = content_footer.strip()
    if why:
        aiDesc.append("<h3>Why buy from Vision Marine</h3>")
        aiDesc.append(f"<p>{html.escape(why)[:800]}</p>")

    # About brand
    if brand:
        about = brand_desc.strip() or f"{brand} is a trusted name in marine systems and electronics, valued for dependable performance and long service life."
        aiDesc.append(f"<h3>About {html.escape(brand)}</h3>")
        aiDesc.append(f"<p>{html.escape(about)[:600]}</p>")

    body = "\n".join(aiDesc)
    body = _sanitize_html_block(body)

    meta_title = (title or f"{brand} Marine Product").strip()
    if len(meta_title) > 60:
        meta_title = meta_title[:57].rstrip() + "…"

    meta_desc = "Reliable marine-grade performance with expert UK support from Vision Marine."
    if len(meta_desc) > 160:
        meta_desc = meta_desc[:157].rstrip() + "…"

    return {
        "aititle": title or (brand + " Marine Product").strip(),
        "aiDesc": body,
        "aboutBrandHtml": _sanitize_html_block(brand_desc),
        "whyBuyFromUsHtml": _sanitize_html_block(content_footer),
        "imageAltTexts": [],
        "whatsInTheBox": [],
        "quickSpecs": [],
        "metaName": meta_title,
        "metadescription": meta_desc,
        "metakeywords": [],
        "primaryKeyword": "",
        "secondaryKeywords": [],
        "specs": base.get("crawler_specs") or base.get("specs") or {},
        "useCases": None,
        "quality": None,
        "_source": "fallback",
    }


# ---------- Public API ----------

def enrich_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entry point. Returns a dict with keys like:
      aiDesc, aititle, aboutBrandHtml, whyBuyFromUsHtml, imageAltTexts, whatsInTheBox,
      quickSpecs, metaName, metadescription, metakeywords, primaryKeyword, secondaryKeywords, specs, useCases, quality, _source
    """
    messages = _build_messages(product)
    raw = _call_openai(messages)

    if not raw or not isinstance(raw, dict):
        # No API or parsing failed → fallback
        base = _extract_sources(product)
        return _fallback_render(base)

    # Normalize/clean expected fields
    aititle = str(raw.get("aititle", "")).strip()
    aiDesc = _sanitize_html_block(raw.get("aiDesc", ""))
    aboutBrandHtml = _sanitize_html_block(raw.get("aboutBrandHtml", raw.get("aBrandDesc", "")))
    whyBuyFromUsHtml = _sanitize_html_block(raw.get("whyBuyFromUsHtml", raw.get("content_footer", "")))

    # Optional lists
    def _as_str_list(x) -> List[str]:
        if isinstance(x, list):
            return [str(i).strip() for i in x if str(i).strip()]
        if isinstance(x, str) and x.strip():
            # Support comma-separated strings
            parts = [p.strip() for p in x.split(",")]
            return [p for p in parts if p]
        return []

    imageAltTexts = _as_str_list(raw.get("imageAltTexts", []))
    whatsInTheBox = _as_str_list(raw.get("whatsInTheBox", []))
    quickSpecs = _as_str_list(raw.get("quickSpecs", []))
    metakeywords = raw.get("metakeywords", [])
    if isinstance(metakeywords, str):
        metakeywords = _as_str_list(metakeywords)

    metaName = str(raw.get("metaName", "")).strip()
    if len(metaName) > 60:
        metaName = metaName[:57].rstrip() + "…"
    metadescription = str(raw.get("metadescription", "")).strip()
    if len(metadescription) > 160:
        metadescription = metadescription[:157].rstrip() + "…"

    # Carry through specs and others as-is, but normalize simple types
    specs = raw.get("specs", {})
    useCases = raw.get("useCases", None)
    quality = raw.get("quality", None)
    primaryKeyword = str(raw.get("primaryKeyword", "")).strip()
    secondaryKeywords = _as_str_list(raw.get("secondaryKeywords", []))

    result = {
        "aititle": aititle,
        "aiDesc": aiDesc,
        "aboutBrandHtml": aboutBrandHtml,
        "whyBuyFromUsHtml": whyBuyFromUsHtml,
        "imageAltTexts": imageAltTexts,
        "whatsInTheBox": whatsInTheBox,
        "quickSpecs": quickSpecs,
        "metaName": metaName,
        "metadescription": metadescription,
        "metakeywords": metakeywords,
        "primaryKeyword": primaryKeyword,
        "secondaryKeywords": secondaryKeywords,
        "specs": specs,
        "useCases": useCases,
        "quality": quality,
        "_source": "openai",
    }

    # Final normalization pass on HTML blocks (just in case)
    for k in ("aiDesc", "aboutBrandHtml", "whyBuyFromUsHtml"):
        result[k] = _sanitize_html_block(result[k])

    return result


# ---------- If you want to quick-test locally ----------
if __name__ == "__main__":
    demo_product = {
        "base": {
            "title": "Victron Energy MultiPlus-II 48/3000/35-32 (PMP482305010)",
            "brand": "Victron Energy",
            "mpn": "PMP482305010",
            "brandcontent": "Victron Energy is respected worldwide for robust, efficient power electronics designed for demanding applications.",
            "vmshortdescription": "Serious 48 V performance in one tidy box. The MultiPlus-II 48/3000/35-32 combines a sine-wave inverter, a settable 35 A charger, and a 32 A transfer switch for smooth power delivery.",
            "content_footer": "Deep DC experience from marine and van installs—safety first. Accurate stock, realistic delivery, and fast technical support. Thousands of units supplied with the right cables, fuses, and accessories.",
            "brand_description": "Victron is widely trusted for reliable off-grid and marine power systems with excellent integration across inverters, chargers, and monitoring.",
            "long_description": "PowerAssist helps prevent breaker trips. Works in Victron ESS layouts. Fast UPS-grade switchover protects sensitive loads.",
            "specs": {
                "Battery voltage": "48 V",
                "Inverter continuous": "2400 W (3000 VA)",
                "Charger current": "35 A",
                "Transfer switch": "32 A",
            },
        }
    }
    enriched = enrich_product(demo_product)
    print(json.dumps(enriched, indent=2, ensure_ascii=False))
