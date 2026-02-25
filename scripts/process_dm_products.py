#!/usr/bin/env python3
import json
import math
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


INPUT_PATH = Path("dm_products_all.jl")
OUTPUT_DIR = Path("outputs")
CSV_PATH = OUTPUT_DIR / "dm_products_reference.csv"
SQLITE_PATH = OUTPUT_DIR / "dm_products_cleaned.sqlite"
PROFILE_PATH = OUTPUT_DIR / "dm_products_profile.md"


DESCRIPTION_HEADER_MAP = {
    "Produktbeschreibung": "desc_productbeschreibung",
    "Produktmerkmale": "desc_produktmerkmale",
    "Verwendungshinweise": "desc_verwendungshinweise",
    "Inhaltsstoffe": "desc_inhaltsstoffe",
    "Aufbewahrungshinweise": "desc_aufbewahrungshinweise",
    "Warnhinweise": "desc_warnhinweise",
    "Hergestellt in": "desc_hergestellt_in",
    "Pflichthinweise": "desc_pflichthinweise",
    "Infos zur Nachhaltigkeit des Produktes": "desc_nachhaltigkeit",
    "Material": "desc_material",
    "Zutaten": "desc_zutaten",
    "Nährwerte": "desc_naehrwerte",
    "Allergene": "desc_allergene",
    "Lieferumfang": "desc_lieferumfang",
}


def text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace("\xa0", " ").strip()
        return value or None
    return str(value)


def join_texts(items: list[str], sep: str = " | ") -> str | None:
    cleaned = []
    seen = set()
    for item in items:
        s = text_or_none(item)
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        cleaned.append(s)
    return sep.join(cleaned) if cleaned else None


def parse_eur(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    s = text_or_none(value)
    if not s:
        return None
    s = s.replace("€", "").replace("EUR", "").replace(" ", "")
    # German decimal format: 1.234,56
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def parse_int_from_text(value: Any) -> int | None:
    s = text_or_none(value)
    if not s:
        return None
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None


def extract_current_price(price_node: Any) -> float | None:
    if not isinstance(price_node, dict):
        return None
    raw = (((price_node.get("price") or {}).get("current") or {}).get("value"))
    return parse_eur(raw)


def parse_not_increased_since(text: Any) -> str | None:
    s = text_or_none(text)
    if not s:
        return None
    m = re.search(r"(\d{2}\.\d{2}\.\d{4})", s)
    if not m:
        return None
    dt = pd.to_datetime(m.group(1), format="%d.%m.%Y", errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date().isoformat()


def normalize_gtin(value: Any) -> str | None:
    s = text_or_none(value)
    if not s:
        return None
    digits = re.sub(r"\D+", "", s)
    return digits or None


def flatten_content_block(block: dict[str, Any], collector: list[str]) -> None:
    if not isinstance(block, dict):
        return
    for key in ("texts", "bulletpoints"):
        values = block.get(key)
        if isinstance(values, list):
            for v in values:
                t = text_or_none(v)
                if t:
                    collector.append(t)
    links = block.get("links")
    if isinstance(links, list):
        for link in links:
            if isinstance(link, dict):
                for k in ("linkText", "href"):
                    t = text_or_none(link.get(k))
                    if t:
                        collector.append(t)
    dlist = block.get("descriptionList")
    if isinstance(dlist, list):
        for item in dlist:
            if isinstance(item, dict):
                title = text_or_none(item.get("title"))
                desc = text_or_none(item.get("description"))
                if title and desc:
                    collector.append(f"{title}: {desc}")
                elif title:
                    collector.append(title)
                elif desc:
                    collector.append(desc)
    table = block.get("table")
    if isinstance(table, list):
        rows = []
        for row in table:
            if isinstance(row, list):
                rows.append(" / ".join(text_or_none(x) or "" for x in row))
        if rows:
            collector.append(" || ".join(rows))


def parse_description_groups(groups: Any) -> tuple[list[str], dict[str, str | None]]:
    headers: list[str] = []
    extracted: dict[str, str | None] = {v: None for v in DESCRIPTION_HEADER_MAP.values()}
    if not isinstance(groups, list):
        return headers, extracted
    for group in groups:
        if not isinstance(group, dict):
            continue
        header = text_or_none(group.get("header"))
        if not header:
            continue
        headers.append(header)
        key = DESCRIPTION_HEADER_MAP.get(header)
        if not key:
            continue
        texts: list[str] = []
        for block in group.get("contentBlock") or []:
            flatten_content_block(block, texts)
        extracted[key] = join_texts(texts, sep="\n")
    return headers, extracted


def parse_unit_info(price_infos: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "unit_quantity": None,
        "unit_quantity_unit": None,
        "unit_price_per_quantity": None,
        "unit_price_per_unit": None,
        "unit_info_raw": None,
    }
    if not isinstance(price_infos, list):
        return out
    info_strings = [text_or_none(x) for x in price_infos if text_or_none(x)]
    out["unit_info_raw"] = join_texts(info_strings)
    for s in info_strings:
        # Example: 0,1 l (6,50 € je 1 l)
        m = re.search(
            r"^\s*([0-9]+(?:[.,][0-9]+)?)\s*([A-Za-z]+)\s*\(([^)]*?)\s*je\s*([0-9]+(?:[.,][0-9]+)?)\s*([A-Za-z]+)\s*\)\s*$",
            s,
        )
        if not m:
            continue
        qty, qty_unit, unit_price_str, per_qty, per_unit = m.groups()
        out["unit_quantity"] = float(qty.replace(",", "."))
        out["unit_quantity_unit"] = qty_unit
        out["unit_price_per_quantity"] = float(per_qty.replace(",", "."))
        out["unit_price_per_unit"] = per_unit
        out["unit_price_eur"] = parse_eur(unit_price_str)
        break
    out.setdefault("unit_price_eur", None)
    return out


def parse_row(raw: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    product = raw.get("product") if isinstance(raw.get("product"), dict) else {}
    p_meta = product.get("metadata") if isinstance(product.get("metadata"), dict) else {}
    p_title = product.get("title") if isinstance(product.get("title"), dict) else {}
    p_brand = product.get("brand") if isinstance(product.get("brand"), dict) else {}
    p_rating = product.get("rating") if isinstance(product.get("rating"), dict) else {}
    p_price = product.get("price") if isinstance(product.get("price"), dict) else {}
    p_net_price = product.get("netPrice") if isinstance(product.get("netPrice"), dict) else {}
    p_seo = (
        ((product.get("seoInformation") or {}).get("structuredData"))
        if isinstance(product.get("seoInformation"), dict)
        else {}
    )
    if not isinstance(p_seo, dict):
        p_seo = {}
    breadcrumbs = product.get("breadcrumbs") if isinstance(product.get("breadcrumbs"), list) else []
    description_headers, description_cols = parse_description_groups(product.get("descriptionGroups"))
    price_infos = p_price.get("infos") if isinstance(p_price, dict) else None
    unit_info = parse_unit_info(price_infos)

    gross_not_inc = None
    net_not_inc = None
    if isinstance(p_price.get("notIncreasedSince"), dict):
        gross_not_inc = parse_not_increased_since(p_price["notIncreasedSince"].get("text"))
    if isinstance(p_net_price.get("notIncreasedSince"), dict):
        net_not_inc = parse_not_increased_since(p_net_price["notIncreasedSince"].get("text"))

    eyecatchers = product.get("eyecatchers") if isinstance(product.get("eyecatchers"), list) else []
    eyecatcher_labels: list[str] = []
    for item in eyecatchers:
        if isinstance(item, dict):
            for key in ("text", "label", "alt"):
                t = text_or_none(item.get(key))
                if t:
                    eyecatcher_labels.append(t)
        elif isinstance(item, str):
            t = text_or_none(item)
            if t:
                eyecatcher_labels.append(t)

    pills = product.get("pills") if isinstance(product.get("pills"), list) else []
    pill_labels: list[str] = []
    for item in pills:
        if isinstance(item, dict):
            for key in ("text", "label"):
                t = text_or_none(item.get(key))
                if t:
                    pill_labels.append(t)
        elif isinstance(item, str):
            t = text_or_none(item)
            if t:
                pill_labels.append(t)

    row: dict[str, Any] = {
        "gtin": normalize_gtin(raw.get("gtin")),
        "dan": pd.to_numeric(raw.get("dan"), errors="coerce"),
        "product_url": text_or_none(raw.get("product_url")),
        "detail_api_url": text_or_none(raw.get("detail_api_url")),
        "slug": text_or_none(raw.get("slug")),
        "scraped_at_utc": text_or_none(raw.get("scraped_at_utc")),
        "name": text_or_none(raw.get("name")),
        "brand": text_or_none(raw.get("brand")),
        "available_raw": raw.get("available"),
        "price_raw": text_or_none(raw.get("price")),
        "price_eur_top": parse_eur(raw.get("price")),
        "product_gtin": normalize_gtin(product.get("gtin")),
        "product_dan": pd.to_numeric(product.get("dan"), errors="coerce"),
        "product_self_slug": text_or_none(product.get("self")),
        "product_is_pharmacy": product.get("isPharmacy") if isinstance(product.get("isPharmacy"), bool) else None,
        "show_cbm_web": product.get("showConfidenceBuildingMeasuresWeb")
        if isinstance(product.get("showConfidenceBuildingMeasuresWeb"), bool)
        else None,
        "show_cbm_app": product.get("showConfidenceBuildingMeasuresApp")
        if isinstance(product.get("showConfidenceBuildingMeasuresApp"), bool)
        else None,
        "brand_product_name": text_or_none(p_brand.get("name")),
        "title_headline": text_or_none(p_title.get("headline")),
        "title_subheadline": text_or_none(p_title.get("subheadline")),
        "a11y_label": text_or_none(product.get("a11yLabel")),
        "breadcrumbs_count": len(breadcrumbs),
        "breadcrumb_1": text_or_none(breadcrumbs[0]) if len(breadcrumbs) > 0 else None,
        "breadcrumb_2": text_or_none(breadcrumbs[1]) if len(breadcrumbs) > 1 else None,
        "breadcrumb_3": text_or_none(breadcrumbs[2]) if len(breadcrumbs) > 2 else None,
        "breadcrumb_4": text_or_none(breadcrumbs[3]) if len(breadcrumbs) > 3 else None,
        "breadcrumbs_path": join_texts([text_or_none(x) or "" for x in breadcrumbs], sep=" > "),
        "rating_count": pd.to_numeric(p_rating.get("ratingCount"), errors="coerce"),
        "rating_value": pd.to_numeric(p_rating.get("ratingValue"), errors="coerce"),
        "metadata_canonical": text_or_none(p_meta.get("canonical")),
        "metadata_currency": text_or_none(p_meta.get("currency")),
        "metadata_price_eur": parse_eur(p_meta.get("price")),
        "metadata_page_title": text_or_none(p_meta.get("pageTitle")),
        "metadata_is_pharmacy": p_meta.get("isPharmacy") if isinstance(p_meta.get("isPharmacy"), bool) else None,
        "metadata_category_codes": join_texts(
            [text_or_none(x) or "" for x in (p_meta.get("categoryCodes") or [])], sep="|"
        ),
        "metadata_description": text_or_none(p_meta.get("description")),
        "gross_price_current_eur": extract_current_price(p_price),
        "net_price_current_eur": extract_current_price(p_net_price),
        "gross_price_infos": join_texts([text_or_none(x) or "" for x in (p_price.get("infos") or [])]),
        "net_price_infos": join_texts([text_or_none(x) or "" for x in (p_net_price.get("infos") or [])]),
        "gross_not_increased_since": gross_not_inc,
        "net_not_increased_since": net_not_inc,
        "payback_info": text_or_none(p_price.get("paybackInfo")) if isinstance(p_price, dict) else None,
        "payback_points": parse_int_from_text(p_price.get("paybackInfo")) if isinstance(p_price, dict) else None,
        "seo_brand": text_or_none(p_seo.get("brand")),
        "seo_category": text_or_none(p_seo.get("category")),
        "seo_price_eur": parse_eur(p_seo.get("price")),
        "seo_price_currency": text_or_none(p_seo.get("priceCurrency")),
        "seo_sku": text_or_none(p_seo.get("sku")),
        "has_variants": isinstance(product.get("variants"), dict),
        "has_videos": isinstance(product.get("videos"), list) and len(product.get("videos")) > 0,
        "has_seals": isinstance(product.get("seals"), list) and len(product.get("seals")) > 0,
        "has_pills": len(pill_labels) > 0,
        "has_eyecatchers": len(eyecatcher_labels) > 0,
        "eyecatchers_count": len(eyecatchers),
        "eyecatchers": join_texts(eyecatcher_labels),
        "pills": join_texts(pill_labels),
        "description_headers_count": len(description_headers),
        "description_headers": join_texts(description_headers),
        "description_headers_json": json.dumps(description_headers, ensure_ascii=False),
    }
    row.update(unit_info)
    row.update(description_cols)
    return row, description_headers


def build_profile(df: pd.DataFrame, header_counts: Counter[str], source_rows: int, invalid_json_rows: int) -> str:
    lines: list[str] = []
    lines.append("# dm_products_all profiling + cleaning report")
    lines.append("")
    lines.append("## Dataset shape")
    lines.append(f"- Source rows read: {source_rows:,}")
    lines.append(f"- Invalid JSON rows skipped: {invalid_json_rows:,}")
    lines.append(f"- Clean rows written: {len(df):,}")
    lines.append(f"- Columns: {df.shape[1]:,}")
    lines.append("")

    lines.append("## Uniqueness / duplicates")
    for col in ["gtin", "dan", "product_url", "slug"]:
        if col in df.columns:
            dup = int(df[col].duplicated(keep=False).sum()) if col in df else 0
            uniq = int(df[col].nunique(dropna=True))
            lines.append(f"- `{col}` unique={uniq:,}, duplicate_rows={dup:,}")
    lines.append("")

    lines.append("## Missingness (top 20 columns by null %)")
    null_pct = (df.isna().mean() * 100).sort_values(ascending=False).head(20)
    for col, pct in null_pct.items():
        lines.append(f"- `{col}`: {pct:.1f}% null")
    lines.append("")

    if "scraped_at_utc" in df.columns:
        lines.append("## Scrape timestamp range")
        lines.append(f"- min: {df['scraped_at_utc'].min()}")
        lines.append(f"- max: {df['scraped_at_utc'].max()}")
        lines.append("")

    numeric_cols = [
        "price_eur_top",
        "gross_price_current_eur",
        "net_price_current_eur",
        "metadata_price_eur",
        "seo_price_eur",
        "rating_count",
        "rating_value",
    ]
    lines.append("## Numeric summaries")
    for col in numeric_cols:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            continue
        lines.append(
            f"- `{col}`: count={len(s):,}, min={s.min():.4g}, median={s.median():.4g}, mean={s.mean():.4g}, max={s.max():.4g}"
        )
    lines.append("")

    lines.append("## Value counts (top 20)")
    for col in [
        "brand",
        "brand_product_name",
        "breadcrumb_1",
        "breadcrumb_2",
        "breadcrumb_3",
        "seo_category",
        "metadata_currency",
        "seo_price_currency",
        "available_norm",
        "has_variants",
        "has_videos",
        "has_seals",
        "has_pills",
        "has_eyecatchers",
    ]:
        if col not in df.columns:
            continue
        vc = df[col].astype("string").fillna("<NA>").value_counts(dropna=False).head(20)
        if vc.empty:
            continue
        lines.append(f"### `{col}`")
        for k, v in vc.items():
            lines.append(f"- {k}: {int(v):,}")
        lines.append("")

    lines.append("## Top description group headers")
    for header, count in header_counts.most_common(30):
        lines.append(f"- {header}: {count:,}")
    lines.append("")

    price_consistency = pd.DataFrame(
        {
            "top_vs_gross_abs_diff": (df["price_eur_top"] - df["gross_price_current_eur"]).abs(),
            "top_vs_meta_abs_diff": (df["price_eur_top"] - df["metadata_price_eur"]).abs(),
            "gross_vs_meta_abs_diff": (df["gross_price_current_eur"] - df["metadata_price_eur"]).abs(),
        }
    )
    lines.append("## Price consistency")
    for col in price_consistency.columns:
        s = price_consistency[col].dropna()
        if s.empty:
            continue
        lines.append(f"- `{col}` > 0.01 EUR: {int((s > 0.01).sum()):,} rows")
    lines.append("")

    return "\n".join(lines).strip() + "\n"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    header_counts: Counter[str] = Counter()
    source_rows = 0
    invalid_json_rows = 0

    with INPUT_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            source_rows += 1
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                invalid_json_rows += 1
                continue
            row, headers = parse_row(raw)
            header_counts.update(headers)
            rows.append(row)

    df = pd.DataFrame(rows)

    # Type normalization
    if "scraped_at_utc" in df.columns:
        df["scraped_at_utc"] = pd.to_datetime(df["scraped_at_utc"], utc=True, errors="coerce")
    for col in [
        "dan",
        "product_dan",
        "rating_count",
        "payback_points",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in [
        "price_eur_top",
        "gross_price_current_eur",
        "net_price_current_eur",
        "metadata_price_eur",
        "seo_price_eur",
        "rating_value",
        "unit_quantity",
        "unit_price_eur",
        "unit_price_per_quantity",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normalize booleans and categorical placeholders
    if "available_raw" in df.columns:
        df["available_norm"] = df["available_raw"].map({True: True, False: False})
        df["available_norm"] = df["available_norm"].astype("boolean")
    for col in [
        "product_is_pharmacy",
        "show_cbm_web",
        "show_cbm_app",
        "metadata_is_pharmacy",
        "has_variants",
        "has_videos",
        "has_seals",
        "has_pills",
        "has_eyecatchers",
    ]:
        if col in df.columns:
            df[col] = pd.Series(df[col], dtype="boolean")

    # Reconcile canonical values where the nested product object is richer.
    df["brand"] = df["brand"].fillna(df["brand_product_name"]).astype("string")
    df["gtin"] = df["gtin"].fillna(df["product_gtin"]).astype("string")
    df["dan"] = df["dan"].fillna(df["product_dan"]).astype("Int64")
    df["price_eur"] = (
        df["price_eur_top"]
        .fillna(df["gross_price_current_eur"])
        .fillna(df["metadata_price_eur"])
        .fillna(df["seo_price_eur"])
    )
    df["category_path"] = df["seo_category"].fillna(df["breadcrumbs_path"]).astype("string")
    df["currency"] = (
        df["metadata_currency"].fillna(df["seo_price_currency"]).fillna(pd.Series(["EUR"] * len(df)))
    ).astype("string")

    # Normalize text columns to pandas string dtype for consistent CSV/SQLite serialization.
    text_cols = [
        c
        for c in df.columns
        if c
        not in {
            "scraped_at_utc",
            "dan",
            "product_dan",
            "rating_count",
            "payback_points",
            "price_eur_top",
            "gross_price_current_eur",
            "net_price_current_eur",
            "metadata_price_eur",
            "seo_price_eur",
            "rating_value",
            "unit_quantity",
            "unit_price_eur",
            "unit_price_per_quantity",
            "price_eur",
            "unit_price_per_unit",
            "available_raw",
            "available_norm",
            "product_is_pharmacy",
            "show_cbm_web",
            "show_cbm_app",
            "metadata_is_pharmacy",
            "has_variants",
            "has_videos",
            "has_seals",
            "has_pills",
            "has_eyecatchers",
            "breadcrumbs_count",
            "eyecatchers_count",
            "description_headers_count",
        }
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Add row-level QA fields.
    df["price_diff_top_vs_gross"] = (df["price_eur_top"] - df["gross_price_current_eur"]).round(4)
    df["price_diff_top_vs_meta"] = (df["price_eur_top"] - df["metadata_price_eur"]).round(4)
    df["price_diff_gross_vs_meta"] = (df["gross_price_current_eur"] - df["metadata_price_eur"]).round(4)
    df["gtin_matches_nested"] = (df["gtin"] == df["product_gtin"]).astype("boolean")
    df["dan_matches_nested"] = (df["dan"] == df["product_dan"]).astype("boolean")

    # Sort deterministically and deduplicate by GTIN, preferring latest scrape timestamp.
    df = df.sort_values(["gtin", "scraped_at_utc", "dan"], na_position="last").reset_index(drop=True)
    before = len(df)
    df = df.drop_duplicates(subset=["gtin"], keep="last").reset_index(drop=True)
    deduped = before - len(df)

    profile_md = build_profile(df, header_counts, source_rows, invalid_json_rows)
    profile_md += f"\n## Deduplication applied\n- Dropped duplicate GTIN rows: {deduped:,}\n"
    PROFILE_PATH.write_text(profile_md, encoding="utf-8")

    # CSV export with UTF-8 BOM for spreadsheet compatibility.
    export_columns = [
        "gtin",
        "dan",
        "name",
        "brand",
        "title_subheadline",
        "price_eur",
        "currency",
        "unit_quantity",
        "unit_quantity_unit",
        "unit_price_eur",
        "unit_price_per_quantity",
        "unit_price_per_unit",
        "category_path",
        "breadcrumb_1",
        "breadcrumb_2",
        "breadcrumb_3",
        "breadcrumbs_path",
        "product_is_pharmacy",
        "rating_count",
        "rating_value",
        "has_variants",
        "has_videos",
        "has_seals",
        "has_pills",
        "has_eyecatchers",
        "eyecatchers",
        "pills",
        "desc_productbeschreibung",
        "desc_produktmerkmale",
        "desc_verwendungshinweise",
        "desc_inhaltsstoffe",
        "desc_aufbewahrungshinweise",
        "desc_warnhinweise",
        "desc_hergestellt_in",
        "desc_pflichthinweise",
        "desc_nachhaltigkeit",
        "desc_material",
        "desc_zutaten",
        "desc_naehrwerte",
        "desc_allergene",
        "desc_lieferumfang",
    ]
    export_columns = [c for c in export_columns if c in df.columns]
    export_df = df[export_columns].copy()
    export_df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    # SQLite export
    with sqlite3.connect(SQLITE_PATH) as conn:
        df_sql = export_df.copy()
        df_sql.to_sql("dm_products_cleaned", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dm_products_cleaned_gtin ON dm_products_cleaned(gtin)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dm_products_cleaned_dan ON dm_products_cleaned(dan)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dm_products_cleaned_brand ON dm_products_cleaned(brand)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dm_products_cleaned_category ON dm_products_cleaned(category_path)")
        conn.commit()

    print(f"Rows read: {source_rows}")
    print(f"Rows written (cleaned): {len(export_df)}")
    print(f"Columns written (cleaned): {len(export_df.columns)}")
    print(f"CSV: {CSV_PATH}")
    print(f"SQLite: {SQLITE_PATH}")
    print(f"Profile: {PROFILE_PATH}")


if __name__ == "__main__":
    main()
