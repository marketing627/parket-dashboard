#!/usr/bin/env python3
"""
Generate data.json from Google Sheets (Meta Ads export).
Can fetch directly from a public Google Sheet or read a local CSV.
"""

import csv
import json
import re
import io
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

# Google Sheets public export URLs
SHEET_ID = "1cyX5e8UP3Hqm9GqxYu1GVrhu-TG2JdSEGJydQIiX8yc"
META_SHEET = "RAW Meta"
GOOGLE_GID = "31847001"
META_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={META_SHEET}"
GOOGLE_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&gid={GOOGLE_GID}"

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "data.json")


def fetch_csv(url, label=""):
    """Fetch CSV directly from Google Sheets."""
    import urllib.request
    print(f"Fetching {label}...")
    clean_url = url.replace(" ", "%20")
    req = urllib.request.Request(clean_url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        content = resp.read().decode("utf-8")
    print(f"  Downloaded {len(content)} bytes")
    return content


def parse_number(val):
    if val is None or val == "":
        return 0.0
    val = str(val).strip()
    val = val.replace("R$", "").replace("%", "").strip()
    val = val.replace('"', '')
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        return 0.0
    if "," in val and "." in val:
        last_comma = val.rfind(",")
        last_dot = val.rfind(".")
        if last_dot > last_comma:
            val = val.replace(",", "")
        else:
            val = val.replace(".", "").replace(",", ".")
    elif "," in val:
        parts = val.split(",")
        if all(len(p) == 3 and p.isdigit() for p in parts[1:]):
            val = val.replace(",", "")
        else:
            val = val.replace(",", ".")
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def extract_creative_type(ad_name):
    name_lower = ad_name.lower()
    if "reels" in name_lower or "vídeo" in name_lower or "video" in name_lower:
        return "Reels/Video"
    if "carrossel motion" in name_lower:
        return "Carrossel Motion"
    if "carrossel" in name_lower:
        return "Carrossel"
    if "motion" in name_lower:
        return "Motion"
    if "estático" in name_lower or "estatico" in name_lower or "pin" in name_lower:
        return "Estatico"
    if "post do instagram" in name_lower or "publicação do instagram" in name_lower:
        return "Post Organico"
    if "remarketing" in name_lower:
        return "Remarketing"
    if "[ads]" in name_lower:
        return "Estatico"
    return "Outro"


def safe_div(a, b, default=0.0):
    return a / b if b != 0 else default


def parse_csv_rows(csv_content):
    """Parse CSV content (from Sheets or file) into row dicts."""
    reader = csv.reader(io.StringIO(csv_content))
    # Google Sheets gviz export merges PT+EN headers into one row
    header = next(reader)

    rows = []
    for row in reader:
        if len(row) < 11:
            continue
        day = row[0].strip().replace('"', '')
        if not day or not re.match(r"\d{4}-\d{2}-\d{2}", day):
            continue

        record = {
            "date": day,
            "campaign": row[1].strip(),
            "adset": row[2].strip(),
            "ad": row[3].strip(),
            "impressions": parse_number(row[4]),
            "reach": parse_number(row[5]),
            "clicks": parse_number(row[6]),
            "ctr_raw": parse_number(row[7]),
            "cpc_raw": parse_number(row[8]),
            "cpm_raw": parse_number(row[9]),
            "spend": parse_number(row[10]),
            "leads": parse_number(row[11]) if len(row) > 11 else 0,
            "cpl_raw": parse_number(row[12]) if len(row) > 12 else 0,
            "conversations": parse_number(row[13]) if len(row) > 13 else 0,
            "cost_per_conversation": parse_number(row[14]) if len(row) > 14 else 0,
            "frequency": parse_number(row[15]) if len(row) > 15 else 0,
        }
        record["creative_type"] = extract_creative_type(record["ad"])
        record["results"] = record["conversations"] + record["leads"]
        rows.append(record)
    return rows


def aggregate(rows, key_fn, label="name"):
    groups = defaultdict(list)
    for r in rows:
        groups[key_fn(r)].append(r)

    result = []
    for name, group in sorted(groups.items(), key=lambda x: -sum(r["spend"] for r in x[1])):
        impressions = sum(r["impressions"] for r in group)
        reach = sum(r["reach"] for r in group)
        clicks = sum(r["clicks"] for r in group)
        spend = sum(r["spend"] for r in group)
        leads = sum(r["leads"] for r in group)
        conversations = sum(r["conversations"] for r in group)
        results = conversations + leads
        days_active = len(set(r["date"] for r in group))

        result.append({
            label: name,
            "impressions": round(impressions),
            "reach": round(reach),
            "clicks": round(clicks),
            "spend": round(spend, 2),
            "leads": round(leads),
            "conversations": round(conversations),
            "results": round(results),
            "ctr": round(safe_div(clicks, impressions) * 100, 2),
            "cpc": round(safe_div(spend, clicks), 2),
            "cpm": round(safe_div(spend, impressions) * 1000, 2),
            "cpa": round(safe_div(spend, results), 2) if results > 0 else 0,
            "cpl": round(safe_div(spend, leads), 2) if leads > 0 else 0,
            "cost_per_conversation": round(safe_div(spend, conversations), 2) if conversations > 0 else 0,
            "cvr": round(safe_div(results, clicks) * 100, 2) if clicks > 0 else 0,
            "frequency": round(safe_div(impressions, reach), 2) if reach > 0 else 0,
            "days_active": days_active,
        })
    return result


def compute_daily(rows):
    groups = defaultdict(list)
    for r in rows:
        groups[r["date"]].append(r)

    daily = []
    for date in sorted(groups.keys()):
        group = groups[date]
        impressions = sum(r["impressions"] for r in group)
        clicks = sum(r["clicks"] for r in group)
        spend = sum(r["spend"] for r in group)
        leads = sum(r["leads"] for r in group)
        conversations = sum(r["conversations"] for r in group)
        results = conversations + leads
        reach = sum(r["reach"] for r in group)

        daily.append({
            "date": date,
            "spend": round(spend, 2),
            "impressions": round(impressions),
            "reach": round(reach),
            "clicks": round(clicks),
            "leads": round(leads),
            "conversations": round(conversations),
            "results": round(results),
            "ctr": round(safe_div(clicks, impressions) * 100, 2),
            "cpc": round(safe_div(spend, clicks), 2),
            "cpm": round(safe_div(spend, impressions) * 1000, 2),
            "cpa": round(safe_div(spend, results), 2) if results > 0 else 0,
            "frequency": round(safe_div(impressions, reach), 2) if reach > 0 else 0,
        })
    return daily


def compute_totals(rows):
    impressions = sum(r["impressions"] for r in rows)
    reach = sum(r["reach"] for r in rows)
    clicks = sum(r["clicks"] for r in rows)
    spend = sum(r["spend"] for r in rows)
    leads = sum(r["leads"] for r in rows)
    conversations = sum(r["conversations"] for r in rows)
    results = conversations + leads
    dates = sorted(set(r["date"] for r in rows))
    num_days = len(dates)

    return {
        "impressions": round(impressions),
        "reach": round(reach),
        "clicks": round(clicks),
        "spend": round(spend, 2),
        "leads": round(leads),
        "conversations": round(conversations),
        "results": round(results),
        "ctr": round(safe_div(clicks, impressions) * 100, 2),
        "cpc": round(safe_div(spend, clicks), 2),
        "cpm": round(safe_div(spend, impressions) * 1000, 2),
        "cpa": round(safe_div(spend, results), 2) if results > 0 else 0,
        "cpl": round(safe_div(spend, leads), 2) if leads > 0 else 0,
        "cost_per_conversation": round(safe_div(spend, conversations), 2) if conversations > 0 else 0,
        "cvr": round(safe_div(results, clicks) * 100, 2),
        "frequency": round(safe_div(impressions, reach), 2),
        "num_days": num_days,
        "date_range": {"start": dates[0], "end": dates[-1]} if dates else {},
        "avg_daily_spend": round(safe_div(spend, num_days), 2),
        "avg_daily_results": round(safe_div(results, num_days), 2),
    }


def compute_concentration(items, metric="spend", label="name"):
    sorted_items = sorted(items, key=lambda x: x.get(metric, 0), reverse=True)
    total = sum(x.get(metric, 0) for x in sorted_items)
    if total == 0:
        return {"top_items": [], "pareto_count": 0, "hhi": 0, "gini": 0}

    cumulative = 0
    top_items = []
    pareto_count = 0
    pareto_reached = False
    shares = []

    for i, item in enumerate(sorted_items):
        val = item.get(metric, 0)
        share = safe_div(val, total) * 100
        cumulative += val
        cum_pct = safe_div(cumulative, total) * 100
        shares.append(share)
        top_items.append({
            "rank": i + 1,
            "name": item[label],
            "spend": round(val, 2),
            "share_pct": round(share, 2),
            "cumulative_pct": round(cum_pct, 2),
        })
        if cum_pct >= 80 and not pareto_reached:
            pareto_count = i + 1
            pareto_reached = True

    hhi = sum((s / 100) ** 2 for s in shares) * 10000
    n = len(shares)
    if n > 1:
        sorted_shares = sorted(shares)
        cum = sum((2 * (i + 1) - n - 1) * sorted_shares[i] for i in range(n))
        gini = cum / (n * sum(sorted_shares)) if sum(sorted_shares) > 0 else 0
    else:
        gini = 0

    return {
        "top_items": top_items[:20],
        "pareto_count": pareto_count,
        "pareto_pct": round(safe_div(pareto_count, len(sorted_items)) * 100, 1),
        "hhi": round(hhi, 1),
        "gini": round(gini, 3),
    }


def compute_quadrants(items, label="name"):
    valid = [i for i in items if i.get("clicks", 0) > 10 and i.get("impressions", 0) > 100]
    if len(valid) < 2:
        return {"quadrants": {}, "medians": {}}

    ctrs = sorted([i["ctr"] for i in valid])
    cvrs = sorted([i["cvr"] for i in valid])
    median_ctr = ctrs[len(ctrs) // 2]
    median_cvr = cvrs[len(cvrs) // 2]

    quadrants = {"stars": [], "efficient": [], "volume": [], "underperformers": []}
    for item in valid:
        high_ctr = item["ctr"] >= median_ctr
        high_cvr = item["cvr"] >= median_cvr
        if high_ctr and high_cvr: q = "stars"
        elif not high_ctr and high_cvr: q = "efficient"
        elif high_ctr and not high_cvr: q = "volume"
        else: q = "underperformers"
        quadrants[q].append({
            "name": item[label], "ctr": item["ctr"], "cvr": item["cvr"],
            "spend": item["spend"], "results": item["results"], "cpa": item["cpa"],
        })

    return {"medians": {"ctr": round(median_ctr, 2), "cvr": round(median_cvr, 2)}, "quadrants": quadrants}


def compute_benchmarks(totals):
    benchmarks = {
        "ctr": {"benchmark": 0.90, "unit": "%", "source": "Meta Ads Benchmark (RE/Construction)"},
        "cpc": {"benchmark": 1.81, "unit": "R$", "source": "Meta Ads Benchmark (RE/Construction)"},
        "cpm": {"benchmark": 15.0, "unit": "R$", "source": "Meta Ads Benchmark (general)"},
        "cpa": {"benchmark": 50.0, "unit": "R$", "source": "Meta Ads Benchmark (messaging)"},
        "frequency": {"benchmark": 2.0, "unit": "x", "source": "Recommended max for prospecting"},
    }
    result = {}
    for metric, bench in benchmarks.items():
        actual = totals.get(metric, 0)
        diff_pct = safe_div(actual - bench["benchmark"], bench["benchmark"]) * 100
        if metric in ["cpc", "cpm", "cpa"]:
            status = "better" if actual < bench["benchmark"] else ("worse" if actual > bench["benchmark"] * 1.2 else "ok")
        elif metric == "frequency":
            status = "ok" if actual <= bench["benchmark"] else "high"
        else:
            status = "better" if actual > bench["benchmark"] else ("worse" if actual < bench["benchmark"] * 0.8 else "ok")
        result[metric] = {
            "actual": round(actual, 2), "benchmark": bench["benchmark"],
            "diff_pct": round(diff_pct, 1), "status": status,
            "unit": bench["unit"], "source": bench["source"],
        }
    return result


def parse_google_ads_rows(csv_content):
    """Parse Google Ads CSV from Sheets."""
    reader = csv.reader(io.StringIO(csv_content))
    header1 = next(reader, None)  # Title row
    header2 = next(reader, None)  # Column headers

    rows = []
    for row in reader:
        if len(row) < 10:
            continue
        day = row[0].strip().replace('"', '')
        if not day or not re.match(r"\d{4}-\d{2}-\d{2}", day):
            continue
        record = {
            "date": day,
            "campaign": row[1].strip(),
            "adgroup": row[2].strip(),
            "keyword": row[3].strip(),
            "match_type": row[4].strip(),
            "impressions": parse_number(row[5]),
            "clicks": parse_number(row[6]),
            "ctr": parse_number(row[7]),
            "cpc": parse_number(row[8]),
            "spend": parse_number(row[9]),
            "conversions": parse_number(row[10]) if len(row) > 10 else 0,
            "cost_per_conversion": parse_number(row[11]) if len(row) > 11 else 0,
            "conv_rate": parse_number(row[12]) if len(row) > 12 else 0,
            "quality_score": parse_number(row[13]) if len(row) > 13 else 0,
        }
        record["results"] = record["conversions"]
        rows.append(record)
    return rows


def build_google_ads_data(rows):
    """Build Google Ads section of data.json."""
    if not rows:
        return {"has_data": False, "totals": {}, "daily": [], "campaigns": [], "adgroups": [], "keywords": []}

    # Totals
    impressions = sum(r["impressions"] for r in rows)
    clicks = sum(r["clicks"] for r in rows)
    spend = sum(r["spend"] for r in rows)
    conversions = sum(r["conversions"] for r in rows)
    dates = sorted(set(r["date"] for r in rows))

    totals = {
        "impressions": round(impressions),
        "clicks": round(clicks),
        "spend": round(spend, 2),
        "conversions": round(conversions),
        "ctr": round(safe_div(clicks, impressions) * 100, 2),
        "cpc": round(safe_div(spend, clicks), 2),
        "cpm": round(safe_div(spend, impressions) * 1000, 2),
        "cpa": round(safe_div(spend, conversions), 2) if conversions > 0 else 0,
        "cvr": round(safe_div(conversions, clicks) * 100, 2),
        "num_days": len(dates),
        "date_range": {"start": dates[0], "end": dates[-1]} if dates else {},
        "avg_daily_spend": round(safe_div(spend, len(dates)), 2),
    }

    # Daily
    day_groups = defaultdict(list)
    for r in rows:
        day_groups[r["date"]].append(r)
    daily = []
    for date in sorted(day_groups.keys()):
        g = day_groups[date]
        d_impr = sum(r["impressions"] for r in g)
        d_clicks = sum(r["clicks"] for r in g)
        d_spend = sum(r["spend"] for r in g)
        d_conv = sum(r["conversions"] for r in g)
        daily.append({
            "date": date, "impressions": round(d_impr), "clicks": round(d_clicks),
            "spend": round(d_spend, 2), "conversions": round(d_conv),
            "ctr": round(safe_div(d_clicks, d_impr) * 100, 2),
            "cpc": round(safe_div(d_spend, d_clicks), 2),
            "cpa": round(safe_div(d_spend, d_conv), 2) if d_conv > 0 else 0,
        })

    # Campaigns
    camp_groups = defaultdict(list)
    for r in rows:
        camp_groups[r["campaign"]].append(r)
    campaigns = []
    for name, g in sorted(camp_groups.items(), key=lambda x: -sum(r["spend"] for r in x[1])):
        c_impr = sum(r["impressions"] for r in g)
        c_clicks = sum(r["clicks"] for r in g)
        c_spend = sum(r["spend"] for r in g)
        c_conv = sum(r["conversions"] for r in g)
        campaigns.append({
            "name": name, "impressions": round(c_impr), "clicks": round(c_clicks),
            "spend": round(c_spend, 2), "conversions": round(c_conv),
            "ctr": round(safe_div(c_clicks, c_impr) * 100, 2),
            "cpc": round(safe_div(c_spend, c_clicks), 2),
            "cpa": round(safe_div(c_spend, c_conv), 2) if c_conv > 0 else 0,
            "cvr": round(safe_div(c_conv, c_clicks) * 100, 2),
            "days_active": len(set(r["date"] for r in g)),
        })

    # Ad Groups
    ag_groups = defaultdict(list)
    for r in rows:
        ag_groups[r["adgroup"]].append(r)
    adgroups = []
    for name, g in sorted(ag_groups.items(), key=lambda x: -sum(r["spend"] for r in x[1])):
        a_impr = sum(r["impressions"] for r in g)
        a_clicks = sum(r["clicks"] for r in g)
        a_spend = sum(r["spend"] for r in g)
        a_conv = sum(r["conversions"] for r in g)
        adgroups.append({
            "name": name, "impressions": round(a_impr), "clicks": round(a_clicks),
            "spend": round(a_spend, 2), "conversions": round(a_conv),
            "ctr": round(safe_div(a_clicks, a_impr) * 100, 2),
            "cpc": round(safe_div(a_spend, a_clicks), 2),
            "cpa": round(safe_div(a_spend, a_conv), 2) if a_conv > 0 else 0,
            "cvr": round(safe_div(a_conv, a_clicks) * 100, 2),
        })

    # Keywords
    kw_groups = defaultdict(list)
    for r in rows:
        if r["keyword"]:
            kw_groups[(r["keyword"], r["match_type"])].append(r)
    keywords = []
    for (kw, mt), g in sorted(kw_groups.items(), key=lambda x: -sum(r["spend"] for r in x[1])):
        k_impr = sum(r["impressions"] for r in g)
        k_clicks = sum(r["clicks"] for r in g)
        k_spend = sum(r["spend"] for r in g)
        k_conv = sum(r["conversions"] for r in g)
        qs_vals = [r["quality_score"] for r in g if r["quality_score"] > 0]
        keywords.append({
            "keyword": kw, "match_type": mt,
            "impressions": round(k_impr), "clicks": round(k_clicks),
            "spend": round(k_spend, 2), "conversions": round(k_conv),
            "ctr": round(safe_div(k_clicks, k_impr) * 100, 2),
            "cpc": round(safe_div(k_spend, k_clicks), 2),
            "cpa": round(safe_div(k_spend, k_conv), 2) if k_conv > 0 else 0,
            "cvr": round(safe_div(k_conv, k_clicks) * 100, 2),
            "quality_score": round(sum(qs_vals) / len(qs_vals), 1) if qs_vals else 0,
        })

    return {
        "has_data": True,
        "totals": totals,
        "daily": daily,
        "campaigns": campaigns,
        "adgroups": adgroups,
        "keywords": keywords,
        "concentration": compute_concentration(campaigns) if campaigns else {},
    }


def main():
    config = {
        "budget_meta_monthly": 40000, "budget_google_monthly": 10000,
        "budget_total_monthly": 50000,
        "meta_vendas_mes": 20, "ticket_medio": 500000,
        "meta_faturamento_mes": 10000000, "lead_to_venda": 0.05,
        "contato_to_mql": 0.40, "mql_to_vendedor": 0.90,
        "vendedor_to_sql": 0.55, "sql_to_venda": 0.50,
    }

    # Fetch Meta Ads
    meta_csv = fetch_csv(META_URL, "Meta Ads")
    rows = parse_csv_rows(meta_csv)
    print(f"  Meta: {len(rows)} rows parsed")

    # Fetch Google Ads
    google_csv = fetch_csv(GOOGLE_URL, "Google Ads")
    google_rows = parse_google_ads_rows(google_csv)
    print(f"  Google: {len(google_rows)} rows parsed")

    if not rows:
        print("ERROR: No data rows found!")
        sys.exit(1)

    # Compute everything
    totals = compute_totals(rows)
    daily = compute_daily(rows)
    campaigns = aggregate(rows, lambda r: r["campaign"], "name")
    adsets = aggregate(rows, lambda r: r["adset"], "name")
    ads = aggregate(rows, lambda r: r["ad"], "name")
    creative_types = aggregate(rows, lambda r: r["creative_type"], "name")

    conc_campaigns = compute_concentration(campaigns)
    conc_ads = compute_concentration(ads)
    quadrant_ads = compute_quadrants(ads)
    bench = compute_benchmarks(totals)

    # Projections (conversion rates only, no invented numbers)
    cr = config
    projections = {
        "conversion_rates": {
            "lead_to_venda": cr["lead_to_venda"],
            "contato_to_mql": cr["contato_to_mql"],
            "mql_to_vendedor": cr["mql_to_vendedor"],
            "vendedor_to_sql": cr["vendedor_to_sql"],
            "sql_to_venda": cr["sql_to_venda"],
        },
        "targets": {
            "vendas_meta": cr["meta_vendas_mes"],
            "leads_needed": round(safe_div(cr["meta_vendas_mes"], cr["lead_to_venda"])),
            "daily_leads_needed": round(safe_div(cr["meta_vendas_mes"], cr["lead_to_venda"]) / 30, 1),
            "faturamento_meta": cr["meta_faturamento_mes"],
        },
        "monthly_projection": {
            "spend": round(totals["avg_daily_spend"] * 30, 2),
            "results": round(totals["avg_daily_results"] * 30),
            "budget_utilization_pct": round(safe_div(totals["avg_daily_spend"] * 30, cr["budget_meta_monthly"]) * 100, 1),
        },
        "pace": {},
    }

    # Pace for current month
    dates = sorted(set(r["date"] for r in rows))
    if dates:
        last_date = datetime.strptime(dates[-1], "%Y-%m-%d")
        month_start = last_date.replace(day=1)
        month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        days_elapsed = (last_date - month_start).days + 1
        days_total = (month_end - month_start).days + 1
        current_month_data = [d for d in daily if d["date"][:7] == last_date.strftime("%Y-%m")]
        month_spend = sum(d["spend"] for d in current_month_data)
        month_results = sum(d["results"] for d in current_month_data)
        projections["pace"] = {
            "current_month": last_date.strftime("%Y-%m"),
            "days_elapsed": days_elapsed, "days_total": days_total,
            "month_spend_so_far": round(month_spend, 2),
            "month_results_so_far": round(month_results),
        }

    # Insights (auto-generated)
    insights = []
    insights.append({"type": "summary", "priority": "high", "title": "Resumo do Periodo",
        "text": f"Em {totals['num_days']} dias, investimos R$ {totals['spend']:,.2f} gerando {totals['results']} resultados a CPA medio de R$ {totals['cpa']:,.2f}."})

    if campaigns:
        best = min([c for c in campaigns if c["results"] > 0], key=lambda x: x["cpa"], default=None)
        if best:
            insights.append({"type": "campaign", "priority": "high", "title": "Melhor Campanha (CPA)",
                "text": f"'{best['name'][:60]}' tem CPA R$ {best['cpa']:,.2f} com {best['results']} resultados."})

    if creative_types:
        best_ct = min([c for c in creative_types if c["results"] > 0], key=lambda x: x["cpa"], default=None)
        if best_ct:
            insights.append({"type": "creative", "priority": "high", "title": "Melhor Tipo de Criativo",
                "text": f"'{best_ct['name']}' CPA R$ {best_ct['cpa']:,.2f} ({best_ct['results']} resultados)."})

    for metric, comp in bench.items():
        if comp["status"] in ("better", "worse"):
            insights.append({"type": "benchmark", "priority": "positive" if comp["status"] == "better" else "warning",
                "title": f"{metric.upper()} vs Benchmark",
                "text": f"{metric.upper()}: {comp['actual']}{comp['unit']} vs benchmark {comp['benchmark']}{comp['unit']} ({comp['diff_pct']:+.1f}%)."})

    # Build Google Ads data
    google_ads = build_google_ads_data(google_rows)

    # Build output
    output = {
        "meta": {
            "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "source": "Google Sheets (Adveronix Export)",
            "rows_processed": len(rows),
            "google_rows_processed": len(google_rows),
            "date_range": totals["date_range"],
            "num_days": totals["num_days"],
            "currency": "BRL",
        },
        "totals": totals,
        "funnel": [
            {"stage": "Impressoes", "value": totals["impressions"]},
            {"stage": "Alcance", "value": totals["reach"]},
            {"stage": "Cliques", "value": totals["clicks"]},
            {"stage": "Resultados", "value": totals["results"]},
            {"stage": "Conversas WA", "value": totals["conversations"]},
        ],
        "daily": daily,
        "campaigns": campaigns,
        "adsets": adsets,
        "ads": ads,
        "creative_types": creative_types,
        "concentration": {"campaigns": conc_campaigns, "ads": conc_ads},
        "quadrant_summary": {"campaigns": {}, "ads": quadrant_ads},
        "significance": {"campaigns": [], "ads": []},
        "cross_analysis": {},
        "benchmarks": bench,
        "goals": {
            "budget_meta_monthly": config["budget_meta_monthly"],
            "budget_total_monthly": config["budget_total_monthly"],
            "vendas_meta": config["meta_vendas_mes"],
            "ticket_medio": config["ticket_medio"],
            "faturamento_meta": config["meta_faturamento_mes"],
            "lead_to_venda": config["lead_to_venda"],
        },
        "projections": projections,
        "insights": insights,
        "google_ads": google_ads,
    }

    print(f"Writing {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"DATA UPDATED SUCCESSFULLY")
    print(f"{'='*60}")
    print(f"  Meta: {totals['num_days']} dias | R$ {totals['spend']:,.2f} | {totals['results']:,} resultados | CPA R$ {totals['cpa']:.2f}")
    print(f"  Google: {len(google_rows)} rows | {'Com dados' if google_ads['has_data'] else 'Sem dados ainda'}")
    print(f"  Campanhas Meta: {len(campaigns)} | Ads: {len(ads)}")


if __name__ == "__main__":
    main()
