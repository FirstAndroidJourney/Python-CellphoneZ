#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CellphoneS_Crawl.py  — Full Console Log Edition

- Discover product sitemaps (recursive, supports .xml.gz)
- Parse product pages with BeautifulSoup
- Build category tree from breadcrumbs
- Deterministic UUIDv5 for categories & products (no slug needed)
- Export CSV: categories.csv, products.csv
- Verbose console logs with levels: INFO / DEBUG / TRACE

Deps:
    pip install requests beautifulsoup4 lxml
"""

import argparse
import csv
import gzip
import io
import os
import random
import re
import sys
import time
import traceback
import unicodedata
import uuid
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ============================ CONFIG ============================

BASE = "https://cellphones.com.vn"

def HEADERS_TEMPLATE(ua: str | None):
    return {
        "User-Agent": ua or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/128.0.0.0 Safari/537.36",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "application/xml, text/xml;q=0.9, */*;q=0.8",
    }

POPULAR_TOP = {
    "dien-thoai", "tablet", "laptop", "am-thanh", "dong-ho",
    "phu-kien", "tivi", "pc", "man-hinh", "gia-dung", "camera", "dien-may"
}

# ============================ LOGGING ============================

LEVELS = {"INFO": 1, "DEBUG": 2, "TRACE": 3}
LOG_LEVEL = LEVELS["INFO"]

def set_log_level(verbose: bool, trace: bool):
    global LOG_LEVEL
    if trace:
        LOG_LEVEL = LEVELS["TRACE"]
    elif verbose:
        LOG_LEVEL = LEVELS["DEBUG"]
    else:
        LOG_LEVEL = LEVELS["INFO"]

def _ts():
    return time.strftime("%H:%M:%S")

def log_info(msg): 
    if LOG_LEVEL >= LEVELS["INFO"]:
        print(f"[{_ts()}] [INFO]  {msg}", flush=True)

def log_debug(msg): 
    if LOG_LEVEL >= LEVELS["DEBUG"]:
        print(f"[{_ts()}] [DEBUG] {msg}", flush=True)

def log_trace(msg): 
    if LOG_LEVEL >= LEVELS["TRACE"]:
        print(f"[{_ts()}] [TRACE] {msg}", flush=True)

def log_warn(msg):
    print(f"[{_ts()}] [WARN]  {msg}", flush=True)

def log_error(msg):
    print(f"[{_ts()}] [ERROR] {msg}", flush=True)

# ============================ UTILS ============================

def http_get(url, headers, timeout=25, max_retry=4, jitter=(0.05, 0.25)):
    """HTTP GET with retries + logs"""
    for attempt in range(max_retry):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            log_trace(f"HTTP {r.status_code} {url}")
            return r
        except requests.RequestException as e:
            log_warn(f"HTTP EXC {url} -> {e.__class__.__name__}: {e}")
            sleep = (attempt + 1) * 1.2 + random.uniform(*jitter)
            log_debug(f"Retrying in {sleep:.2f}s (attempt {attempt+1}/{max_retry})")
            time.sleep(sleep)
    # final try (let raise if fails)
    r = requests.get(url, headers=headers, timeout=timeout)
    log_trace(f"HTTP {r.status_code} {url}")
    return r

def norm_text(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-zA-Z0-9\s\/\-\|]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def to_path(*parts: str) -> str:
    cleaned = []
    for p in parts:
        if not p: 
            continue
        p2 = norm_text(p).lower().replace(" ", "-").strip("-/|")
        if p2:
            cleaned.append(p2)
    return "/".join(cleaned)

def uuid_cat(path: str) -> str:
    if not path:
        return ""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"cellphones:/cat/{path}"))

def uuid_prod(url: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"cellphones:/prod/{url}"))

def normalize_price_vnd(text: str):
    if not text:
        return None
    m = re.search(r"(\d[\d\.]+)\s*đ", text.replace(",", ".").lower())
    if not m:
        return None
    v = m.group(1).replace(".", "")
    try:
        return float(v)
    except Exception:
        return None

# ============================ MODELS ============================

@dataclass
class CategoryNode:
    name: str
    path: str
    parent_path: str | None
    is_popular: bool

# ============================ SITEMAP WALKER ============================

def _fetch_xml_text(url, headers, timeout=25):
    """Fetch and decode XML / XML.GZ with logs"""
    r = http_get(url, headers=headers, timeout=timeout)
    if r is None:
        log_warn(f"FETCH {url} -> None")
        return None, 0
    if r.status_code != 200:
        log_warn(f"FETCH {url} -> status={r.status_code}")
        return None, r.status_code

    content = r.content or b""
    gz = url.lower().endswith(".gz") or \
         r.headers.get("Content-Encoding","").lower().find("gzip") != -1 or \
         r.headers.get("Content-Type","").lower().find("gzip") != -1

    if gz:
        try:
            content = gzip.decompress(content)
            log_trace(f"DECOMPRESS: gzip OK {url}")
        except Exception:
            try:
                content = gzip.GzipFile(fileobj=io.BytesIO(r.content)).read()
                log_trace(f"DECOMPRESS: gzip stream OK {url}")
            except Exception as e:
                log_warn(f"DECOMPRESS FAIL {url} -> {e}")

    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        text = r.text

    log_debug(f"[FETCH] {url} -> bytes={len(text)}")
    return text, r.status_code

def _extract_locs_from_xml(xml_text: str):
    """Parse XML, return (type, locs) with logs"""
    if not xml_text:
        return "unknown", []
    soup = BeautifulSoup(xml_text, "xml")

    # Strict detection
    if soup.find("sitemapindex"):
        locs = [(el.text or "").strip() for el in soup.find_all("loc") if (el.text or "").strip()]
        log_debug(f"XML type=sitemapindex, locs={len(locs)}")
        return "sitemapindex", locs
    if soup.find("urlset"):
        locs = [(el.text or "").strip() for el in soup.find_all("loc") if (el.text or "").strip()]
        log_debug(f"XML type=urlset, locs={len(locs)}")
        return "urlset", locs

    # Fallback messy XML
    locs = [(el.text or "").strip() for el in soup.find_all("loc") if (el.text or "").strip()]
    if locs:
        if soup.find_all("url"):
            log_debug(f"XML fallback=urlset, locs={len(locs)}")
            return "urlset", locs
        if soup.find_all("sitemap"):
            log_debug(f"XML fallback=sitemapindex, locs={len(locs)}")
            return "sitemapindex", locs

    log_debug("XML type=unknown, locs=0")
    return "unknown", []

def _discover_product_urls_from_sitemap(url, headers, seen, budget_left):
    """Recursive discovery with deep logs"""
    if url in seen:
        log_trace(f"SEEN   {url} -> skip")
        return []
    seen.add(url)

    xml_text, status = _fetch_xml_text(url, headers=headers)
    log_info(f"[SITEMAP] {url} -> status={status}, bytes={len(xml_text or '')}")
    if status != 200 or not xml_text:
        return []

    typ, locs = _extract_locs_from_xml(xml_text)
    log_info(f"[PARSE]   {url} -> type={typ}, locs={len(locs)}")

    results = []
    if typ == "sitemapindex":
        for i, loc in enumerate(locs, 1):
            if budget_left is not None and len(results) >= budget_left:
                break
            nxt = urljoin(url, loc)
            log_debug(f"  [INDEX] {i}/{len(locs)} → {nxt}")
            sub = _discover_product_urls_from_sitemap(
                nxt, headers, seen,
                None if budget_left is None else (budget_left - len(results))
            )
            results.extend(sub)
        log_info(f"[INDEX DONE] {url} -> collected={len(results)}")
        return results

    if typ == "urlset":
        for loc in locs:
            if budget_left is not None and len(results) >= budget_left:
                break
            # filter probable product links
            if loc.lower().endswith(".html"):
                results.append(loc)
        log_info(f"[URLSET] {url} -> collected_html={len(results)} of {len(locs)} locs")
        return results

    log_warn(f"[UNKNOWN] {url} -> No <urlset>/<sitemapindex>")
    return []

def find_product_sitemaps(headers, max_guess=120):
    """Collect entrypoint sitemaps with logs"""
    entrypoints = set()
    common_indexes = [
        f"{BASE}/sitemap.xml",
        f"{BASE}/sitemap_index.xml",
        f"{BASE}/sitemap/sitemap.xml",
        f"{BASE}/sitemap/product-sitemap.xml",
    ]
    for u in common_indexes:
        try:
            r = http_get(u, headers=headers)
            if r is not None and r.status_code == 200:
                log_info(f"[ENTRY] {u} (200)")
                entrypoints.add(u)
            else:
                log_trace(f"[ENTRY] {u} -> {getattr(r,'status_code',None)}")
        except Exception as e:
            log_warn(f"[ENTRY ERR] {u} -> {e}")

    # brute-force numeric patterns and .gz
    for n in range(0, max_guess + 1):
        for p in (
            f"{BASE}/sitemap/product-sitemap{n}.xml",
            f"{BASE}/sitemap/products-sitemap{n}.xml",
            f"{BASE}/sitemap/product-sitemap{n}.xml.gz",
            f"{BASE}/sitemap/products-sitemap{n}.xml.gz",
        ):
            try:
                r = http_get(p, headers=headers)
                if r is not None and r.status_code == 200:
                    log_info(f"[ENTRY] {p} (200)")
                    entrypoints.add(p)
            except Exception as e:
                log_trace(f"[ENTRY ERR] {p} -> {e}")
    return sorted(entrypoints)

def iter_product_urls(headers, limit=None):
    seen = set()
    total = 0
    entries = find_product_sitemaps(headers)
    if not entries:
        log_warn("[SITEMAP] Không tìm thấy entry nào.")
        return
    log_info(f"[SITEMAP] Entry points = {len(entries)}")
    for entry in entries:
        if limit is not None and total >= limit:
            break
        budget = None if limit is None else (limit - total)
        urls = _discover_product_urls_from_sitemap(entry, headers, seen, budget)
        for u in urls:
            yield u
            total += 1
            if limit is not None and total >= limit:
                break

# ============================ PRODUCT PARSER ============================

def extract_breadcrumbs(soup: BeautifulSoup):
    crumbs = []
    cands = []
    cands.extend(soup.select('[aria-label*="breadcrumb" i]'))
    cands.extend(soup.select('[role="navigation"][aria-label*="breadcrumb" i]'))
    cands.extend(soup.select('.breadcrumb, .breadcrumbs, nav.breadcrumb'))
    cands = [c for c in cands if c]
    if not cands:
        log_trace("Breadcrumb: not found")
        return crumbs
    nav = cands[0]
    items = nav.select("li, a, span")
    for it in items:
        t = it.get_text(" ", strip=True)
        t = t.replace("Trang chủ", "").strip()
        if t:
            crumbs.append(t)
    seen = set()
    out = []
    for c in crumbs:
        k = c.lower()
        if k not in seen:
            seen.add(k)
            out.append(c)
    log_debug(f"Breadcrumb: {out}")
    return out

def pick_category_path_from_breadcrumb(crumbs):
    if not crumbs:
        return []
    arr = [c for c in crumbs if c and len(c) > 1]
    cleaned = []
    for c in arr:
        if len(c) > 60:
            break
        cleaned.append(c)
        if len(cleaned) >= 3:
            break
    out = []
    seen = set()
    for c in cleaned:
        k = c.lower()
        if k not in seen:
            seen.add(k)
            out.append(c)
    log_debug(f"Category chain picked: {out}")
    return out

def pick_image(soup):
    og = soup.select_one('meta[property="og:image"]')
    if og and og.get("content"):
        return og["content"]
    for im in soup.select("img"):
        src = im.get("src") or im.get("data-src")
        if src and src.startswith("http"):
            return src
    return None

def pick_price_text(soup):
    cand = soup.find(string=re.compile(r"Giá\s*sản\s*phẩm", re.I))
    if cand and cand.parent:
        return cand.parent.get_text(" ", strip=True)
    return soup.get_text(" ", strip=True)

def parse_product(url: str, headers):
    try:
        r = http_get(url, headers=headers)
    except Exception as e:
        log_error(f"GET {url} -> {e}")
        return None
    if r is None or r.status_code != 200:
        log_warn(f"GET {url} -> status={getattr(r,'status_code',None)}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    h1 = soup.select_one("h1")
    name = h1.get_text(strip=True) if h1 else ""
    price_txt = pick_price_text(soup)
    price = normalize_price_vnd(price_txt)
    desc = None
    marker = soup.find(string=re.compile("Tính năng nổi bật", re.I))
    if marker:
        box = marker.find_parent()
        if box:
            bullets = [el.get_text(" ", strip=True) for el in box.find_all(["li","p"]) if el.get_text(strip=True)]
            desc = " • ".join(bullets[:8]) if bullets else None
    image = pick_image(soup) or ""
    low = soup.get_text(" ", strip=True).lower()
    is_avail = any(k in low for k in ["mua ngay", "thêm vào giỏ", "còn hàng"])
    if "hết hàng" in low or "đặt trước" in low:
        is_avail = False
    crumbs = extract_breadcrumbs(soup)
    cats = pick_category_path_from_breadcrumb(crumbs)

    log_info(f"[PARSE PROD] name={'OK' if name else 'NA'}, "
             f"price={'OK' if price else 'NA'}, img={'OK' if bool(image) else 'NA'}, "
             f"avail={is_avail}, cats={cats}")

    return {
        "url": url,
        "name": name or "",
        "price": price,
        "description": (desc or "")[:1000],
        "image_url": image,
        "is_available": bool(is_avail),
        "category_chain": cats,
    }

# ============================ CATEGORY & CSV ============================

def ensure_category(nodes: dict, chain):
    if not chain:
        return None
    parent_path = None
    built_paths = []
    for depth, name in enumerate(chain, start=1):
        if not name:
            continue
        if depth == 1:
            path = to_path(name)
        else:
            path = to_path(built_paths[-1], name)
        if path not in nodes:
            node = CategoryNode(
                name=name.strip(),
                path=path,
                parent_path=parent_path,
                is_popular=(depth == 1 and to_path(name) in POPULAR_TOP)
            )
            nodes[path] = node
            log_debug(f"[CAT NEW] {node}")
        parent_path = path
        built_paths.append(path)
    return built_paths[-1] if built_paths else None

def topological_categories(nodes: dict):
    items = sorted(nodes.values(), key=lambda n: (n.path.count("/"), n.path))
    return items

def write_categories_csv(nodes: dict, out_path: str):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    cats = topological_categories(nodes)
    log_info(f"WRITE {out_path} — rows={len(cats)}")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "parent_id", "is_popular"])
        for c in cats:
            cid = uuid_cat(c.path)
            pid = uuid_cat(c.parent_path) if c.parent_path else ""
            w.writerow([cid, c.name, pid, "true" if c.is_popular else "false"])

def write_products_csv(rows: list, out_path: str, cat_nodes: dict):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    log_info(f"WRITE {out_path} — rows={len(rows)}")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "price", "description", "image_url", "is_available", "category_id"])
        for p in rows:
            pid = uuid_prod(p["url"])
            last_path = ensure_category(cat_nodes, p.get("category_chain") or []) if p.get("category_chain") else None
            cat_id = uuid_cat(last_path) if last_path else ""
            w.writerow([
                pid,
                p.get("name") or "",
                p.get("price") if p.get("price") is not None else "",
                p.get("description") or "",
                p.get("image_url") or "",
                "true" if p.get("is_available") else "false",
                cat_id
            ])

# ============================ MAIN ============================

def find_product_sitemaps(headers, max_guess=120):
    """Collect entrypoint sitemaps with logs"""
    entrypoints = set()
    common_indexes = [
        f"{BASE}/sitemap.xml",
        f"{BASE}/sitemap_index.xml",
        f"{BASE}/sitemap/sitemap.xml",
        f"{BASE}/sitemap/product-sitemap.xml",
    ]
    for u in common_indexes:
        try:
            r = http_get(u, headers=headers)
            if r is not None and r.status_code == 200:
                log_info(f"[ENTRY] {u} (200)")
                entrypoints.add(u)
            else:
                log_trace(f"[ENTRY] {u} -> {getattr(r,'status_code',None)}")
        except Exception as e:
            log_warn(f"[ENTRY ERR] {u} -> {e}")

    # brute-force numeric patterns and .gz
    for n in range(0, max_guess + 1):
        for p in (
            f"{BASE}/sitemap/product-sitemap{n}.xml",
            f"{BASE}/sitemap/products-sitemap{n}.xml",
            f"{BASE}/sitemap/product-sitemap{n}.xml.gz",
            f"{BASE}/sitemap/products-sitemap{n}.xml.gz",
        ):
            try:
                r = http_get(p, headers=headers)
                if r is not None and r.status_code == 200:
                    log_info(f"[ENTRY] {p} (200)")
                    entrypoints.add(p)
            except Exception as e:
                log_trace(f"[ENTRY ERR] {p} -> {e}")
    return sorted(entrypoints)

def iter_product_urls(headers, limit=None):
    seen = set()
    total = 0
    entries = find_product_sitemaps(headers)
    if not entries:
        log_warn("[SITEMAP] Không tìm thấy entry nào.")
        return
    log_info(f"[SITEMAP] Entry points = {len(entries)}")
    for entry in entries:
        if limit is not None and total >= limit:
            break
        budget = None if limit is None else (limit - total)
        urls = _discover_product_urls_from_sitemap(entry, headers, seen, budget)
        for u in urls:
            log_debug(f"[URL] {u}")
            yield u
            total += 1
            if limit is not None and total >= limit:
                break
    log_info(f"[SITEMAP] Total discovered urls={total}")

def main():
    ap = argparse.ArgumentParser(description="Crawl CellphoneS -> CSV (categories.csv, products.csv)")
    ap.add_argument("--limit", type=int, default=200, help="Giới hạn số sản phẩm (None = toàn bộ)")
    ap.add_argument("--delay", type=float, default=0.35, help="Delay giữa các request (giây)")
    ap.add_argument("--ua", type=str, default=None, help="User-Agent tuỳ chỉnh")
    ap.add_argument("--outdir", type=str, default=".", help="Thư mục xuất CSV")
    ap.add_argument("--verbose", action="store_true", help="Bật DEBUG log")
    ap.add_argument("--trace", action="store_true", help="Bật TRACE log (rất ồn)")
    args = ap.parse_args()

    set_log_level(args.verbose, args.trace)
    headers = HEADERS_TEMPLATE(args.ua)
    outdir = args.outdir.rstrip("/") if args.outdir else "."
    os.makedirs(outdir, exist_ok=True)

    categories = {}
    products = []

    count = 0
    try:
        for url in iter_product_urls(headers, limit=args.limit):
            count += 1
            log_info(f"[{count}] {url}")
            try:
                pdata = parse_product(url, headers=headers)
            except Exception as e:
                log_error(f"[PARSE EXC] {url} -> {e}")
                if LOG_LEVEL >= LEVELS["TRACE"]:
                    traceback.print_exc()
                pdata = None

            if pdata:
                products.append(pdata)
                ensure_category(categories, pdata.get("category_chain") or [])
            time.sleep(args.delay + random.uniform(0.05, 0.25))
    except KeyboardInterrupt:
        log_warn("Interrupted by user (Ctrl+C), flushing partial CSV...")

    cat_csv = f"{outdir}/categories.csv"
    prod_csv = f"{outdir}/products.csv"

    write_categories_csv(categories, cat_csv)
    write_products_csv(products, prod_csv, categories)

    log_info(f"✅ Done. Wrote {cat_csv} & {prod_csv}")
    log_info(f"Categories: {len(categories)} | Products parsed: {len(products)}")

if __name__ == "__main__":
    main()
