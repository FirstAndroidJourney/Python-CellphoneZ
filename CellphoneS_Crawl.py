#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawl CellphoneS -> CSV (categories.csv, products.csv)

Features:
- Tìm product-sitemap (đa dạng pattern), đệ quy sitemapindex -> urlset
- Giải nén .xml.gz nếu cần
- Parse trang sản phẩm (name, price, description, image, availability, breadcrumb)
- Tạo UUID v5 ổn định cho category/product (không cần slug)
- Xuất CSV: categories.csv (id,name,parent_id,is_popular) và products.csv (id,name,price,description,image_url,is_available,category_id)
"""

import argparse
import csv
import gzip
import io
import os
import random
import re
import time
import unicodedata
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ---------------- config ----------------
BASE = "https://cellphones.com.vn"

HEADERS_TEMPLATE = lambda ua: {
    "User-Agent": ua or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "application/xml, text/xml;q=0.9, */*;q=0.8",
}

POPULAR_TOP = {
    "dien-thoai", "tablet", "laptop", "am-thanh", "dong-ho",
    "phu-kien", "tivi", "pc", "man-hinh", "gia-dung", "camera", "dien-may"
}

# ---------------- util ----------------
def log(msg: str):
    print(msg, flush=True)

def http_get(url, headers, timeout=25, max_retry=4):
    """Simple GET with retries"""
    last = None
    for attempt in range(max_retry):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            return r
        except requests.RequestException as e:
            last = e
            sleep = (attempt + 1) * 1.2 + random.uniform(0.0, 0.6)
            time.sleep(sleep)
    # final try (let exception raise)
    return requests.get(url, headers=headers, timeout=timeout)

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
        if not p: continue
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

# ---------------- models ----------------
@dataclass
class CategoryNode:
    name: str
    path: str
    parent_path: str | None
    is_popular: bool

# ---------------- sitemap helpers ----------------
def _fetch_xml_text(url, headers, timeout=25):
    """Fetch content and decode xml or xml.gz, return (text, status)"""
    r = http_get(url, headers=headers, timeout=timeout)
    if r is None:
        return None, 0
    if r.status_code != 200:
        return None, r.status_code
    content = r.content or b""
    # decompress if gz
    if url.lower().endswith(".gz") or r.headers.get("Content-Encoding","").lower().find("gzip") != -1 or r.headers.get("Content-Type","").lower().find("gzip") != -1:
        try:
            content = gzip.decompress(content)
        except Exception:
            try:
                content = gzip.GzipFile(fileobj=io.BytesIO(r.content)).read()
            except Exception:
                pass
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        text = r.text
    return text, r.status_code

def _extract_locs_from_xml(xml_text):
    """Return (type, locs) where type is 'sitemapindex' or 'urlset' or 'unknown'"""
    if not xml_text:
        return "unknown", []
    soup = BeautifulSoup(xml_text, "xml")
    if soup.find("sitemapindex"):
        locs = [(el.text or "").strip() for el in soup.find_all("loc") if (el.text or "").strip()]
        return "sitemapindex", locs
    if soup.find("urlset"):
        locs = [(el.text or "").strip() for el in soup.find_all("loc") if (el.text or "").strip()]
        return "urlset", locs
    locs = [(el.text or "").strip() for el in soup.find_all("loc") if (el.text or "").strip()]
    if locs:
        if soup.find_all("url"):
            return "urlset", locs
        if soup.find_all("sitemap"):
            return "sitemapindex", locs
    return "unknown", []

def _discover_product_urls_from_sitemap(url, headers, seen, budget_left):
    if url in seen:
        return []
    seen.add(url)

    xml_text, status = _fetch_xml_text(url, headers=headers)
    log(f"  [FETCH] {url} -> status={status}, bytes={len(xml_text or '')}")

    if status != 200 or not xml_text:
        return []

    typ, locs = _extract_locs_from_xml(xml_text)
    log(f"  [PARSE] {url} -> type={typ}, locs={len(locs)}")

    results = []
    if typ == "sitemapindex":
        for loc in locs:
            if budget_left is not None and len(results) >= budget_left:
                break
            nxt = urljoin(url, loc)
            log(f"    [RECURSE] sitemap -> {nxt}")
            sub = _discover_product_urls_from_sitemap(
                nxt, headers, seen,
                None if budget_left is None else (budget_left - len(results))
            )
            results.extend(sub)
        return results

    if typ == "urlset":
        for loc in locs:
            if budget_left is not None and len(results) >= budget_left:
                break
            if loc.lower().endswith(".html"):
                results.append(loc)
        log(f"  [URLSET] {url} -> collected={len(results)}")
        return results

    log(f"  [UNKNOWN] {url} -> no <urlset>/<sitemapindex> found")
    return []

def find_product_sitemaps(headers, max_guess=120):
    """Gather entrypoint sitemap URLs (index or direct product sitemaps)"""
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
                entrypoints.add(u)
        except Exception:
            pass
    # brute-force numeric patterns and .gz variants
    for n in range(0, max_guess + 1):
        for p in (f"{BASE}/sitemap/product-sitemap{n}.xml",
                  f"{BASE}/sitemap/products-sitemap{n}.xml",
                  f"{BASE}/sitemap/product-sitemap{n}.xml.gz",
                  f"{BASE}/sitemap/products-sitemap{n}.xml.gz"):
            try:
                r = http_get(p, headers=headers)
                if r is not None and r.status_code == 200:
                    entrypoints.add(p)
            except Exception:
                pass
    return sorted(entrypoints)

def iter_product_urls(headers, limit=None):
    seen = set()
    total = 0
    entries = find_product_sitemaps(headers)
    if not entries:
        log("[WARN] Không tìm thấy entry sitemap nào.")
        return
    for entry in entries:
        if limit is not None and total >= limit:
            break
        log(f"[SITEMAP] {entry}")
        budget = None if limit is None else (limit - total)
        try:
            urls = _discover_product_urls_from_sitemap(entry, headers, seen, budget)
        except Exception as e:
            log(f"[WARN] error walking {entry}: {e}")
            urls = []
        for u in urls:
            yield u
            total += 1
            if limit is not None and total >= limit:
                break

# ---------------- product parsing ----------------
def extract_breadcrumbs(soup: BeautifulSoup):
    crumbs = []
    cands = []
    cands.extend(soup.select('[aria-label*="breadcrumb" i]'))
    cands.extend(soup.select('[role="navigation"][aria-label*="breadcrumb" i]'))
    cands.extend(soup.select('.breadcrumb, .breadcrumbs, nav.breadcrumb'))
    cands = [c for c in cands if c]
    if not cands:
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
        log(f"[ERROR] GET {url} -> {e}")
        return None
    if r is None or r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    h1 = soup.select_one("h1")
    name = h1.get_text(strip=True) if h1 else None
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
    return {
        "url": url,
        "name": name or "",
        "price": price,
        "description": (desc or "")[:1000],
        "image_url": image,
        "is_available": bool(is_avail),
        "category_chain": cats,
    }

# ---------------- categories builder & csv writers ----------------
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
            nodes[path] = CategoryNode(
                name=name.strip(),
                path=path,
                parent_path=parent_path,
                is_popular=(depth == 1 and to_path(name) in POPULAR_TOP)
            )
        parent_path = path
        built_paths.append(path)
    return built_paths[-1] if built_paths else None

def topological_categories(nodes: dict):
    items = sorted(nodes.values(), key=lambda n: (n.path.count("/"), n.path))
    return items

def write_categories_csv(nodes: dict, out_path: str):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    cats = topological_categories(nodes)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "parent_id", "is_popular"])
        for c in cats:
            cid = uuid_cat(c.path)
            pid = uuid_cat(c.parent_path) if c.parent_path else ""
            w.writerow([cid, c.name, pid, "true" if c.is_popular else "false"])

def write_products_csv(rows: list, out_path: str, cat_nodes: dict):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
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

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser(description="Crawl CellphoneS -> CSV (categories.csv, products.csv)")
    ap.add_argument("--limit", type=int, default=200, help="Giới hạn số sản phẩm (None = toàn bộ)")
    ap.add_argument("--delay", type=float, default=0.35, help="Delay giữa các request (giây)")
    ap.add_argument("--ua", type=str, default=None, help="User-Agent tuỳ chỉnh")
    ap.add_argument("--outdir", type=str, default=".", help="Thư mục xuất CSV")
    args = ap.parse_args()

    headers = HEADERS_TEMPLATE(args.ua)
    outdir = args.outdir.rstrip("/") if args.outdir else "."
    os.makedirs(outdir, exist_ok=True)

    categories = {}
    products = []

    count = 0
    for url in iter_product_urls(headers, limit=args.limit):
        count += 1
        log(f"[{count}] {url}")
        pdata = parse_product(url, headers=headers)
        if pdata:
            products.append(pdata)
            ensure_category(categories, pdata.get("category_chain") or [])
        time.sleep(args.delay + random.uniform(0.05, 0.25))

    cat_csv = f"{outdir}/categories.csv"
    prod_csv = f"{outdir}/products.csv"

    write_categories_csv(categories, cat_csv)
    write_products_csv(products, prod_csv, categories)

    log(f"✅ Done. Wrote {cat_csv} & {prod_csv}")
    log(f"Categories: {len(categories)} | Products parsed: {len(products)}")

if __name__ == "__main__":
    main()