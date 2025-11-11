#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import re
import time
import random
import unicodedata
import uuid
from collections import defaultdict, OrderedDict
from dataclasses import dataclass
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

BASE = "https://cellphones.com.vn"
HEADERS_TEMPLATE = lambda ua: {
    "User-Agent": ua or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

POPULAR_TOP = {
    "điện thoại", "tablet", "laptop", "âm thanh", "đồng hồ",
    "phụ kiện", "tivi", "pc", "màn hình", "gia dụng", "camera", "điện máy"
}

# ------------------------- utils -------------------------

def log(msg):
    print(msg, flush=True)

def http_get(url, headers, timeout=25, max_retry=4):
    for attempt in range(max_retry):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code in (200, 404):
                return r
        except requests.RequestException:
            pass
        sleep = (attempt + 1) * 1.2 + random.uniform(0.0, 0.6)
        time.sleep(sleep)
    # final try raises
    r = requests.get(url, headers=headers, timeout=timeout)
    return r

def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = s.strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # remove accents
    # keep letters, numbers, separators
    s = re.sub(r"[^a-zA-Z0-9\s\/\-\|]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def to_path(*parts: str) -> str:
    cleaned = []
    for p in parts:
        p = norm_text(p).lower().replace(" ", "-").strip("-/|")
        if p:
            cleaned.append(p)
    return "/".join(cleaned)

def uuid_cat(path: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"cellphones:/cat/{path}"))

def uuid_prod(url: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"cellphones:/prod/{url}"))

def normalize_price_vnd(text: str):
    if not text:
        return None
    # ví dụ: "27.280.000đ" hoặc "27.280.000 đ"
    m = re.search(r"(\d[\d\.]+)\s*đ", text.replace(",", ".").lower())
    if not m:
        return None
    v = m.group(1).replace(".", "")
    try:
        return float(v)
    except Exception:
        return None

# ------------------------- models -------------------------

@dataclass
class CategoryNode:
    name: str
    path: str  # dien-thoai/samsung
    parent_path: str | None
    is_popular: bool

# ------------------------- sitemap discovery -------------------------

def find_product_sitemaps(headers):
    """
    Cách 1: đọc /sitemap.xml và lọc các sitemap con có 'product' trong tên.
    Cách 2 (fallback): thử tuần tự /sitemap/product-sitemap{n}.xml cho đến khi 404.
    """
    sitemaps = []

    # try main index
    idx = http_get(f"{BASE}/sitemap.xml", headers=headers)
    if idx.status_code == 200 and "<sitemapindex" in idx.text:
        soup = BeautifulSoup(idx.text, "xml")
        for sm in soup.find_all("sitemap"):
            loc = (sm.loc or "").text.strip() if sm.loc else ""
            if "product" in loc:
                sitemaps.append(loc)

    if sitemaps:
        return sitemaps

    # fallback numeric
    n = 1
    while True:
        url = f"{BASE}/sitemap/product-sitemap{n}.xml"
        r = http_get(url, headers=headers)
        if r.status_code == 404:
            break
        if r.status_code == 200 and "<urlset" in r.text:
            sitemaps.append(url)
        n += 1
    return sitemaps

def iter_product_urls(headers, limit=None):
    count = 0
    for sm in find_product_sitemaps(headers):
        log(f"[SITEMAP] {sm}")
        r = http_get(sm, headers=headers)
        if r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "xml")
        for loc in soup.find_all("loc"):
            url = loc.text.strip()
            if not url:
                continue
            yield url
            count += 1
            if limit and count >= limit:
                return

# ------------------------- product parsing -------------------------

def extract_breadcrumbs(soup: BeautifulSoup):
    """
    Trả về danh sách tên breadcrumb (bỏ 'Trang chủ' nếu có).
    Tìm theo class/aria-label chứa 'breadcrumb'.
    """
    crumbs = []
    # candidates
    cands = []
    cands.extend(soup.select('[aria-label*="breadcrumb" i]'))
    cands.extend(soup.select('[role="navigation"][aria-label*="breadcrumb" i]'))
    cands.extend(soup.select('.breadcrumb, .breadcrumbs, nav.breadcrumb'))
    cands = [c for c in cands if c]

    if not cands:
        return crumbs

    nav = cands[0]
    # common: li > a / span
    items = nav.select("li, a, span")
    for it in items:
        t = it.get_text(" ", strip=True)
        t = t.replace("Trang chủ", "").strip()
        if t:
            crumbs.append(t)
    # dedupe keep order
    seen = set()
    out = []
    for c in crumbs:
        k = c.lower()
        if k not in seen:
            seen.add(k)
            out.append(c)
    return out

def pick_category_path_from_breadcrumb(crumbs: list[str]) -> list[str]:
    """
    Lấy chuỗi danh mục từ breadcrumb:
    - bỏ các hạt nhiễu (Trang chủ, Tin tức… nếu có)
    - thường 1-2 cấp đầu là category chính, ví dụ: ["Điện thoại", "Samsung Galaxy", ...]
    """
    if not crumbs:
        return []
    # keep first 2-3 meaningful tokens
    arr = [c for c in crumbs if c and len(c) > 1]
    # heuristic: cắt đến khi gặp tên sản phẩm (dài > 35 ký tự?)
    cleaned = []
    for c in arr:
        if len(c) > 60:  # rất dài -> likely product name
            break
        cleaned.append(c)
        if len(cleaned) >= 3:
            break
    # remove duplicates like "Điện thoại | Điện thoại"
    out = []
    seen = set()
    for c in cleaned:
        k = c.lower()
        if k not in seen:
            seen.add(k)
            out.append(c)
    return out

def pick_image(soup: BeautifulSoup):
    # og:image first
    og = soup.select_one('meta[property="og:image"]')
    if og and og.get("content"):
        return og["content"]
    # first visible img
    for im in soup.select("img"):
        src = im.get("src") or im.get("data-src")
        if src and src.startswith("http"):
            return src
    return None

def pick_price_text(soup: BeautifulSoup):
    # ưu tiên vùng chứa từ "Giá sản phẩm"
    cand = soup.find(string=re.compile(r"Giá\s*sản\s*phẩm", re.I))
    if cand and cand.parent:
        txt = cand.parent.get_text(" ", strip=True)
        return txt
    # fallback: toàn trang
    return soup.get_text(" ", strip=True)

def parse_product(url: str, headers):
    r = http_get(url, headers=headers)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")

    # name
    h1 = soup.select_one("h1")
    name = h1.get_text(strip=True) if h1 else None

    # price
    price_txt = pick_price_text(soup)
    price = normalize_price_vnd(price_txt)

    # description: "Tính năng nổi bật"
    desc = None
    marker = soup.find(string=re.compile("Tính năng nổi bật", re.I))
    if marker:
        box = marker.find_parent()
        if box:
            bullets = [el.get_text(" ", strip=True) for el in box.find_all(["li","p"]) if el.get_text(strip=True)]
            desc = " • ".join(bullets[:8]) if bullets else None

    # image
    image = pick_image(soup)

    # availability
    low = soup.get_text(" ", strip=True).lower()
    is_avail = any(k in low for k in ["mua ngay", "thêm vào giỏ", "còn hàng"])
    if "hết hàng" in low or "đặt trước" in low:
        is_avail = False

    # categories from breadcrumb
    crumbs = extract_breadcrumbs(soup)
    cats = pick_category_path_from_breadcrumb(crumbs)

    return {
        "url": url,
        "name": name,
        "price": price,
        "description": (desc or "")[:1000],
        "image_url": image or "",
        "is_available": bool(is_avail),
        "category_chain": cats,  # e.g. ["Điện thoại","Samsung Galaxy"]
    }

# ------------------------- category building -------------------------

def ensure_category(nodes: dict[str, CategoryNode], chain: list[str]):
    """
    Từ chain ["Điện thoại","Samsung Galaxy"] tạo:
      - top: path = "dien-thoai", parent=None
      - sub:  path = "dien-thoai/samsung-galaxy", parent="dien-thoai"
    """
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
                is_popular=(depth == 1 and norm_text(name).lower() in POPULAR_TOP)
            )
        parent_path = path
        built_paths.append(path)
    return built_paths[-1] if built_paths else None

def topological_categories(nodes: dict[str, CategoryNode]):
    """
    Sắp theo thứ tự cha trước con để ghi CSV đẹp.
    """
    # sort by depth then lexicographic
    items = sorted(nodes.values(), key=lambda n: (n.path.count("/"), n.path))
    return items

# ------------------------- CSV writers -------------------------

def write_categories_csv(nodes: dict[str, CategoryNode], out_path: str):
    cats = topological_categories(nodes)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "parent_id", "is_popular"])
        for c in cats:
            cid = uuid_cat(c.path)
            pid = uuid_cat(c.parent_path) if c.parent_path else ""
            w.writerow([cid, c.name, pid, "true" if c.is_popular else "false"])

def write_products_csv(rows: list[dict], out_path: str, cat_nodes: dict[str, CategoryNode]):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "price", "description", "image_url", "is_available", "category_id"])
        for p in rows:
            pid = uuid_prod(p["url"])
            # map category
            cat_chain = p.get("category_chain") or []
            # tạo category nếu chưa có trong dict (đề phòng sp có chain mới)
            last_path = ensure_category(cat_nodes, cat_chain) if cat_chain else None
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

# ------------------------- main -------------------------

def main():
    ap = argparse.ArgumentParser(description="Crawl CellphoneS -> CSV (categories.csv, products.csv)")
    ap.add_argument("--limit", type=int, default=200, help="Giới hạn số sản phẩm (None = toàn bộ)")
    ap.add_argument("--delay", type=float, default=0.35, help="Delay giữa các request (giây)")
    ap.add_argument("--ua", type=str, default=None, help="User-Agent tuỳ chỉnh")
    ap.add_argument("--outdir", type=str, default=".", help="Thư mục xuất CSV")
    args = ap.parse_args()

    headers = HEADERS_TEMPLATE(args.ua)
    os.makedirs(args.outdir, exist_ok=True)
    categories: dict[str, CategoryNode] = {}
    products: list[dict] = []

    count = 0
    for url in iter_product_urls(headers, limit=args.limit):
        count += 1
        log(f"[{count}] {url}")
        pdata = parse_product(url, headers=headers)
        if pdata:
            products.append(pdata)
            # build category nodes incrementally
            ensure_category(categories, pdata.get("category_chain") or [])
        time.sleep(args.delay + random.uniform(0.05, 0.25))

    # write CSV
    cat_csv = f"{args.outdir.rstrip('/')}/categories.csv"
    prod_csv = f"{args.outdir.rstrip('/')}/products.csv"

    write_categories_csv(categories, cat_csv)
    write_products_csv(products, prod_csv, categories)

    log(f"✅ Done. Wrote {cat_csv} & {prod_csv}")
    log(f"Categories: {len(categories)} | Products parsed: {len(products)}")

if __name__ == "__main__":
    main()
