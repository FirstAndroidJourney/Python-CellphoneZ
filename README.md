
# 📦 CellphoneS Product Crawler (BeautifulSoup + CSV Export)

A Python crawler for extracting **product and category data** from [CellphoneS.com.vn](https://cellphones.com.vn/).  
It uses `requests` and `BeautifulSoup` to collect structured product information and export clean **CSV files** ready for import into **Supabase** or any other backend system.

---

## 🚀 Key Features

✅ Automatically discovers and parses all `product-sitemap.xml` and `.xml.gz` files  
✅ Recursively follows sitemap indexes → urlsets  
✅ Extracts **name, price, description, image, availability, and breadcrumb (category)**  
✅ Builds a hierarchical **category tree** dynamically from breadcrumbs  
✅ Generates **deterministic UUID v5** IDs for both categories and products (no duplicates, no slug needed)  
✅ Exports two clean CSV files:
- `categories.csv` — id, name, parent_id, is_popular  
- `products.csv` — id, name, price, description, image_url, is_available, category_id  

---

## 🧱 Repository Structure
```

📂 Python-CellphoneZ/
├── CellphoneS_Crawl.py     # Main crawler script
├── export/                 # Output directory (auto-created)
└── README.md               # This file

````

---

## 🧩 Requirements

- Python **>= 3.10**
- Install dependencies:
```bash
pip install requests beautifulsoup4 lxml
````

---

## ⚙️ How to Use

### 1️⃣ Basic run

```bash
python CellphoneS_Crawl.py --limit 500 --outdir ./export
```

### 2️⃣ Optional arguments

| Flag       | Description                                  | Default           |
| ---------- | -------------------------------------------- | ----------------- |
| `--limit`  | Max number of products to crawl (None = all) | 200               |
| `--outdir` | Output folder for CSV files                  | `.`               |
| `--delay`  | Delay between requests (seconds)             | 0.35              |
| `--ua`     | Custom User-Agent string                     | default Chrome UA |

Example:

```bash
python CellphoneS_Crawl.py --limit 1000 --delay 0.6 --ua "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
```

Expected output:

```
✅ Done. Wrote ./export/categories.csv & ./export/products.csv
Categories: 80 | Products parsed: 500
```

---

## 📄 Output File Details

### 🗂 categories.csv

| Column     | Type      | Description                                          |
| ---------- | --------- | ---------------------------------------------------- |
| id         | uuid      | UUID v5 generated from category path                 |
| name       | text      | Category name (e.g. “Smartphones”, “Samsung Galaxy”) |
| parent_id  | uuid/null | Parent category UUID (or null for root)              |
| is_popular | boolean   | true if part of the top navigation                   |

### 📦 products.csv

| Column       | Type             | Description                                         |
| ------------ | ---------------- | --------------------------------------------------- |
| id           | uuid             | UUID v5 generated from product URL                  |
| name         | text             | Product name (H1 title)                             |
| price        | double precision | Price (converted from “27.280.000đ” → `27280000.0`) |
| description  | text             | Concatenated “Key features” section                 |
| image_url    | text             | Product thumbnail or `og:image`                     |
| is_available | boolean          | true if “MUA NGAY” or “Add to cart” visible         |
| category_id  | uuid             | Foreign key to matching category                    |

---

## 🧠 UUID v5 Strategy

To ensure stable, repeatable IDs across multiple runs:

```
category.id = uuid5(NAMESPACE_URL, "cellphones:/cat/" + category_path)
product.id  = uuid5(NAMESPACE_URL, "cellphones:/prod/" + product_url)
```

→ Guarantees **no duplicates** and **no ID drift** even if crawled at different times.

---

## 🔍 How It Works

1. **Discover sitemaps** (`product-sitemap*.xml` / `.xml.gz`)
   → Recursively traverse sitemap indexes to get all product URLs
2. **Iterate through product pages** and extract:

   * H1 name
   * Price block (“Giá sản phẩm”)
   * Description (“Tính năng nổi bật”)
   * Image (gallery or Open Graph)
   * Availability (based on button text)
   * Breadcrumbs for category mapping
3. **Generate UUIDs** and build a category tree in memory
4. **Export as CSV** with proper relational foreign keys.

---

## 🧰 Technical Notes

| Situation                                 | Behavior                           |
| ----------------------------------------- | ---------------------------------- |
| Sitemap uses `.xml.gz`                    | Script auto-decompresses           |
| Nested sitemap indexes                    | Fully recursive                    |
| Breadcrumb noise (“Trang chủ”, “Tin tức”) | Automatically filtered             |
| Vietnamese currency                       | Regex-normalized to numeric format |
| Anti-bot defenses                         | Add `--ua` or increase `--delay`   |
| Missing output folder                     | Automatically created              |

---

## 🧾 Sample Output (simplified)

**categories.csv**

```
id,name,parent_id,is_popular
3b8...,"Smartphones",,true
2a5...,"Samsung Galaxy",3b8...,false
```

**products.csv**

```
id,name,price,description,image_url,is_available,category_id
7d1...,"Samsung Galaxy S25 Ultra",27280000,"• Premium titanium frame • 200MP camera",https://cdn.cellphones.com.vn/media/catalog/product/s25ultra.jpg,true,2a5...
```

---

## 🧪 Quick Test

```bash
python CellphoneS_Crawl.py --limit 10 --outdir ./export --delay 0.5
```

---

## 🧤 License

**MIT License** — free to use, modify, and distribute for learning, research, or personal projects.

---

## 👨‍💻 Author

**TheKhiem7**

> Flutter + Supabase + Python stack enthusiast 🐍📱

---

### ⭐ If you find this useful, give the repo a Star!