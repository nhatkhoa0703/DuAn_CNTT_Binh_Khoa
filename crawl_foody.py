import requests
from bs4 import BeautifulSoup
import pandas as pd
import time, random, re, csv
import numpy as np
from urllib.parse import urlparse

# --------- FUNCTION TO CRAWL 1 URL  ---------
def crawl_foody_reviews(base_url, max_pages=5, session=None, headers=None, delay=(2,5)):
    reviews = []
    session = session or requests.Session()
    headers = headers or {"User-Agent": "Mozilla/5.0"}

    # determine whether to use ?page or &page (in case the url already has query params)
    has_query = urlparse(base_url).query != ""
    page_sep = "&" if has_query else "?"

    for page in range(1, max_pages+1):
        url = f"{base_url}{page_sep}page={page}"
        r = session.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            print("Error:", r.status_code, url)
            break

        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select(".review-item"):
            author = item.select_one(".ru-username")
            rating = item.select_one(".review-points")
            date = item.select_one(".ru-time")
            content = item.select_one(".review-des")

            author_text  = author.text.strip() if author else ""
            rating_text  = rating.text.strip() if rating else ""
            date_text    = date.text.strip() if date else ""
            content_text = content.text.strip() if content else ""

            # Skip placeholders
            if "{{" in author_text or "{{" in rating_text:
                continue

            # Extract place_name more safely
            title = soup.title.text if soup.title else ""
            place_name = title.split("|")[0].strip() if "|" in title else title.strip()

            reviews.append({
                "place_name": place_name,
                "author": author_text,
                "rating": rating_text,
                "date": date_text,
                "text": content_text
            })

        time.sleep(random.uniform(*delay))

    return reviews

# --------- FUNCTION TO CRAWL MULTIPLE URLs (Inherits from the previous function) ---------
def crawl_many_foody(urls, max_pages_each=5, delay=(2,5)):
    sess = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.foody.vn/"
    }

    all_reviews = []
    for u in urls:
        try:
            print(f"Crawling: {u} (max_pages={max_pages_each})")
            data = crawl_foody_reviews(
                base_url=u,
                max_pages=max_pages_each,
                session=sess,
                headers=headers,
                delay=delay
            )
            all_reviews.extend(data)
        except Exception as e:
            print("Error when crawling URL:", u, "->", e)
            continue
    return all_reviews

# ----------------- CLEANING FUNCTION -----------------
def clean_reviews(df):
    df["place_name"] = df["place_name"].astype(str).str.strip()
    df["author"] = df["author"].astype(str).str.strip()
    df["text"] = df["text"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    df["date"] = df["date"].astype(str).str.strip()

    # drop reviews that are too short
    df = df[df["text"].str.len() >= 8]

    # rating -> float
    def parse_rating(x):
        m = re.search(r"(\d+(?:[.,]\d+)?)", str(x))
        return float(m.group(1).replace(",", ".")) if m else np.nan
    df["rating"] = df["rating"].apply(parse_rating)

    # normalize date dd/mm/yyyy -> yyyy-mm-dd
    def normalize_date(s):
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", str(s))
        if m:
            d, mo, y = map(int, m.groups())
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return s if s and str(s).lower() not in ["nan","none"] else np.nan
    df["date"] = df["date"].apply(normalize_date)

    # remove duplicates by place_name + text
    df = df.drop_duplicates(subset=["place_name", "text"], keep="first").reset_index(drop=True)

    # add review_id
    df.insert(0, "review_id", df.index + 1)
    return df

# ----------------- RUN TEST -----------------
if __name__ == "__main__":
    urls = [
        "https://www.foody.vn/ho-chi-minh/quan-an-hue-o-xuan-2/binh-luan",
    ]

    raw = crawl_many_foody(urls, max_pages_each=5, delay=(2,4))
    df_raw = pd.DataFrame(raw)
    print("Raw collected:", len(df_raw))

    if not df_raw.empty:
        df_clean = clean_reviews(df_raw)
        df_clean.to_csv(
            "foody_reviews_multi.csv",
            index=False, encoding="utf-8-sig",
            quoting=csv.QUOTE_ALL, lineterminator="\n"
        )
        print("Raw:", len(df_raw), "-> Clean:", len(df_clean), "reviews (removed duplicates/cleaned)")
    else:
        print("No reviews collected. Check URL/selectors or the site may be blocking.")
