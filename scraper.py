import os
import csv
import json
import time
import re
import requests
from playwright.sync_api import sync_playwright

# ================= CONFIG =================
BNI_EMAIL = os.getenv("BNI_EMAIL", "").strip()
BNI_PASSWORD = os.getenv("BNI_PASSWORD", "").strip()
BNI_CITIES = [c.strip() for c in os.getenv("BNI_CITIES", "Nagpur").split(",") if c.strip()]

GOOGLE_WEBAPP_URL = os.getenv("GOOGLE_WEBAPP_URL", "").strip()
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

CSV_FILE = os.getenv("CSV_FILE", "bni_members.csv")
PROGRESS_FILE = os.getenv("PROGRESS_FILE", "progress_state.json")

# ================= CSV =================
def init_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "City","Name","Phone","Email","Website","Address","Classification"
            ])

def append_csv(row):
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)

# ================= PROGRESS =================
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        return json.load(open(PROGRESS_FILE))
    return {"completed_cities": []}

def save_progress(p):
    json.dump(p, open(PROGRESS_FILE, "w"))

# ================= GOOGLE =================
def push_to_sheet(row):
    if not GOOGLE_WEBAPP_URL or "YOUR" in GOOGLE_WEBAPP_URL:
        return
    try:
        requests.post(GOOGLE_WEBAPP_URL, json=row, timeout=10)
    except:
        pass

# ================= LOGIN =================
def login(page):
    print("🔐 Login...")
    page.goto("https://www.bniconnectglobal.com/login/")
    page.fill('input[name="username"]', BNI_EMAIL)
    page.fill('input[name="password"]', BNI_PASSWORD)
    page.click('button[type="submit"]')
    page.wait_for_timeout(5000)
    print("✅ Login done")

# ================= SEARCH =================
def open_search(page):
    page.goto("https://www.bniconnectglobal.com/web/dashboard/search")
    page.wait_for_timeout(5000)

# ================= 🔥 FIXED EXTRACTION =================
def extract_data(page):
    data = {"phone":"","email":"","website":"","address":"","classification":""}

    try:
        page.wait_for_timeout(3000)

        # scroll (important)
        for _ in range(3):
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(1000)

        text = page.inner_text("body")

        # EMAIL
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
        if emails:
            data["email"] = emails[0]

        # PHONE
        phones = re.findall(r"\+?\d[\d\s\-]{8,15}", text)
        if phones:
            data["phone"] = " / ".join(list(set(phones[:2])))

        # WEBSITE
        sites = re.findall(r"https?://[^\s]+", text)
        if sites:
            data["website"] = sites[0]

        # ADDRESS
        if "Address" in text:
            try:
                data["address"] = text.split("Address")[1].split("\n")[1][:120]
            except:
                pass

        # CLASSIFICATION
        if "Classification" in text:
            try:
                data["classification"] = text.split("Classification")[1].split("\n")[1]
            except:
                pass

    except Exception as e:
        print("⚠️ Extract error:", e)

    return data

# ================= PROCESS CITY =================
def process_city(page, city, progress):
    print(f"\n🌍 {city}")
    open_search(page)

    page.fill("input", city)
    page.keyboard.press("Enter")
    page.wait_for_timeout(5000)

    seen = set()

    while True:
        members = page.query_selector_all("a[href*='member']")

        for m in members:
            name = m.inner_text().strip()

            if name in seen:
                continue
            seen.add(name)

            try:
                m.click()
                page.wait_for_timeout(4000)

                # retry extraction
                for i in range(2):
                    d = extract_data(page)
                    if d["email"] or d["phone"]:
                        break
                    page.wait_for_timeout(2000)

                row = [city, name, d["phone"], d["email"], d["website"], d["address"], d["classification"]]

                append_csv(row)

                push_to_sheet({
                    "city": city,
                    "name": name,
                    **d
                })

                print("✅", name)

                page.go_back()
                page.wait_for_timeout(3000)

            except Exception as e:
                print("❌ Error:", name)
                page.go_back()
                page.wait_for_timeout(3000)

        # scroll to load more
        before = len(seen)
        page.mouse.wheel(0, 5000)
        page.wait_for_timeout(3000)

        if len(seen) == before:
            break

    progress["completed_cities"].append(city)
    save_progress(progress)

# ================= MAIN =================
def main():
    if not BNI_EMAIL or not BNI_PASSWORD:
        print("❌ Set env variables")
        return

    init_csv()
    progress = load_progress()

    remaining = [c for c in BNI_CITIES if c not in progress["completed_cities"]]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        login(page)

        for city in remaining:
            try:
                process_city(page, city, progress)
            except Exception as e:
                print("❌ City fail:", city)
                break

        browser.close()

# ================= RUN =================
if __name__ == "__main__":
    main()
