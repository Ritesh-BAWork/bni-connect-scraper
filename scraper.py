# BNI SCRAPER — FINAL STABLE VERSION (FIXED)

import csv, re
from typing import Dict, List, Set
from playwright.sync_api import sync_playwright

# ===== CONFIG =====
BNI_EMAIL    = "your_email"
BNI_PASSWORD = "your_password"

CITY = "Nagpur"

LOGIN_URL  = "https://www.bniconnectglobal.com/login/"
SEARCH_URL = "https://www.bniconnectglobal.com/web/dashboard/search"

CSV_FILE = "bni_members.csv"
HEADLESS = False


# ===== CSV =====
HEADERS = [
    "Name","Chapter","Company","City","Industry",
    "Phone","Email","Website","Address"
]

def init_csv():
    with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow(HEADERS)

def append_csv(row):
    with open(CSV_FILE, "a", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow([row.get(h,"") for h in HEADERS])


# ===== HELPERS =====
def norm(t):
    return re.sub(r"\s+"," ",t or "").strip()

def find_email(t):
    m = re.search(r"[\w\.-]+@[\w\.-]+\.\w+", t or "")
    return m.group(0) if m else ""

def is_phone(t):
    digits = re.sub(r"\D","",t or "")
    return 8 <= len(digits) <= 15


# ===== LOGIN =====
def login(page):
    print("🔐 Logging in...")
    page.goto(LOGIN_URL)
    page.wait_for_timeout(3000)

    page.fill('input[name="username"]', BNI_EMAIL)
    page.fill('input[name="password"]', BNI_PASSWORD)
    page.click('button[type="submit"]')

    page.wait_for_url("**/web/dashboard**", timeout=30000)
    page.wait_for_timeout(3000)

    print("✅ Login success")


# ===== SEARCH =====
def open_search(page):
    print("🔍 Opening search page...")
    page.goto(SEARCH_URL)
    page.wait_for_timeout(5000)

    page.fill('input[type="search"]', CITY)
    page.press('input[type="search"]', "Enter")

    page.wait_for_timeout(5000)
    print("✅ Search done")


# ===== GET MEMBERS =====
def get_members(page):
    return page.evaluate("""
    () => {
        const rows = document.querySelectorAll('div.css-1rb62l');
        const result = [];

        rows.forEach(row => {
            const link = row.querySelector('a[href*="networkHome"]');
            if(!link) return;

            const name = link.innerText.trim();
            let href = link.getAttribute("href");
            if(href.startsWith("/"))
                href = "https://www.bniconnectglobal.com" + href;

            const texts = Array.from(row.children)
                .map(x => x.innerText.trim())
                .filter(x => x && x !== name && x !== "+" && x !== "Connect");

            result.push({
                name: name,
                href: href,
                chapter: texts.find(x => x.includes("BNI")) || "",
                city: texts.find(x => x.toLowerCase()=="nagpur") || "Nagpur",
                industry: texts.find(x => x.includes(">")) || "",
                company: texts[0] || ""
            });
        });

        return result;
    }
    """)


# ===== PROFILE =====
def extract_profile(page):
    text = page.inner_text("body")

    return {
        "Phone": "",
        "Email": find_email(text),
        "Website": "",
        "Address": ""
    }


# ===== MAIN =====
def main():
    init_csv()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=200)
        page = browser.new_page()

        login(page)
        open_search(page)

        members = get_members(page)
        print(f"👥 Found: {len(members)} members")

        for i, m in enumerate(members):
            print(f"\n➡️ {i+1}. {m['name']}")

            try:
                page.goto(m["href"])
                page.wait_for_timeout(3000)
            except:
                print("❌ Profile failed")
                continue

            prof = extract_profile(page)

            row = {
                "Name": m["name"],
                "Chapter": m["chapter"],
                "Company": m["company"],
                "City": m["city"],
                "Industry": m["industry"],
                "Phone": prof["Phone"],
                "Email": prof["Email"],
                "Website": prof["Website"],
                "Address": prof["Address"]
            }

            append_csv(row)

            page.go_back()
            page.wait_for_timeout(2000)

        print("\n🎉 DONE")

        input("Press Enter to exit...")
        browser.close()


if __name__ == "__main__":
    main()
