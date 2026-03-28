import csv
import os
import re
import time
from typing import Dict, List, Optional, Set

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# =========================================================
# CONFIG
# =========================================================
BNI_EMAIL = os.getenv("BNI_EMAIL", "").strip()
BNI_PASSWORD = os.getenv("BNI_PASSWORD", "").strip()
BNI_CITIES = [c.strip() for c in os.getenv("BNI_CITIES", "Nagpur").split(",") if c.strip()]

LOGIN_URL = "https://www.bniconnectglobal.com/login/"
DASHBOARD_URL = "https://www.bniconnectglobal.com/web/dashboard"
SEARCH_URL = "https://www.bniconnectglobal.com/web/dashboard/search"

HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
SLOW_MO = int(os.getenv("SLOW_MO", "250"))

CSV_FILE = os.getenv("CSV_FILE", "bni_members.csv")

GOOGLE_WEBAPP_URL = os.getenv("GOOGLE_WEBAPP_URL", "").strip()

# optional debugging
DEBUG_HTML = os.getenv("DEBUG_HTML", "true").lower() == "true"


# =========================================================
# OUTPUT COLUMNS
# =========================================================
HEADERS = [
    "Search City",
    "Name",
    "Chapter",
    "Company",
    "City",
    "Industry and Classification",
    "Profile URL",
    "Phone",
    "Email",
    "Website",
    "Address",
    "Professional Classification",
    "Business Description",
]


# =========================================================
# HELPERS
# =========================================================
def norm(t: str) -> str:
    return re.sub(r"\s+", " ", t or "").strip()


def find_email(t: str) -> str:
    m = re.search(r"[\w\.-]+@[\w\.-]+\.\w+", t or "")
    return m.group(0) if m else ""


def is_phone(t: str) -> bool:
    if re.search(r"\d{2}/\d{2}/\d{4}", t or ""):
        return False
    digits = re.sub(r"\D", "", t or "")
    return 8 <= len(digits) <= 15


def save_html(page, filename: str) -> None:
    if not DEBUG_HTML:
        return
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"💾 Saved HTML: {filename}")
    except Exception as e:
        print(f"⚠️ Could not save HTML {filename}: {e}")


def init_csv() -> None:
    with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow(HEADERS)
    print(f"📄 CSV ready: {CSV_FILE}")


def append_csv(row: Dict) -> None:
    with open(CSV_FILE, "a", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow([row.get(h, "") for h in HEADERS])


def post_to_google_sheet_apps_script(row: Dict) -> None:
    if not GOOGLE_WEBAPP_URL or GOOGLE_WEBAPP_URL == "YOUR_URL":
        return
    try:
        payload = {"rows": [row]}
        r = requests.post(GOOGLE_WEBAPP_URL, json=payload, timeout=60)
        print(f"📤 Apps Script POST: {r.status_code}")
    except Exception as e:
        print(f"⚠️ Apps Script POST failed: {e}")


def click_first_visible(page, selectors: List[str], timeout_ms: int = 3000) -> bool:
    for sel in selectors:
        try:
            locator = page.locator(sel)
            count = locator.count()
            for i in range(count):
                try:
                    el = locator.nth(i)
                    if el.is_visible(timeout=1000):
                        el.click(timeout=timeout_ms)
                        return True
                except Exception:
                    pass
        except Exception:
            pass
    return False


def fill_first_visible(page, selectors: List[str], value: str, press_enter: bool = False) -> bool:
    for sel in selectors:
        try:
            locator = page.locator(sel)
            count = locator.count()
            for i in range(count):
                try:
                    el = locator.nth(i)
                    if el.is_visible(timeout=1000):
                        el.click(timeout=2000)
                        el.fill("")
                        el.fill(value)
                        if press_enter:
                            el.press("Enter")
                        return True
                except Exception:
                    pass
        except Exception:
            pass
    return False


# =========================================================
# LOGIN
# =========================================================
def login(page) -> None:
    print("\n🔐 Logging in...")
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(2500)
    save_html(page, "debug_login_page.html")

    email_ok = fill_first_visible(
        page,
        [
            'input[name="username"]',
            'input[name="email"]',
            'input[type="email"]',
            'input[placeholder*="Email"]',
            'input[placeholder*="Username"]',
        ],
        BNI_EMAIL,
    )
    if not email_ok:
        raise Exception("Could not find email/username input")

    password_ok = fill_first_visible(
        page,
        [
            'input[name="password"]',
            'input[type="password"]',
        ],
        BNI_PASSWORD,
    )
    if not password_ok:
        raise Exception("Could not find password input")

    clicked = click_first_visible(
        page,
        [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("Sign In")',
            'button:has-text("Login")',
            'button:has-text("SIGN IN")',
        ],
        timeout_ms=5000,
    )
    if not clicked:
        raise Exception("Could not click login button")

    try:
        page.wait_for_url("**/web/dashboard**", timeout=30000)
    except PlaywrightTimeoutError:
        save_html(page, "debug_login_failed_after_submit.html")
        raise Exception(f"Login did not reach dashboard. Current URL: {page.url}")

    page.wait_for_timeout(3000)
    save_html(page, "debug_after_login.html")
    print(f"✅ Login successful: {page.url}")


# =========================================================
# HUMAN-LIKE SEARCH PAGE OPEN
# =========================================================
def open_real_search_page(page) -> None:
    print("🧭 Opening Search Members page...")

    # Start from current dashboard or go there.
    if "/web/dashboard" not in page.url:
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)

    save_html(page, "debug_dashboard_before_search_click.html")

    # Try clicking like a human from dashboard/menu first
    clicked = click_first_visible(
        page,
        [
            'a:has-text("Search Members")',
            'a:has-text("Member Search")',
            'a:has-text("Find Members")',
            'a:has-text("Members")',
            'button:has-text("Search Members")',
            'button:has-text("Member Search")',
            'button:has-text("Members")',
            'a[href*="/web/dashboard/search"]',
            'a[href*="dashboard/search"]',
            'a[href*="search"]',
        ],
        timeout_ms=5000,
    )

    if clicked:
        page.wait_for_timeout(4000)
        print(f"✅ Search page opened by click: {page.url}")
    else:
        print("ℹ️ Dashboard click path not found, using direct search URL fallback")
        page.goto(SEARCH_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(4000)

    save_html(page, "debug_search_page_opened.html")

    # Final guard
    if "/web/dashboard/search" not in page.url:
        print(f"ℹ️ Current search-page URL: {page.url}")


# =========================================================
# SEARCH CITY
# =========================================================
def search_city(page, city: str) -> None:
    print(f"\n🔍 Searching city: {city}")

    search_ok = fill_first_visible(
        page,
        [
            'input[type="search"]',
            'input[placeholder*="Search"]',
            'input[placeholder*="search"]',
            'input[placeholder*="City"]',
            'input[name*="search"]',
            'input[type="text"]',
        ],
        city,
        press_enter=True,
    )

    if not search_ok:
        save_html(page, f"debug_no_search_input_{city}.html")
        raise Exception(f"Could not find search input for city: {city}")

    # optional button click too
    click_first_visible(
        page,
        [
            'button:has-text("Search")',
            'button:has-text("Apply")',
            'button:has-text("Filter")',
            'a:has-text("Search")',
        ],
        timeout_ms=2000,
    )

    page.wait_for_timeout(6000)
    save_html(page, f"debug_results_{city}.html")
    print("✅ Search completed")


# =========================================================
# TOTAL ROWS / MEMBERS READ
# =========================================================
def read_total_rows_text(page) -> str:
    candidates = [
        'text=/Total\\s*Rows/i',
        'text=/Total\\s*Results/i',
        'text=/Showing/i',
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                txt = norm(loc.first.inner_text())
                if txt:
                    return txt
        except Exception:
            pass
    return ""


def get_members(page, city: str) -> List[Dict]:
    # Based on your reference script structure:
    # row container class includes css-1rb62l
    # profile link pattern includes networkHome?userId
    results = page.evaluate(
        """
        (searchCity) => {
            const members = [];

            const rows = Array.from(document.querySelectorAll('div'))
                .filter(el => {
                    const c = (el.className || '').toString();
                    return c.includes('css-1rb62l');
                });

            for (const row of rows) {
                const link = row.querySelector('a[href*="networkHome?userId"]');
                if (!link) continue;

                const name = (link.innerText || '').replace(/\\s+/g, ' ').trim();
                let href = link.getAttribute('href') || '';
                if (href.startsWith('/')) {
                    href = 'https://www.bniconnectglobal.com' + href;
                }

                if (!name || !href) continue;

                const childTexts = Array.from(row.children)
                    .map(c => (c.innerText || '').replace(/\\s+/g, ' ').trim())
                    .filter(t => t && t !== name && t !== '+' && t !== 'Connect');

                const chapter = childTexts.find(x => /^BNI\\s/i.test(x)) || '';
                const cityVal = childTexts.find(x => x.toLowerCase() === searchCity.toLowerCase()) || searchCity;
                const industry = childTexts.find(x => x.includes('>')) || '';

                const used = new Set([name, chapter, cityVal, industry, '', 'Connect', '+']);
                const company = childTexts.find(x => !used.has(x)) || '';

                members.push({
                    name,
                    href,
                    chapter,
                    company,
                    city: cityVal,
                    industry
                });
            }

            const seen = new Set();
            return members.filter(m => {
                if (seen.has(m.href)) return false;
                seen.add(m.href);
                return true;
            });
        }
        """,
        city,
    )

    cleaned = []
    for r in results:
        name = norm(r.get("name", ""))
        href = norm(r.get("href", ""))
        if not name or not href:
            continue
        cleaned.append(
            {
                "name": name,
                "href": href,
                "chapter": norm(r.get("chapter", "")),
                "company": norm(r.get("company", "")),
                "city": norm(r.get("city", "")) or city,
                "industry": norm(r.get("industry", "")),
            }
        )
    return cleaned


# =========================================================
# PROFILE EXTRACTION
# =========================================================
def extract_profile(page) -> Dict:
    det = {
        "Phone": "",
        "Email": "",
        "Website": "",
        "Address": "",
        "Professional Classification": "",
        "Business Description": "",
    }

    try:
        page.wait_for_load_state("networkidle", timeout=12000)
    except Exception:
        pass
    page.wait_for_timeout(2500)

    save_html(page, "debug_current_profile.html")

    lines = [norm(x) for x in page.locator("body").inner_text().splitlines() if norm(x)]
    full = " ".join(lines)

    # Email
    try:
        mailto = page.query_selector_all('a[href^="mailto:"]')
        if mailto:
            det["Email"] = norm((mailto[0].get_attribute("href") or "").replace("mailto:", ""))
    except Exception:
        pass
    if not det["Email"]:
        det["Email"] = find_email(full)

    # Website
    try:
        link_count = page.locator("a[href]").count()
        for i in range(link_count):
            href = (page.locator("a[href]").nth(i).get_attribute("href") or "").strip()
            low = href.lower()
            if (
                href.startswith("http")
                and "bniconnect" not in low
                and not low.startswith("mailto:")
                and not low.startswith("tel:")
                and "#" not in href
            ):
                det["Website"] = href
                break
    except Exception:
        pass

    # Phone
    phones = []
    for line in lines:
        if re.search(r"\d{2}/\d{2}/\d{4}", line):
            continue
        cleaned = re.sub(r"[\s\-\(\)\+]", "", line)
        if is_phone(cleaned) and cleaned not in phones:
            phones.append(cleaned)
        if len(phones) == 2:
            break
    if phones:
        det["Phone"] = " / ".join(phones)

    # Address
    addr_candidates = []
    for i, line in enumerate(lines):
        if line == "City" and i > 0:
            for j in range(i - 1, max(i - 6, 0), -1):
                candidate = lines[j]
                if (
                    len(candidate) > 5
                    and "@" not in candidate
                    and not candidate.startswith("http")
                    and not is_phone(re.sub(r"[\s\-]", "", candidate))
                    and candidate not in {
                        "Personal Details",
                        "Professional Details",
                        "My Bio",
                        "Profile",
                        "MSP",
                        "Training History",
                    }
                    and not re.search(r"\d{2}/\d{2}/\d{4}", candidate)
                ):
                    addr_candidates.append(candidate)
                    break
    if addr_candidates:
        det["Address"] = addr_candidates[0]

    if not det["Address"]:
        for line in lines:
            if any(
                k in line.lower()
                for k in [
                    "road",
                    "nagar",
                    "tower",
                    "complex",
                    "floor",
                    "lane",
                    "building",
                    "colony",
                    "plot",
                    "apartment",
                    "ward",
                    "sector",
                    "phase",
                    "deo",
                ]
            ):
                det["Address"] = line
                break

    # Professional Classification
    prof_section = False
    for line in lines:
        if line == "Professional Details":
            prof_section = True
            continue
        if prof_section:
            if line in {"My Bio", "Training History", "‹", "›", "Profile"}:
                break
            if len(line) > 3 and not re.search(r"\d{2}/\d{2}/\d{4}", line):
                det["Professional Classification"] = line
                break

    # Business Description
    prof_section = False
    count = 0
    for line in lines:
        if line == "Professional Details":
            prof_section = True
            count = 0
            continue
        if prof_section:
            if line in {"My Bio", "Training History", "‹", "›", "Profile"}:
                break
            if len(line) > 10 and not re.search(r"\d{2}/\d{2}/\d{4}", line):
                count += 1
                if count == 2:
                    det["Business Description"] = line
                    break

    return det


# =========================================================
# ONE CITY FLOW
# =========================================================
def process_city(page, city: str, done_urls: Set[str], all_rows: List[Dict]) -> None:
    open_real_search_page(page)
    search_city(page, city)

    total_rows_text = read_total_rows_text(page)
    if total_rows_text:
        print(f"📊 Total rows text: {total_rows_text}")

    no_new = 0

    while True:
        members = get_members(page, city)
        new_this_round = 0
        print(f"👥 Members visible this round for {city}: {len(members)}")

        for m in members:
            url = m["href"]
            if url in done_urls:
                continue
            done_urls.add(url)

            print(f"\n➡️ [{len(all_rows)+1}] {m['name']} | {city}")

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=25000)
                page.wait_for_timeout(2500)
            except Exception as e:
                print(f"⚠️ Cannot open profile: {e}")
                open_real_search_page(page)
                search_city(page, city)
                continue

            prof = extract_profile(page)

            final = {
                "Search City": city,
                "Name": m["name"],
                "Chapter": m["chapter"],
                "Company": m["company"],
                "City": m["city"],
                "Industry and Classification": m["industry"],
                "Profile URL": url,
                "Phone": prof["Phone"],
                "Email": prof["Email"],
                "Website": prof["Website"],
                "Address": prof["Address"],
                "Professional Classification": prof["Professional Classification"],
                "Business Description": prof["Business Description"],
            }

            print(f"   Chapter        : {final['Chapter']}")
            print(f"   Company        : {final['Company']}")
            print(f"   Industry       : {final['Industry and Classification']}")
            print(f"   Phone          : {final['Phone']}")
            print(f"   Email          : {final['Email']}")
            print(f"   Website        : {final['Website']}")
            print(f"   Address        : {final['Address']}")
            print(f"   Classification : {final['Professional Classification']}")

            append_csv(final)
            post_to_google_sheet_apps_script(final)
            all_rows.append(final)
            new_this_round += 1

            # go back to results and continue
            try:
                page.go_back(wait_until="domcontentloaded", timeout=15000)
                page.wait_for_timeout(2500)
            except Exception:
                open_real_search_page(page)
                search_city(page, city)

            if "/web/dashboard/search" not in page.url:
                open_real_search_page(page)
                search_city(page, city)

        no_new = 0 if new_this_round else no_new + 1
        if no_new >= 3:
            print(f"\n✅ No new members in 3 rounds for {city} — done")
            break

        print(f"\n⬇️ Scrolling for more members in {city}...")
        try:
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(2000)
            page.evaluate(
                """
                () => {
                    document.querySelectorAll('div').forEach(el => {
                        const s = window.getComputedStyle(el);
                        if ((s.overflowY === 'auto' || s.overflowY === 'scroll')
                                && el.scrollHeight > el.clientHeight) {
                            el.scrollTop += 2000;
                        }
                    });
                }
                """
            )
            page.wait_for_timeout(2500)
        except Exception as e:
            print(f"⚠️ Scroll issue: {e}")
            break


# =========================================================
# MAIN
# =========================================================
def main():
    if not BNI_EMAIL or not BNI_PASSWORD:
        raise Exception("Set BNI_EMAIL and BNI_PASSWORD first")

    print("=" * 70)
    print("BNI Connect Scraper — Final Stable Hybrid")
    print(f"Cities    : {', '.join(BNI_CITIES)}")
    print(f"CSV       : {CSV_FILE}")
    print(f"Headless  : {HEADLESS}")
    print("=" * 70)

    init_csv()

    all_rows: List[Dict] = []
    done_urls: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
        context = browser.new_context()
        page = context.new_page()

        login(page)

        for city in BNI_CITIES:
            try:
                process_city(page, city, done_urls, all_rows)
            except Exception as e:
                print(f"❌ City failed: {city} | {e}")

        print("\n" + "=" * 70)
        print(f"🎉 DONE! Total members saved: {len(all_rows)}")
        print(f"📄 CSV: {CSV_FILE}")
        if GOOGLE_WEBAPP_URL and GOOGLE_WEBAPP_URL != "YOUR_URL":
            print("📤 Apps Script posting was enabled")
        print("=" * 70)

        input("\nPress ENTER to close browser...")
        context.close()
        browser.close()


if __name__ == "__main__":
    main()
