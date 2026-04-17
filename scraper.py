import csv
import json
import os
import re
import time
from typing import Dict, List, Set, Optional

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# =========================================================
# CONFIG
# =========================================================
BNI_EMAIL = os.getenv("BNI_EMAIL", "").strip()
BNI_PASSWORD = os.getenv("BNI_PASSWORD", "").strip()
BNI_CITIES = [c.strip() for c in os.getenv("BNI_CITIES", "Nagpur").split(",") if c.strip()]

LOGIN_URL = "https://www.bniconnectglobal.com/login/"
SEARCH_URL = "https://www.bniconnectglobal.com/web/dashboard/search"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
SLOW_MO = int(os.getenv("SLOW_MO", "0"))
CSV_FILE = os.getenv("CSV_FILE", "bni_members.csv")
PROGRESS_FILE = os.getenv("PROGRESS_FILE", "progress_state.json")
GOOGLE_WEBAPP_URL = os.getenv("GOOGLE_WEBAPP_URL", "").strip()
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"

MAX_RUN_MINUTES = int(os.getenv("MAX_RUN_MINUTES", "330"))
SAFE_EXIT_BUFFER_SECONDS = int(os.getenv("SAFE_EXIT_BUFFER_SECONDS", "300"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))


# =========================================================
# HEADERS
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


def clean_phone(t: str) -> str:
    digits = re.sub(r"\D", "", t or "")
    return digits if 8 <= len(digits) <= 15 else ""


def find_email(t: str) -> str:
    m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", t or "")
    return m.group(0) if m else ""


def make_deadline() -> float:
    return time.time() + (MAX_RUN_MINUTES * 60)


def should_stop(deadline: float) -> bool:
    return time.time() >= (deadline - SAFE_EXIT_BUFFER_SECONDS)


def is_bad_website(url: str) -> bool:
    low = (url or "").lower()
    bad = [
        "bniconnectglobal.com",
        "facebook.com/share",
        "wa.me/share",
        "instagram.com/share",
        "linkedin.com/share",
    ]
    return any(x in low for x in bad)


def looks_like_address(text: str) -> bool:
    if not text:
        return False
    text = text.lower()
    keywords = [
        "road", "street", "lane", "nagar", "colony", "plot", "floor",
        "building", "apartment", "complex", "near", "opp", "city",
        "area", "block", "sector", "phase", "marg", "chowk", "society",
        "west", "east", "north", "south"
    ]
    return any(word in text for word in keywords)


def save_html(page, filename: str) -> None:
    if not DEBUG_HTML:
        return
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"💾 Saved HTML: {filename}")
    except Exception as e:
        print(f"⚠️ Could not save HTML {filename}: {e}")


# =========================================================
# CSV
# =========================================================
def init_csv() -> None:
    if os.path.exists(CSV_FILE):
        print(f"📄 CSV ready: {CSV_FILE}")
        return
    with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow(HEADERS)
    print(f"📄 CSV created: {CSV_FILE}")


def append_csv(row: Dict) -> None:
    with open(CSV_FILE, "a", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow([row.get(h, "") for h in HEADERS])


def load_done_urls_from_csv() -> Set[str]:
    done = set()
    if not os.path.exists(CSV_FILE):
        return done
    try:
        with open(CSV_FILE, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = norm(row.get("Profile URL", ""))
                if url:
                    done.add(url)
    except Exception as e:
        print(f"⚠️ CSV resume read error: {e}")
    return done


# =========================================================
# PROGRESS
# =========================================================
def default_progress() -> Dict:
    return {
        "completed_cities": [],
        "done_urls": [],
        "current_city": "",
        "city_queue": [],
        "city_index": 0,
        "last_saved_at": "",
    }


def load_progress() -> Dict:
    if not os.path.exists(PROGRESS_FILE):
        return default_progress()
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        base = default_progress()
        base.update(data)
        return base
    except Exception:
        return default_progress()


def save_progress(progress: Dict) -> None:
    progress["last_saved_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)


def mark_url_done(progress: Dict, url: str) -> None:
    if url and url not in progress["done_urls"]:
        progress["done_urls"].append(url)


def mark_city_completed(progress: Dict, city: str) -> None:
    if city not in progress["completed_cities"]:
        progress["completed_cities"].append(city)
    progress["current_city"] = ""
    progress["city_queue"] = []
    progress["city_index"] = 0
    save_progress(progress)


def get_remaining_cities(progress: Dict) -> List[str]:
    completed = set(progress.get("completed_cities", []))
    return [c for c in BNI_CITIES if c not in completed]


# =========================================================
# GOOGLE SHEET
# =========================================================
def flush_google_batch(rows: List[Dict]) -> None:
    if not rows:
        return

    if not GOOGLE_WEBAPP_URL or GOOGLE_WEBAPP_URL == "YOUR_URL":
        print("ℹ️ GOOGLE_WEBAPP_URL not set. Skipping Google Sheet upload.")
        return

    payload = {"rows": rows}

    for attempt in range(1, 4):
        try:
            r = requests.post(GOOGLE_WEBAPP_URL, json=payload, timeout=60)
            print(f"📤 Apps Script POST: {r.status_code} | batch={len(rows)}")
            if r.ok:
                return
            print(f"📤 Response: {r.text[:200]}")
        except Exception as e:
            print(f"⚠️ Google Sheet POST failed attempt {attempt}: {e}")
        time.sleep(2)

    print("❌ Failed to post rows to Google Sheet")


# =========================================================
# PLAYWRIGHT HELPERS
# =========================================================
def click_first_visible(page, selectors: List[str], timeout_ms: int = 3000) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel)
            cnt = loc.count()
            for i in range(cnt):
                el = loc.nth(i)
                try:
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
            loc = page.locator(sel)
            cnt = loc.count()
            for i in range(cnt):
                el = loc.nth(i)
                try:
                    if el.is_visible(timeout=1000):
                        el.click(timeout=2000)
                        try:
                            el.press("Control+A")
                            el.press("Backspace")
                        except Exception:
                            pass
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
    print("🔐 Login...")
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(2500)

    if not fill_first_visible(page, ['input[name="username"]', 'input[type="email"]'], BNI_EMAIL):
        raise Exception("Username field not found")

    if not fill_first_visible(page, ['input[name="password"]', 'input[type="password"]'], BNI_PASSWORD):
        raise Exception("Password field not found")

    if not click_first_visible(page, ['button[type="submit"]', 'input[type="submit"]', 'button:has-text("Sign In")', 'button:has-text("Login")'], 5000):
        raise Exception("Login button not found")

    try:
        page.wait_for_url("**/web/dashboard**", timeout=30000)
    except PlaywrightTimeoutError:
        save_html(page, "debug_login_failed.html")
        raise Exception(f"Login failed. Current URL: {page.url}")

    page.wait_for_timeout(3000)
    print("✅ Login done")


# =========================================================
# SEARCH / COLLECT ALL LINKS FOR ONE CITY
# =========================================================
def open_search(page) -> None:
    page.goto(SEARCH_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(4000)


def search_city(page, city: str) -> None:
    print(f"🌍 {city}")
    if not fill_first_visible(
        page,
        [
            'input[type="search"]',
            'input[placeholder*="Search"]',
            'input[placeholder*="City"]',
            'input[name*="search"]',
            'input[type="text"]',
        ],
        city,
        press_enter=True,
    ):
        save_html(page, f"debug_no_search_input_{city}.html")
        raise Exception(f"Search input not found for city: {city}")

    click_first_visible(
        page,
        [
            'button:has-text("Search")',
            'button:has-text("Apply")',
            'button:has-text("Filter")',
        ],
        2000,
    )

    page.wait_for_timeout(5000)


def get_visible_members(page, city: str) -> List[Dict]:
    js = """
    (searchCity) => {
        const members = [];
        const anchors = Array.from(
            document.querySelectorAll('a[href*="networkHome?userId"], a[href*="/web/member?uuId="], a[href*="/web/member?uuid="]')
        );

        for (const a of anchors) {
            const name = (a.innerText || '').replace(/\\s+/g, ' ').trim();
            let href = a.getAttribute('href') || '';
            if (!href) continue;
            if (href.startsWith('/')) href = 'https://www.bniconnectglobal.com' + href;

            const row = a.closest('div');
            const rowText = row ? (row.innerText || '').replace(/\\s+/g, ' ').trim() : '';

            members.push({ name, href, rowText });
        }

        const seen = new Set();
        return members.filter(m => {
            if (!m.name || !m.href) return false;
            if (seen.has(m.href)) return false;
            seen.add(m.href);
            return true;
        });
    }
    """
    raw = page.evaluate(js, city)

    out = []
    for r in raw:
        name = norm(r.get("name", ""))
        href = norm(r.get("href", ""))
        row_text = norm(r.get("rowText", ""))

        if not name or not href:
            continue

        chapter = ""
        industry = ""

        parts = [x.strip() for x in row_text.split("  ") if x.strip()]
        for p in parts:
            if p.startswith("BNI "):
                chapter = p
            if ">" in p and not industry:
                industry = p

        out.append({
            "name": name,
            "href": href,
            "chapter": chapter,
            "company": "",
            "city": city,
            "industry": industry,
        })

    return out


def deep_scroll_search_results(page) -> None:
    # main window scroll
    try:
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1500)
    except Exception:
        pass

    # mouse wheel
    try:
        page.mouse.wheel(0, 5000)
        page.wait_for_timeout(1500)
    except Exception:
        pass

    # scroll every scrollable div
    try:
        page.evaluate(
            """
            () => {
                const els = Array.from(document.querySelectorAll('div'));
                for (const el of els) {
                    const s = window.getComputedStyle(el);
                    if ((s.overflowY === 'auto' || s.overflowY === 'scroll') && el.scrollHeight > el.clientHeight) {
                        el.scrollTop = el.scrollHeight;
                    }
                }
            }
            """
        )
        page.wait_for_timeout(1500)
    except Exception:
        pass


def collect_all_links_for_city(page, city: str, deadline: float) -> List[Dict]:
    open_search(page)
    search_city(page, city)

    print(f"📥 Collecting all links for city: {city}")

    collected: List[Dict] = []
    seen_urls: Set[str] = set()
    previous_count = 0
    stable_rounds = 0

    # repeat until no new members appear after multiple scrolls
    for round_no in range(1, 401):
        if should_stop(deadline):
            break

        try:
            visible = get_visible_members(page, city)
        except Exception as e:
            print(f"⚠️ get_visible_members failed in {city}: {e}")
            break

        for m in visible:
            if m["href"] not in seen_urls:
                seen_urls.add(m["href"])
                collected.append(m)

        print(f"👥 {city} members collected so far: {len(collected)} | round={round_no}")

        if len(collected) == previous_count:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= 5:
            print(f"✅ Link collection finished for {city}: total={len(collected)}")
            break

        previous_count = len(collected)
        deep_scroll_search_results(page)

    return collected


# =========================================================
# PROFILE EXTRACTION
# =========================================================
def extract_profile(page, member_industry: str = "") -> Dict:
    result = {
        "Phone": "",
        "Email": "",
        "Website": "",
        "Address": "",
        "Professional Classification": "",
        "Business Description": "",
    }

    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass
    page.wait_for_timeout(2500)

    try:
        links = page.locator("a[href]")
        count = links.count()
        phones = []
        emails = []
        websites = []

        for i in range(count):
            href = (links.nth(i).get_attribute("href") or "").strip()
            txt = norm(links.nth(i).inner_text())

            if href.startswith("tel:"):
                p = clean_phone(href.replace("tel:", ""))
                if p and p not in phones:
                    phones.append(p)
            elif href.startswith("mailto:"):
                e = norm(href.replace("mailto:", ""))
                if e and e not in emails:
                    emails.append(e)
            elif href.startswith("http") and not is_bad_website(href):
                if href not in websites:
                    websites.append(href)

            if "@" in txt and txt not in emails:
                emails.append(txt)

            cp = clean_phone(txt)
            if cp and cp not in phones:
                phones.append(cp)

        if phones:
            result["Phone"] = " / ".join(phones[:2])
        if emails:
            result["Email"] = emails[0]
        if websites:
            result["Website"] = websites[0]
    except Exception:
        pass

    try:
        text = page.inner_text("body")
    except Exception:
        text = ""

    lines = [norm(x) for x in text.splitlines() if norm(x)]

    if not result["Email"]:
        em = find_email(text)
        if em:
            result["Email"] = em

    if not result["Phone"]:
        phones = re.findall(r"\+?\d[\d\s\-]{8,15}", text)
        clean = []
        for p in phones:
            cp = clean_phone(p)
            if cp and cp not in clean:
                clean.append(cp)
        if clean:
            result["Phone"] = " / ".join(clean[:2])

    if not result["Website"]:
        sites = re.findall(r"https?://[^\s]+", text)
        for s in sites:
            if not is_bad_website(s):
                result["Website"] = s
                break

    if not result["Address"]:
        for i, line in enumerate(lines):
            if line.lower() in {"city", "zip / postal code", "zip/postal code", "country"}:
                addr_block = []
                for j in range(max(0, i - 4), i):
                    cand = lines[j]
                    if looks_like_address(cand):
                        addr_block.append(cand)
                if addr_block:
                    result["Address"] = ", ".join(dict.fromkeys(addr_block))
                    break

    if not result["Address"]:
        for line in lines:
            if looks_like_address(line):
                result["Address"] = line
                break

    if ">" in member_industry:
        parts = [norm(x) for x in member_industry.split(">")]
        if parts:
            result["Professional Classification"] = parts[-1]

    if not result["Business Description"]:
        for i, line in enumerate(lines):
            if line.lower() in {"professional details", "my bio", "bio"}:
                block = []
                for j in range(i + 1, min(len(lines), i + 5)):
                    cand = lines[j]
                    if cand.lower() in {"training history", "profile"}:
                        break
                    if cand and cand != result["Professional Classification"]:
                        block.append(cand)
                if block:
                    result["Business Description"] = " ".join(block[:2])
                    break

    return result


def scrape_one_profile(profile_page, member: Dict, city: str) -> Optional[Dict]:
    url = member["href"]

    try:
        profile_page.goto(url, wait_until="domcontentloaded", timeout=25000)
        profile_page.wait_for_timeout(2000)
    except Exception as e:
        print(f"⚠️ Cannot open profile: {e}")
        return None

    prof = extract_profile(profile_page, member.get("industry", ""))
    if not prof["Phone"] and not prof["Email"]:
        profile_page.wait_for_timeout(2000)
        prof = extract_profile(profile_page, member.get("industry", ""))

    final = {
        "Search City": city,
        "Name": member["name"],
        "Chapter": member["chapter"],
        "Company": member["company"],
        "City": member["city"],
        "Industry and Classification": member["industry"],
        "Profile URL": url,
        "Phone": prof["Phone"],
        "Email": prof["Email"],
        "Website": prof["Website"],
        "Address": prof["Address"],
        "Professional Classification": prof["Professional Classification"],
        "Business Description": prof["Business Description"],
    }

    print(f"✅ {final['Name']} | {city}")
    print(f"   Phone: {final['Phone']}")
    print(f"   Email: {final['Email']}")
    print(f"   Website: {final['Website']}")
    print(f"   Address: {final['Address']}")
    print(f"   Classification: {final['Professional Classification']}")

    return final


# =========================================================
# PROCESS ONE CITY FULLY
# =========================================================
def process_city(search_page, profile_page, city: str, done_urls: Set[str], progress: Dict, deadline: float) -> bool:
    progress["current_city"] = city
    save_progress(progress)

    # collect full queue for this city first
    if not progress.get("city_queue"):
        queue = collect_all_links_for_city(search_page, city, deadline)
        if not queue:
            print(f"⚠️ No members found for city: {city}")
            return False
        progress["city_queue"] = queue
        progress["city_index"] = 0
        save_progress(progress)
    else:
        queue = progress["city_queue"]
        print(f"♻️ Resuming queue for {city}: total={len(queue)} from={progress.get('city_index', 0)}")

    print(f"📌 Starting scrape for full city: {city} | total profiles in queue={len(queue)}")

    batch_rows: List[Dict] = []
    start_index = int(progress.get("city_index", 0))
    scraped_count = 0

    for idx in range(start_index, len(queue)):
        if should_stop(deadline):
            print(f"⏳ Safe stop before timeout in {city} at index {idx}")
            flush_google_batch(batch_rows)
            save_progress(progress)
            return False

        member = queue[idx]
        url = member["href"]

        progress["current_city"] = city
        progress["city_index"] = idx
        save_progress(progress)

        if url in done_urls:
            continue

        final = scrape_one_profile(profile_page, member, city)
        if not final:
            continue

        append_csv(final)
        batch_rows.append(final)

        done_urls.add(url)
        mark_url_done(progress, url)
        scraped_count += 1

        if len(batch_rows) >= BATCH_SIZE:
            flush_google_batch(batch_rows)
            batch_rows = []

    flush_google_batch(batch_rows)

    if scraped_count == 0 and start_index == 0:
        print(f"⚠️ City {city} had zero scraped profiles. Not marking complete.")
        return False

    mark_city_completed(progress, city)
    print(f"✅ Full city completed: {city} | scraped={scraped_count}")
    return True


# =========================================================
# MAIN
# =========================================================
def main():
    if not BNI_EMAIL or not BNI_PASSWORD:
        raise Exception("Missing BNI_EMAIL or BNI_PASSWORD")

    init_csv()
    progress = load_progress()

    done_urls = load_done_urls_from_csv() | set(progress.get("done_urls", []))
    deadline = make_deadline()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
        context = browser.new_context()
        search_page = context.new_page()
        profile_page = context.new_page()

        login(search_page)

        # unfinished city first
        current_city = progress.get("current_city", "")
        if current_city and current_city not in progress.get("completed_cities", []):
            ok = process_city(search_page, profile_page, current_city, done_urls, progress, deadline)
            if not ok:
                browser.close()
                return

        # next cities one by one
        remaining_cities = get_remaining_cities(progress)

        for city in remaining_cities:
            progress["current_city"] = city
            progress["city_queue"] = []
            progress["city_index"] = 0
            save_progress(progress)

            ok = process_city(search_page, profile_page, city, done_urls, progress, deadline)
            if not ok:
                break

        try:
            profile_page.close()
        except Exception:
            pass
        try:
            search_page.close()
        except Exception:
            pass
        context.close()
        browser.close()


if __name__ == "__main__":
    main()
