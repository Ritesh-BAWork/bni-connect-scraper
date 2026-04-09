import csv
import json
import os
import re
import time
from typing import Dict, List, Set

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

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
SLOW_MO = int(os.getenv("SLOW_MO", "0"))
CSV_FILE = os.getenv("CSV_FILE", "bni_members.csv")
GOOGLE_WEBAPP_URL = os.getenv("GOOGLE_WEBAPP_URL", "").strip()
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"
PROGRESS_FILE = os.getenv("PROGRESS_FILE", "progress_state.json")

# Safe stop before GitHub hard timeout
MAX_RUN_MINUTES = int(os.getenv("MAX_RUN_MINUTES", "330"))
SAFE_EXIT_BUFFER_SECONDS = int(os.getenv("SAFE_EXIT_BUFFER_SECONDS", "300"))

# Batch post to Google Sheet
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))


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
# BASIC HELPERS
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


def make_deadline() -> float:
    return time.time() + (MAX_RUN_MINUTES * 60)


def should_stop(deadline: float) -> bool:
    return time.time() >= (deadline - SAFE_EXIT_BUFFER_SECONDS)


# =========================================================
# CSV
# =========================================================
def init_csv() -> None:
    if os.path.exists(CSV_FILE):
        print(f"📄 CSV exists: {CSV_FILE}")
        return

    with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow(HEADERS)

    print(f"📄 CSV ready: {CSV_FILE}")


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
        print(f"⚠️ Could not read existing CSV for resume: {e}")

    return done


# =========================================================
# GOOGLE SHEET BATCH POST
# =========================================================
def flush_google_batch(batch_rows: List[Dict]) -> None:
    if not batch_rows:
        return

    if not GOOGLE_WEBAPP_URL or GOOGLE_WEBAPP_URL == "YOUR_URL":
        print("ℹ️ GOOGLE_WEBAPP_URL not set. Skipping Google Sheet upload.")
        return

    payload = {"rows": batch_rows}

    for attempt in range(1, 4):
        try:
            r = requests.post(GOOGLE_WEBAPP_URL, json=payload, timeout=120)
            print(f"📤 Apps Script POST: {r.status_code} | batch={len(batch_rows)}")
            print(f"📤 Response: {r.text[:200]}")
            if r.ok:
                return
        except Exception as e:
            print(f"⚠️ POST failed (attempt {attempt}): {e}")
        time.sleep(2)

    print("❌ Failed to push data batch to Google Sheet")


# =========================================================
# PROGRESS / RESUME
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
    except Exception as e:
        print(f"⚠️ Could not read progress file, starting fresh: {e}")
        return default_progress()


def save_progress(progress: Dict) -> None:
    try:
        progress["last_saved_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(progress, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ Could not save progress file: {e}")


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


def get_remaining_cities(progress: Dict, all_cities: List[str]) -> List[str]:
    completed = set(progress.get("completed_cities", []))
    return [c for c in all_cities if c not in completed]


# =========================================================
# PLAYWRIGHT HELPERS
# =========================================================
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
# SEARCH PAGE
# =========================================================
def open_real_search_page(page) -> None:
    print("🧭 Opening Search Members page...")

    if "/web/dashboard" not in page.url:
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)

    save_html(page, "debug_dashboard_before_search_click.html")

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
# RESULT READING
# =========================================================
def get_members(page, city: str) -> List[Dict]:
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
                if (href.startsWith('/')) href = 'https://www.bniconnectglobal.com' + href;
                if (!name || !href) continue;

                const texts = Array.from(row.querySelectorAll('*'))
                    .map(el => (el.innerText || '').replace(/\\s+/g, ' ').trim())
                    .filter(Boolean);

                const uniqueTexts = [...new Set(texts)];

                let chapter = uniqueTexts.find(x => /^BNI\\s/i.test(x)) || '';
                let cityVal = uniqueTexts.find(x => x.toLowerCase() === searchCity.toLowerCase()) || searchCity;
                let industry = uniqueTexts.find(x => x.includes('>')) || '';

                let company = '';
                for (const x of uniqueTexts) {
                    if (
                        x !== name &&
                        x !== chapter &&
                        x !== cityVal &&
                        x !== industry &&
                        x !== '+' &&
                        x.toLowerCase() !== 'connect' &&
                        !/^BNI\\s/i.test(x) &&
                        !x.includes('>')
                    ) {
                        company = x;
                        break;
                    }
                }

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

        cleaned.append({
            "name": name,
            "href": href,
            "chapter": norm(r.get("chapter", "")),
            "company": norm(r.get("company", "")),
            "city": norm(r.get("city", "")) or city,
            "industry": norm(r.get("industry", "")),
        })

    return cleaned


def collect_all_links_for_city(search_page, city: str, deadline: float) -> List[Dict]:
    open_real_search_page(search_page)
    search_city(search_page, city)

    print(f"📥 Collecting all member links for {city}...")

    collected: List[Dict] = []
    seen_urls: Set[str] = set()
    stable_rounds = 0
    previous_count = 0
    max_scroll_rounds = 500

    for _ in range(max_scroll_rounds):
        if should_stop(deadline):
            break

        members = get_members(search_page, city)

        for m in members:
            if m["href"] not in seen_urls:
                seen_urls.add(m["href"])
                collected.append(m)

        print(f"👥 Total unique visible members collected so far for {city}: {len(collected)}")

        if len(collected) == previous_count:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= 3:
            print(f"✅ Finished link collection for {city}: {len(collected)}")
            break

        previous_count = len(collected)

        try:
            search_page.evaluate(
                """
                () => {
                    window.scrollTo(0, document.body.scrollHeight);
                }
                """
            )
            search_page.wait_for_timeout(2000)

            search_page.mouse.wheel(0, 4000)
            search_page.wait_for_timeout(2000)

            search_page.evaluate(
                """
                () => {
                    const els = Array.from(document.querySelectorAll('div'));
                    for (const el of els) {
                        const s = window.getComputedStyle(el);
                        if ((s.overflowY === 'auto' || s.overflowY === 'scroll') &&
                            el.scrollHeight > el.clientHeight) {
                            el.scrollTop = el.scrollHeight;
                        }
                    }
                }
                """
            )
            search_page.wait_for_timeout(2500)
        except Exception as e:
            print(f"⚠️ Scroll issue in {city}: {e}")
            break

    return collected


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

    try:
        mailto = page.query_selector_all('a[href^="mailto:"]')
        if mailto:
            det["Email"] = norm((mailto[0].get_attribute("href") or "").replace("mailto:", ""))
    except Exception:
        pass
    if not det["Email"]:
        det["Email"] = find_email(full)

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
        bad_address_phrases = [
            "i look forward",
            "happy to write",
            "construction & building materials",
            "advertising & marketing",
            "real estate services",
            "travel agent",
            "consulting",
        ]

        for line in lines:
            low = line.lower()
            if any(p in low for p in bad_address_phrases):
                continue

            if any(
                k in low for k in [
                    "road", "nagar", "tower", "complex", "floor", "lane",
                    "building", "colony", "plot", "apartment", "ward",
                    "sector", "phase", "square", "ring road", "gandhibagh",
                    "lakadganj", "dhantoli", "manewada", "deo"
                ]
            ):
                det["Address"] = line
                break

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


def scrape_one_profile(profile_page, member: Dict, city: str) -> Dict | None:
    url = member["href"]

    try:
        profile_page.goto(url, wait_until="domcontentloaded", timeout=25000)
        profile_page.wait_for_timeout(2500)
    except Exception as e:
        print(f"⚠️ Cannot open profile: {e}")
        return None

    prof = extract_profile(profile_page)

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

    print(f"\n➡️ {final['Name']} | {city}")
    print(f"   Chapter        : {final['Chapter']}")
    print(f"   Company        : {final['Company']}")
    print(f"   Industry       : {final['Industry and Classification']}")
    print(f"   Phone          : {final['Phone']}")
    print(f"   Email          : {final['Email']}")
    print(f"   Website        : {final['Website']}")
    print(f"   Address        : {final['Address']}")
    print(f"   Classification : {final['Professional Classification']}")

    return final


# =========================================================
# PROCESS ONE CITY FULLY, THEN NEXT CITY
# =========================================================
def process_city(
    search_page,
    profile_page,
    city: str,
    done_urls: Set[str],
    all_rows: List[Dict],
    progress: Dict,
    google_batch: List[Dict],
    deadline: float,
) -> bool:
    progress["current_city"] = city
    save_progress(progress)

    # Build queue only if current city queue is empty or for another city
    if progress.get("current_city") != city or not progress.get("city_queue"):
        progress["current_city"] = city
        progress["city_queue"] = []
        progress["city_index"] = 0
        save_progress(progress)

    if not progress.get("city_queue"):
        queue = collect_all_links_for_city(search_page, city, deadline)
        progress["city_queue"] = queue
        progress["city_index"] = 0
        save_progress(progress)
    else:
        queue = progress["city_queue"]
        print(f"♻️ Resuming existing queue for {city}: total={len(queue)} start_index={progress.get('city_index', 0)}")

    start_index = int(progress.get("city_index", 0))

    for idx in range(start_index, len(queue)):
        if should_stop(deadline):
            print(f"⏳ Safe stop before timeout in city {city} at profile index {idx}")
            flush_google_batch(google_batch)
            google_batch.clear()
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
        google_batch.append(final)
        all_rows.append(final)

        done_urls.add(url)
        mark_url_done(progress, url)

        if len(google_batch) >= BATCH_SIZE:
            flush_google_batch(google_batch)
            google_batch.clear()

    flush_google_batch(google_batch)
    google_batch.clear()

    mark_city_completed(progress, city)
    print(f"✅ City completed fully: {city}")
    return True


# =========================================================
# MAIN
# =========================================================
def main():
    if not BNI_EMAIL or not BNI_PASSWORD:
        raise Exception("Set BNI_EMAIL and BNI_PASSWORD first")

    print("=" * 70)
    print("BNI Connect Scraper — Full City Queue Resume")
    print(f"Cities    : {', '.join(BNI_CITIES)}")
    print(f"CSV       : {CSV_FILE}")
    print(f"Headless  : {HEADLESS}")
    print(f"Progress  : {PROGRESS_FILE}")
    print(f"Max run   : {MAX_RUN_MINUTES} minutes")
    print(f"Batch size: {BATCH_SIZE}")
    print("=" * 70)

    init_csv()

    progress = load_progress()

    done_urls_from_csv = load_done_urls_from_csv()
    done_urls_from_progress = set(progress.get("done_urls", []))
    done_urls: Set[str] = set(done_urls_from_csv) | set(done_urls_from_progress)

    all_rows: List[Dict] = []
    google_batch: List[Dict] = []

    remaining_cities = get_remaining_cities(progress, BNI_CITIES)

    print(f"✅ Already completed cities: {progress.get('completed_cities', [])}")
    print(f"▶ Remaining cities: {remaining_cities}")
    print(f"🔁 Already saved profile URLs: {len(done_urls)}")

    if not remaining_cities and not progress.get("current_city"):
        print("🎉 All configured cities already completed.")
        return

    deadline = make_deadline()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
        context = browser.new_context()
        search_page = context.new_page()
        profile_page = context.new_page()

        login(search_page)

        # If there is an unfinished current city, do it first
        current_city = progress.get("current_city", "")
        if current_city and current_city not in progress.get("completed_cities", []):
            print(f"♻️ Resuming unfinished city first: {current_city}")
            try:
                completed = process_city(
                    search_page=search_page,
                    profile_page=profile_page,
                    city=current_city,
                    done_urls=done_urls,
                    all_rows=all_rows,
                    progress=progress,
                    google_batch=google_batch,
                    deadline=deadline,
                )
                if not completed:
                    print("⏸ Run stopped safely. Next run will continue from same city and same profile index.")
                    return
            except Exception as e:
                print(f"❌ Current city failed: {current_city} | {e}")
                save_progress(progress)
                flush_google_batch(google_batch)
                google_batch.clear()
                return

        # Recompute remaining after possible current city completion
        remaining_cities = get_remaining_cities(progress, BNI_CITIES)

        for city in remaining_cities:
            try:
                completed = process_city(
                    search_page=search_page,
                    profile_page=profile_page,
                    city=city,
                    done_urls=done_urls,
                    all_rows=all_rows,
                    progress=progress,
                    google_batch=google_batch,
                    deadline=deadline,
                )
                if not completed:
                    print(f"⏸ Run stopped safely. Next run will resume from city: {city}")
                    break
            except Exception as e:
                print(f"❌ City failed: {city} | {e}")
                save_progress(progress)
                flush_google_batch(google_batch)
                google_batch.clear()
                break

        flush_google_batch(google_batch)
        google_batch.clear()

        print("\n" + "=" * 70)
        print(f"🎉 DONE! New rows saved in this run: {len(all_rows)}")
        print(f"📄 CSV: {CSV_FILE}")
        if GOOGLE_WEBAPP_URL and GOOGLE_WEBAPP_URL != "YOUR_URL":
            print("📤 Google Sheet posting was enabled")
        print("=" * 70)

        print("\nClosing browser...")
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
