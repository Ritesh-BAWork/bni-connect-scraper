import os
import re
import csv
import time
from typing import List, Dict, Set
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

LOGIN_URL = "https://www.bniconnectglobal.com/login/"
SEARCH_URL = "https://www.bniconnectglobal.com/web/dashboard/search"
BASE_URL = "https://www.bniconnectglobal.com"

BNI_EMAIL = os.getenv("BNI_EMAIL")
BNI_PASSWORD = os.getenv("BNI_PASSWORD")

CITIES_RAW = os.getenv("BNI_CITIES", "Nagpur")
CITIES = [c.strip() for c in CITIES_RAW.split(",") if c.strip()]

GOOGLE_WEBAPP_URL = os.getenv("GOOGLE_WEBAPP_URL", "").strip()
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

CSV_FILE = "bni_multi_city_owners.csv"

HEADERS = [
    "Name",
    "Chapter",
    "Company",
    "City",
    "Industry and Classification",
    "Contact",
    "Mail",
    "Web Page Link",
    "Address",
    "Profile URL",
]


def sleep(ms: int):
    time.sleep(ms / 1000)


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def ensure_env():
    if not BNI_EMAIL or not BNI_PASSWORD:
        raise ValueError("BNI_EMAIL or BNI_PASSWORD missing in environment variables.")
    if not CITIES:
        raise ValueError("No cities found in BNI_CITIES.")


def looks_like_phone(text: str) -> bool:
    digits = re.sub(r"\D", "", text or "")
    return 8 <= len(digits) <= 15


def extract_email(text: str) -> str:
    m = re.search(r"[\w\.-]+@[\w\.-]+\.\w+", text or "")
    return m.group(0) if m else ""


def extract_website(text: str) -> str:
    m = re.search(r"(https?://[^\s]+|www\.[^\s]+)", text or "", flags=re.I)
    return m.group(0) if m else ""


def save_html(page, filename: str):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"Saved HTML: {filename}")
    except Exception as e:
        print(f"Could not save HTML {filename}: {e}")


def save_screenshot(page, filename: str):
    try:
        page.screenshot(path=filename, full_page=True)
        print(f"Saved screenshot: {filename}")
    except Exception as e:
        print(f"Could not save screenshot {filename}: {e}")


def init_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)


def append_csv(row: Dict[str, str]):
    with open(CSV_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            row.get("Name", ""),
            row.get("Chapter", ""),
            row.get("Company", ""),
            row.get("City", ""),
            row.get("Industry and Classification", ""),
            row.get("Contact", ""),
            row.get("Mail", ""),
            row.get("Web Page Link", ""),
            row.get("Address", ""),
            row.get("Profile URL", ""),
        ])


def post_rows_to_google(rows: List[Dict[str, str]]):
    if not GOOGLE_WEBAPP_URL or not rows:
        return
    try:
        r = requests.post(GOOGLE_WEBAPP_URL, json=rows, timeout=60)
        print("Google Sheet response:", r.text[:500])
    except Exception as e:
        print("Google Sheet post failed:", e)


def wait_for_any(page, selectors: List[str], timeout_ms: int = 30000) -> str:
    start = time.time()
    while (time.time() - start) * 1000 < timeout_ms:
        for selector in selectors:
            try:
                if page.locator(selector).count() > 0:
                    return selector
            except Exception:
                pass
        sleep(500)
    raise RuntimeError(f"None of these selectors appeared: {selectors}")


def login(page):
    print("Opening login page...")
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
    sleep(3000)

    print("Login page URL:", page.url)
    save_html(page, "debug_login_page.html")

    user_selector = wait_for_any(page, [
        'input[name="username"]',
        'input[name="Username"]',
        'input[type="email"]',
        'input[type="text"]',
    ])

    pass_selector = wait_for_any(page, [
        'input[name="password"]',
        'input[name="Password"]',
        'input[type="password"]',
    ])

    print("Using username selector:", user_selector)
    print("Using password selector:", pass_selector)

    page.locator(user_selector).first.fill(BNI_EMAIL)
    page.locator(pass_selector).first.fill(BNI_PASSWORD)

    submit_selector = wait_for_any(page, [
        'button[type="submit"]',
        'input[type="submit"]',
        'button',
    ], 15000)

    page.locator(submit_selector).first.click()
    page.wait_for_url("**/web/dashboard**", timeout=40000)
    sleep(3000)

    print("Post-login URL:", page.url)
    save_html(page, "debug_after_login.html")
    save_screenshot(page, "debug_after_login.png")
    print("✓ Login flow completed")


def open_member_search_page(page) -> bool:
    print("Opening member search page...")

    candidates = [
        SEARCH_URL,
        "https://www.bniconnectglobal.com/web/dashboard/",
    ]

    for url in candidates:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            sleep(2500)
            print("Tried URL:", page.url)

            for sel in [
                'input[placeholder*="search" i]',
                'input[placeholder*="city" i]',
                'input[type="search"]',
                'input[type="text"]',
            ]:
                try:
                    if page.locator(sel).count() > 0:
                        save_html(page, "debug_search_landing.html")
                        save_screenshot(page, "debug_search_landing.png")
                        return True
                except Exception:
                    pass
        except Exception as e:
            print("URL open failed:", url, e)

    # fallback clicks from dashboard
    click_candidates = [
        'button:has-text("Search Members")',
        'a:has-text("Search Members")',
        'text="Search Members"',
        '[aria-label*="search" i]',
        '[title*="search" i]',
        'button:has(svg)',
        'a:has(svg)',
    ]

    for sel in click_candidates:
        try:
            count = page.locator(sel).count()
            for i in range(min(count, 10)):
                try:
                    loc = page.locator(sel).nth(i)
                    loc.scroll_into_view_if_needed()
                    loc.click(timeout=3000)
                    sleep(2500)
                    for input_sel in [
                        'input[placeholder*="search" i]',
                        'input[placeholder*="city" i]',
                        'input[type="search"]',
                        'input[type="text"]',
                    ]:
                        if page.locator(input_sel).count() > 0:
                            save_html(page, "debug_search_landing.html")
                            save_screenshot(page, "debug_search_landing.png")
                            return True
                except Exception:
                    pass
        except Exception:
            pass

    save_html(page, "debug_failed_open_search.html")
    save_screenshot(page, "debug_failed_open_search.png")
    return False


def search_city(page, city_name: str) -> bool:
    ok = open_member_search_page(page)
    if not ok:
        print("Could not open member search page.")
        return False

    input_selector = None
    for sel in [
        'input[placeholder*="search" i]',
        'input[placeholder*="city" i]',
        'input[type="search"]',
        'input[type="text"]',
    ]:
        try:
            if page.locator(sel).count() > 0:
                input_selector = sel
                break
        except Exception:
            pass

    if not input_selector:
        print("Could not find city search input.")
        save_html(page, "debug_no_search_input.html")
        save_screenshot(page, "debug_no_search_input.png")
        return False

    print(f"Searching city: {city_name}")
    box = page.locator(input_selector).first
    box.click()
    box.fill(city_name)
    page.keyboard.press("Enter")
    sleep(5000)

    save_html(page, f"debug_after_search_{city_name}.html")
    save_screenshot(page, f"debug_after_search_{city_name}.png")
    print("✓ Search completed")
    return True


def collect_rows_from_page(page, city_name: str) -> List[Dict[str, str]]:
    """
    Collect visible result rows and actual member profile URLs from anchor tags.
    """
    js = """
    (cityName) => {
        function clean(t) {
            return (t || '').replace(/\\s+/g, ' ').trim();
        }

        function getRowContainer(a) {
            let node = a;
            for (let i = 0; i < 10 && node; i++, node = node.parentElement) {
                const txt = clean(node?.innerText || '');
                if (
                    txt &&
                    txt.toLowerCase().includes(cityName.toLowerCase()) &&
                    /BNI\\s+/i.test(txt) &&
                    txt.length > 20 &&
                    txt.length < 800 &&
                    !/Search Members/i.test(txt) &&
                    !/Search Results/i.test(txt)
                ) {
                    return node;
                }
            }
            return null;
        }

        const anchors = Array.from(document.querySelectorAll('a[href]'));
        const out = [];
        const seen = new Set();

        for (const a of anchors) {
            const href = a.getAttribute('href') || '';
            const name = clean(a.innerText || '');

            if (!name) continue;
            if (!href.toLowerCase().includes('/web/member')) continue;

            const rowNode = getRowContainer(a);
            if (!rowNode) continue;

            const lines = (rowNode.innerText || '')
                .split('\\n')
                .map(clean)
                .filter(Boolean)
                .filter(x => ![
                    'Name', 'Chapter', 'Company', 'City',
                    'Industry and Classification', 'Connect', '+'
                ].includes(x));

            const chapter = lines.find(x => /^BNI\\s+/i.test(x)) || '';
            const city = lines.find(x => x.toLowerCase() === cityName.toLowerCase()) || cityName;
            const industry = lines.find(x => x.includes('>')) || '';

            let company = '';
            for (const x of lines) {
                if (x === name || x === chapter || x === city || x === industry) continue;
                company = x;
                break;
            }

            if (!chapter) continue;

            const key = `${name}|${chapter}|${company}|${href}`;
            if (seen.has(key)) continue;
            seen.add(key);

            out.push({
                Name: name,
                Chapter: chapter,
                Company: company,
                City: city,
                "Industry and Classification": industry,
                href: href
            });
        }
        return out;
    }
    """
    raw = page.evaluate(js, city_name)

    rows = []
    for r in raw:
        href = r.get("href", "")
        profile_url = urljoin(BASE_URL, href)
        rows.append({
            "Name": norm(r.get("Name", "")),
            "Chapter": norm(r.get("Chapter", "")),
            "Company": norm(r.get("Company", "")),
            "City": norm(r.get("City", "")) or city_name,
            "Industry and Classification": norm(r.get("Industry and Classification", "")),
            "Profile URL": profile_url,
        })
    return rows


def load_all_city_rows(page, city_name: str) -> List[Dict[str, str]]:
    all_rows: List[Dict[str, str]] = []
    seen_keys: Set[str] = set()
    no_growth = 0

    for round_no in range(1, 25):
        current_rows = collect_rows_from_page(page, city_name)

        added = 0
        for row in current_rows:
            key = f"{row['Name']}|{row['Chapter']}|{row['Company']}|{row['Profile URL']}"
            if key not in seen_keys:
                seen_keys.add(key)
                all_rows.append(row)
                added += 1

        print(f"City: {city_name} | Round {round_no}: visible rows = {len(current_rows)} | total collected = {len(all_rows)}")

        if added == 0:
            no_growth += 1
        else:
            no_growth = 0

        if no_growth >= 3:
            break

        # scroll whole page
        page.evaluate("window.scrollBy(0, 2500)")
        sleep(1500)

        # scroll any scrollable divs
        page.evaluate(
            """() => {
                const els = Array.from(document.querySelectorAll('div'));
                for (const el of els) {
                    const style = window.getComputedStyle(el);
                    const canScroll =
                        (style.overflowY === 'auto' || style.overflowY === 'scroll') &&
                        el.scrollHeight > el.clientHeight;
                    if (canScroll) el.scrollTop = el.scrollTop + 1500;
                }
            }"""
        )
        sleep(1500)

    print(f"Total rows for {city_name}: {len(all_rows)}")
    return all_rows


def extract_profile(page, profile_url: str) -> Dict[str, str]:
    print("Opening profile URL:", profile_url)
    page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
    sleep(2500)

    body_text = page.locator("body").inner_text()
    lines = [norm(x) for x in body_text.splitlines() if norm(x)]

    mail = ""
    website = ""
    contact = ""
    address = ""

    try:
        mailto = page.locator('a[href^="mailto:"]')
        if mailto.count() > 0:
            href = mailto.first.get_attribute("href") or ""
            mail = href.replace("mailto:", "").strip()
    except Exception:
        pass

    if not mail:
        mail = extract_email(body_text)

    try:
        links = page.locator("a[href]")
        for i in range(links.count()):
            href = (links.nth(i).get_attribute("href") or "").strip()
            if (
                href
                and "bniconnectglobal.com" not in href.lower()
                and not href.lower().startswith("mailto:")
                and not href.startswith("#")
            ):
                website = href
                break
    except Exception:
        pass

    if not website:
        website = extract_website(body_text)

    phones = []
    for line in lines:
        if looks_like_phone(line) and line not in phones:
            phones.append(line)
    if phones:
        contact = " / ".join(phones[:2])

    collecting = False
    address_lines = []

    for line in lines:
        low = line.lower()

        if website and website in line:
            collecting = True
            continue

        if collecting:
            if low in {"city", "zip / postal code", "country"}:
                break
            if line in {"‹", "›", "<", ">", "Personal Details"}:
                continue
            address_lines.append(line)

    if address_lines:
        address = " ".join(address_lines)

    if not address:
        candidates = []
        for line in lines:
            low = line.lower()
            if any(k in low for k in [
                "road", "rd", "complex", "bank", "nagar", "floor",
                "lane", "building", "chaoni", "colony", "plot", "apartment"
            ]):
                candidates.append(line)
        if candidates:
            address = " ".join(candidates[:3])

    return {
        "Contact": contact,
        "Mail": mail,
        "Web Page Link": website,
        "Address": address,
    }


def main():
    ensure_env()
    init_csv()

    print("=" * 70)
    print("BNI Connect Multi-City Scraper — Starting")
    print("Cities    :", ", ".join(CITIES))
    print("Email     :", BNI_EMAIL)
    print("CSV       :", CSV_FILE)
    print("Headless  :", HEADLESS)
    print("=" * 70)

    all_rows: List[Dict[str, str]] = []
    batch_rows: List[Dict[str, str]] = []
    done_profiles: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        login(page)

        for city_name in CITIES:
            print("\n" + "=" * 70)
            print("Processing city:", city_name)
            print("=" * 70)

            if not search_city(page, city_name):
                print("Could not search city:", city_name)
                continue

            city_rows = load_all_city_rows(page, city_name)
            if not city_rows:
                print("No visible rows found for city:", city_name)
                continue

            for row in city_rows:
                profile_url = row.get("Profile URL", "")
                if not profile_url or profile_url in done_profiles:
                    continue

                try:
                    profile = extract_profile(page, profile_url)
                except PlaywrightTimeoutError:
                    print("Timeout while opening profile:", profile_url)
                    continue
                except Exception as e:
                    print("Profile extraction failed:", profile_url, e)
                    continue

                final_row = {
                    "Name": row.get("Name", ""),
                    "Chapter": row.get("Chapter", ""),
                    "Company": row.get("Company", ""),
                    "City": row.get("City", city_name),
                    "Industry and Classification": row.get("Industry and Classification", ""),
                    "Contact": profile.get("Contact", ""),
                    "Mail": profile.get("Mail", ""),
                    "Web Page Link": profile.get("Web Page Link", ""),
                    "Address": profile.get("Address", ""),
                    "Profile URL": profile_url,
                }

                print(final_row)

                all_rows.append(final_row)
                batch_rows.append(final_row)
                done_profiles.add(profile_url)
                append_csv(final_row)

                if len(batch_rows) >= 10:
                    post_rows_to_google(batch_rows)
                    batch_rows = []

            # go back to search page for next city
            page.goto("https://www.bniconnectglobal.com/web/dashboard", wait_until="domcontentloaded", timeout=30000)
            sleep(2500)

        if batch_rows:
            post_rows_to_google(batch_rows)

        print(f"\nCompleted. Total extracted rows: {len(all_rows)}")
        browser.close()


if __name__ == "__main__":
    main()
