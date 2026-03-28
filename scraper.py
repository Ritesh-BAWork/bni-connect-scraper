import os
import re
import csv
import time
import requests
from typing import List, Dict, Set
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

LOGIN_URL = "https://www.bniconnectglobal.com/login/"

BNI_EMAIL = os.getenv("BNI_EMAIL")
BNI_PASSWORD = os.getenv("BNI_PASSWORD")

CITIES_RAW = os.getenv(
    "BNI_CITIES",
    "Nagpur,Bangalore,Pune"
)
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
    sleep(4000)

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

    page.locator(user_selector).first.click()
    page.locator(user_selector).first.fill(BNI_EMAIL)

    page.locator(pass_selector).first.click()
    page.locator(pass_selector).first.fill(BNI_PASSWORD)

    submit_selector = wait_for_any(page, [
        'button[type="submit"]',
        'input[type="submit"]',
        'button',
    ], 15000)

    page.locator(submit_selector).first.click()

    page.wait_for_url("**/web/dashboard", timeout=40000)
    sleep(4000)

    print("Post-login URL:", page.url)
    save_html(page, "debug_after_login.html")
    save_screenshot(page, "debug_after_login.png")
    print("✓ Login flow completed")


def open_member_search_page(page) -> bool:
    """
    Must navigate like user from dashboard, because direct URL redirection
    may land on /v2/dashboard instead of actual search UI.
    """
    print("Opening member search page from dashboard...")

    candidate_urls = [
        "https://www.bniconnectglobal.com/web/dashboard",
        "https://www.bniconnectglobal.com/web/dashboard/search",
    ]

    for url in candidate_urls:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            sleep(3000)
            print("Tried URL:", page.url)
        except Exception as e:
            print("Candidate URL failed:", url, e)

    save_html(page, "debug_search_landing.html")
    save_screenshot(page, "debug_search_landing.png")

    # First: maybe search box is already present
    search_input_selectors = [
        'input[type="search"]',
        'input[placeholder*="search" i]',
        'input[placeholder*="city" i]',
        'input[type="text"]'
    ]

    for sel in search_input_selectors:
        try:
            if page.locator(sel).count() > 0:
                print("Search input already visible using selector:", sel)
                return True
        except Exception:
            pass

    # Try clicking obvious search / search members controls
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
            if count == 0:
                continue

            for i in range(min(count, 10)):
                try:
                    loc = page.locator(sel).nth(i)
                    loc.scroll_into_view_if_needed(timeout=3000)
                    loc.click(timeout=3000)
                    sleep(3000)

                    for input_sel in search_input_selectors:
                        if page.locator(input_sel).count() > 0:
                            print(f"Opened search page by clicking {sel} [{i}]")
                            return True
                except Exception:
                    pass
        except Exception:
            pass

    # Try text-based click by page JS
    try:
        js_clicked = page.evaluate("""
        () => {
            function clean(t) {
                return (t || '').replace(/\\s+/g, ' ').trim();
            }
            const els = Array.from(document.querySelectorAll('a, button, div, span'));
            for (const el of els) {
                const txt = clean(el.innerText || '');
                if (txt === 'Search Members' || txt === 'Search Member') {
                    el.scrollIntoView({behavior: 'instant', block: 'center'});
                    el.click();
                    return true;
                }
            }
            return false;
        }
        """)
        if js_clicked:
            sleep(3000)
            for sel in search_input_selectors:
                if page.locator(sel).count() > 0:
                    print("Opened search page using text click")
                    return True
    except Exception:
        pass

    print("Could not open real member search page.")
    save_html(page, "debug_failed_open_search.html")
    save_screenshot(page, "debug_failed_open_search.png")
    return False


def search_city(page, city_name: str):
    ok = open_member_search_page(page)
    if not ok:
        print("Could not reach search page. Printing preview.")
        preview = page.locator("body").inner_text()[:3000]
        print("===== PAGE PREVIEW START =====")
        print(preview)
        print("===== PAGE PREVIEW END =====")
        return False

    input_selector = None
    for sel in [
        'input[type="search"]',
        'input[placeholder*="city" i]',
        'input[placeholder*="search" i]',
        'input[type="text"]'
    ]:
        try:
            if page.locator(sel).count() > 0:
                input_selector = sel
                break
        except Exception:
            pass

    if not input_selector:
        print("Could not find search input after opening search page.")
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


def debug_preview(page, city_name: str):
    preview = page.locator("body").inner_text()[:4000]
    print(f"\n===== PAGE PREVIEW START ({city_name}) =====")
    print(preview)
    print(f"===== PAGE PREVIEW END ({city_name}) =====\n")


def print_selector_counts(page):
    selectors = [
        "tr",
        "tbody tr",
        "div[role='row']",
        "a[href*='member']",
        "a[href*='Member']",
        "a[href*='profile']",
        "a[href*='Profile']",
        "div[class*='member']",
        "div[class*='result']",
        ".card",
        ".search-result",
        ".results-card",
        ".list-group-item",
    ]
    print("===== SELECTOR COUNTS START =====")
    for sel in selectors:
        try:
            print(f"{sel} -> {page.locator(sel).count()}")
        except Exception:
            print(f"{sel} -> error")
    print("===== SELECTOR COUNTS END =====")


def get_visible_rows(page, city_name: str) -> List[Dict[str, str]]:
    rows = page.evaluate(
        """(cityName) => {
            function clean(t) {
              return (t || "").replace(/\\s+/g, " ").trim();
            }

            const out = [];
            const seen = new Set();
            const els = Array.from(document.querySelectorAll("body *"));

            for (const el of els) {
              const txt = clean(el.innerText || "");
              if (!/^BNI\\s+/i.test(txt)) continue;
              if (txt.length > 120) continue;

              let node = el;
              let found = null;

              for (let i = 0; i < 12 && node; i++, node = node.parentElement) {
                const block = clean(node.innerText || "");
                if (
                  block &&
                  block.toLowerCase().includes(cityName.toLowerCase()) &&
                  /BNI\\s+/i.test(block) &&
                  block.length > 20 &&
                  block.length < 700 &&
                  !/Search Members/i.test(block) &&
                  !/Search Results/i.test(block)
                ) {
                  found = node;
                  break;
                }
              }

              if (!found) continue;

              const lines = (found.innerText || "")
                .split("\\n")
                .map(clean)
                .filter(Boolean)
                .filter(x => ![
                  "Name", "Chapter", "Company", "City",
                  "Industry and Classification", "Connect", "+"
                ].includes(x));

              if (lines.length < 4) continue;

              const chapter = lines.find(x => /^BNI\\s+/i.test(x)) || "";
              const chapterIdx = lines.indexOf(chapter);
              let name = chapterIdx > 0 ? lines[chapterIdx - 1] : lines[0];
              let city = lines.find(x => x.toLowerCase() === cityName.toLowerCase()) || cityName;
              let industry = lines.find(x => x.includes(">")) || "";

              let company = "";
              for (const x of lines) {
                if (x === name || x === chapter || x === city || x === industry) continue;
                company = x;
                break;
              }

              if (!name || !chapter) continue;

              const key = `${name}|${chapter}|${company}|${city}|${industry}`;
              if (seen.has(key)) continue;
              seen.add(key);

              out.push({
                Name: name,
                Chapter: chapter,
                Company: company,
                City: city,
                "Industry and Classification": industry
              });
            }

            return out;
        }""",
        city_name,
    )

    cleaned = []
    for row in rows:
        if row.get("Name") and row.get("Chapter"):
            cleaned.append({
                "Name": norm(row.get("Name", "")),
                "Chapter": norm(row.get("Chapter", "")),
                "Company": norm(row.get("Company", "")),
                "City": norm(row.get("City", "")) or city_name,
                "Industry and Classification": norm(row.get("Industry and Classification", "")),
            })
    return cleaned


def click_member_name(page, target_name: str) -> bool:
    js = """(targetName) => {
        function clean(t) {
          return (t || '').replace(/\\s+/g, ' ').trim();
        }
        const candidates = Array.from(document.querySelectorAll('a, span, div'));
        for (const el of candidates) {
          const txt = clean(el.innerText || '');
          if (txt === targetName) {
            el.scrollIntoView({behavior: 'instant', block: 'center'});
            el.click();
            return true;
          }
        }
        return false;
    }"""
    try:
        return page.evaluate(js, target_name)
    except Exception:
        return False


def extract_profile(page) -> Dict[str, str]:
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
    done: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        login(page)

        for city_name in CITIES:
            print("\n" + "=" * 70)
            print("Processing city:", city_name)
            print("=" * 70)

            search_ok = search_city(page, city_name)
            if not search_ok:
                print("Could not search city:", city_name)
                continue

            no_new_rounds = 0
            round_no = 0
            did_debug = False

            while True:
                round_no += 1
                visible_rows = get_visible_rows(page, city_name)
                new_count = 0

                print(f"City: {city_name} | Round {round_no}: visible rows = {len(visible_rows)}")

                if not visible_rows and not did_debug:
                    debug_preview(page, city_name)
                    print_selector_counts(page)
                    did_debug = True

                for row in visible_rows:
                    key = f"{city_name}|{row['Name']}|{row['Chapter']}|{row['Company']}"
                    if key in done:
                        continue

                    print("Opening profile:", row["Name"], "| City:", city_name)
                    clicked = click_member_name(page, row["Name"])
                    if not clicked:
                        print("Could not click:", row["Name"])
                        continue

                    try:
                        page.wait_for_url("**/web/member**", timeout=15000)
                    except PlaywrightTimeoutError:
                        print("Profile did not open:", row["Name"])
                        continue

                    profile = extract_profile(page)

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
                    }

                    print(final_row)

                    all_rows.append(final_row)
                    batch_rows.append(final_row)
                    done.add(key)
                    new_count += 1

                    append_csv(final_row)

                    if len(batch_rows) >= 10:
                        post_rows_to_google(batch_rows)
                        batch_rows = []

                    page.go_back(wait_until="domcontentloaded")
                    sleep(2500)

                    # reopen search page if back lands elsewhere
                    if "/search" not in page.url.lower():
                        search_ok = search_city(page, city_name)
                        if not search_ok:
                            break

                if new_count == 0:
                    no_new_rounds += 1
                else:
                    no_new_rounds = 0

                if no_new_rounds >= 3:
                    print(f"No new members found for {city_name}. Moving to next city.")
                    break

                page.evaluate("window.scrollBy(0, 2500)")
                sleep(1500)

                page.evaluate(
                    """() => {
                        const els = Array.from(document.querySelectorAll("div"));
                        for (const el of els) {
                            const style = window.getComputedStyle(el);
                            const canScroll =
                              (style.overflowY === "auto" || style.overflowY === "scroll") &&
                              el.scrollHeight > el.clientHeight;
                            if (canScroll) el.scrollTop = el.scrollTop + 1500;
                        }
                    }"""
                )
                sleep(1500)

        if batch_rows:
            post_rows_to_google(batch_rows)

        print(f"\nCompleted. Total extracted rows: {len(all_rows)}")
        browser.close()


if __name__ == "__main__":
    main()
