import csv
import os
import re
import time
from typing import Dict, List

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# ================= CONFIG =================
BNI_EMAIL = os.getenv("BNI_EMAIL", "").strip()
BNI_PASSWORD = os.getenv("BNI_PASSWORD", "").strip()

CITY_TO_SCRAPE = "Mumbai"

LOGIN_URL = "https://www.bniconnectglobal.com/login/"
SEARCH_URL = "https://www.bniconnectglobal.com/web/dashboard/search"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
SLOW_MO = int(os.getenv("SLOW_MO", "0"))
CSV_FILE = os.getenv("CSV_FILE", "mumbai_bni_members.csv")
GOOGLE_WEBAPP_URL = os.getenv("GOOGLE_WEBAPP_URL", "").strip()
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))


# ================= HEADERS =================
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


# ================= HELPERS =================
def norm(t: str) -> str:
    return re.sub(r"\s+", " ", t or "").strip()


def clean_phone(t: str) -> str:
    digits = re.sub(r"\D", "", t or "")
    return digits if 8 <= len(digits) <= 15 else ""


def find_email(t: str) -> str:
    m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", t or "")
    return m.group(0) if m else ""


def save_html(page, filename: str) -> None:
    if not DEBUG_HTML:
        return
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"Saved HTML: {filename}")
    except Exception as e:
        print(f"Could not save HTML {filename}: {e}")


def is_bad_website(url: str) -> bool:
    low = (url or "").lower()
    bad = [
        "bniconnectglobal.com",
        "bnitos.com",
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
        "west", "east", "north", "south", "office", "shop", "flat"
    ]
    return any(word in text for word in keywords)


# ================= CSV =================
def init_csv() -> None:
    with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow(HEADERS)
    print(f"CSV ready: {CSV_FILE}")


def append_csv(row: Dict) -> None:
    with open(CSV_FILE, "a", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerow([row.get(h, "") for h in HEADERS])


# ================= GOOGLE SHEET =================
def flush_google_batch(rows: List[Dict]) -> None:
    if not rows:
        return

    if not GOOGLE_WEBAPP_URL or GOOGLE_WEBAPP_URL == "YOUR_URL":
        print("GOOGLE_WEBAPP_URL not set. Skipping Google Sheet upload.")
        return

    payload = {"rows": rows}

    for attempt in range(1, 4):
        try:
            r = requests.post(GOOGLE_WEBAPP_URL, json=payload, timeout=60)
            print(f"Apps Script POST: {r.status_code} | batch={len(rows)}")
            if r.ok:
                return
            print(f"Response: {r.text[:300]}")
        except Exception as e:
            print(f"POST failed attempt {attempt}: {e}")
        time.sleep(2)

    print("Failed to post rows to Google Sheet")


# ================= PLAYWRIGHT HELPERS =================
def click_first_visible(page, selectors, timeout_ms=3000):
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


def fill_first_visible(page, selectors, value, press_enter=False):
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


# ================= LOGIN =================
def login(page) -> None:
    print("Login...")
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(3000)

    if not fill_first_visible(page, ['input[name="username"]', 'input[type="email"]'], BNI_EMAIL):
        raise Exception("Username field not found")

    if not fill_first_visible(page, ['input[name="password"]', 'input[type="password"]'], BNI_PASSWORD):
        raise Exception("Password field not found")

    if not click_first_visible(
        page,
        ['button[type="submit"]', 'input[type="submit"]', 'button:has-text("Sign In")', 'button:has-text("Login")'],
        5000
    ):
        raise Exception("Login button not found")

    try:
        page.wait_for_url("**/web/dashboard**", timeout=30000)
    except PlaywrightTimeoutError:
        save_html(page, "debug_login_failed.html")
        raise Exception(f"Login failed. Current URL: {page.url}")

    page.wait_for_timeout(3000)
    print("Login done")


# ================= SEARCH / COLLECT LINKS =================
def open_search(page) -> None:
    page.goto(SEARCH_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(5000)


def search_city(page, city: str) -> None:
    print(f"Searching city: {city}")

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
    () => {
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
    raw = page.evaluate(js)

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
    try:
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1500)
    except Exception:
        pass

    try:
        page.mouse.wheel(0, 5000)
        page.wait_for_timeout(1500)
    except Exception:
        pass

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


def collect_all_links_for_city(page, city: str) -> List[Dict]:
    open_search(page)
    search_city(page, city)

    print(f"Collecting all links for city: {city}")

    collected: List[Dict] = []
    seen_urls = set()
    previous_count = 0
    stable_rounds = 0

    for round_no in range(1, 401):
        visible = get_visible_members(page, city)

        for m in visible:
            if m["href"] not in seen_urls:
                seen_urls.add(m["href"])
                collected.append(m)

        print(f"{city} members collected so far: {len(collected)} | round={round_no}")

        if len(collected) == previous_count:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= 5:
            print(f"Link collection finished for {city}: total={len(collected)}")
            break

        previous_count = len(collected)
        deep_scroll_search_results(page)

    return collected


# ================= PROFILE CARD EXTRACTION =================
def get_card_content(page, heading_text: str) -> Dict:
    js = """
    (headingText) => {
        const clean = (s) => (s || '').replace(/\\s+/g, ' ').trim();
        const headingNeedle = clean(headingText).toLowerCase();

        const elements = Array.from(document.querySelectorAll('*'));
        for (const el of elements) {
            const text = clean(el.innerText);
            if (!text) continue;
            if (text.toLowerCase() !== headingNeedle) continue;

            let card = el;
            for (let i = 0; i < 7; i++) {
                if (!card || !card.parentElement) break;
                card = card.parentElement;
                const cardText = clean(card.innerText);
                if (cardText && cardText.toLowerCase().includes(headingNeedle) && cardText.length > headingText.length + 20) {
                    return {
                        text: card.innerText || '',
                        links: Array.from(card.querySelectorAll('a[href]')).map(a => ({
                            href: a.getAttribute('href') || '',
                            text: clean(a.innerText)
                        }))
                    };
                }
            }
        }
        return { text: '', links: [] };
    }
    """
    try:
        return page.evaluate(js, heading_text)
    except Exception:
        return {"text": "", "links": []}


def extract_anchor_data(card_links: List[Dict]) -> Dict:
    data = {"phones": [], "emails": [], "websites": []}

    for item in card_links:
        href = norm(item.get("href", ""))
        txt = norm(item.get("text", ""))

        if href.startswith("tel:"):
            p = clean_phone(href.replace("tel:", ""))
            if p and p not in data["phones"]:
                data["phones"].append(p)
            continue

        if href.startswith("mailto:"):
            e = norm(href.replace("mailto:", ""))
            if e and e not in data["emails"]:
                data["emails"].append(e)
            continue

        if href.startswith("http") and not is_bad_website(href):
            if href not in data["websites"]:
                data["websites"].append(href)

        if "@" in txt and txt not in data["emails"]:
            data["emails"].append(txt)

        cp = clean_phone(txt)
        if cp and cp not in data["phones"]:
            data["phones"].append(cp)

    return data


def extract_industry_from_profile(page) -> str:
    try:
        text = page.inner_text("body")
        lines = [norm(l) for l in text.splitlines() if norm(l)]

        for i, line in enumerate(lines):
            if line.lower() in ["professional details", "business category", "classification"]:
                for j in range(i + 1, min(i + 4, len(lines))):
                    if ">" in lines[j]:
                        return lines[j]

        for line in lines:
            if ">" in line:
                return line
    except Exception:
        pass

    return ""


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

    personal = get_card_content(page, "Personal Details")
    professional = get_card_content(page, "Professional Details")
    bio = get_card_content(page, "My Bio")
    if not bio.get("text"):
        bio = get_card_content(page, "Bio")

    personal_lines = [norm(x) for x in personal.get("text", "").splitlines() if norm(x)]
    professional_lines = [norm(x) for x in professional.get("text", "").splitlines() if norm(x)]
    bio_lines = [norm(x) for x in bio.get("text", "").splitlines() if norm(x)]

    anchor_data = extract_anchor_data(personal.get("links", []))

    if anchor_data["phones"]:
        result["Phone"] = " / ".join(anchor_data["phones"][:2])
    if anchor_data["emails"]:
        result["Email"] = anchor_data["emails"][0]
    if anchor_data["websites"]:
        result["Website"] = anchor_data["websites"][0]

    full_text = " ".join(personal_lines + professional_lines + bio_lines)

    if not result["Email"]:
        em = find_email(full_text)
        if em:
            result["Email"] = em

    if not result["Phone"]:
        phones = []
        for line in personal_lines:
            cp = clean_phone(line)
            if cp and cp not in phones:
                phones.append(cp)
        if phones:
            result["Phone"] = " / ".join(phones[:2])

    if not result["Website"]:
        for line in personal_lines:
            if line.startswith("http") and not is_bad_website(line):
                result["Website"] = line
                break

    if not result["Address"]:
        for i, line in enumerate(personal_lines):
            if line.lower() in {"city", "zip / postal code", "zip/postal code", "country"}:
                addr_block = []
                for j in range(max(0, i - 4), i):
                    cand = personal_lines[j]
                    if looks_like_address(cand):
                        addr_block.append(cand)
                if addr_block:
                    result["Address"] = ", ".join(dict.fromkeys(addr_block))
                    break

    if not result["Address"]:
        for line in personal_lines:
            if looks_like_address(line):
                result["Address"] = line
                break

    if ">" in member_industry:
        parts = [norm(x) for x in member_industry.split(">")]
        if parts:
            result["Professional Classification"] = parts[-1]

    if not result["Professional Classification"]:
        clean_prof = [x for x in professional_lines if x.lower() not in {"professional details"}]
        if clean_prof:
            first = clean_prof[0]
            if first and len(first) < 120:
                result["Professional Classification"] = first

    if not result["Business Description"]:
        clean_prof = [x for x in professional_lines if x.lower() not in {"professional details"}]
        if len(clean_prof) >= 2:
            result["Business Description"] = clean_prof[1]

    if not result["Business Description"] and bio_lines:
        clean_bio = [x for x in bio_lines if x.lower() not in {"my bio", "bio"}]
        if clean_bio:
            result["Business Description"] = " ".join(clean_bio[:3])

    return result


def scrape_one_profile(profile_page, member: Dict, city: str) -> Dict | None:
    url = member["href"]

    try:
        profile_page.goto(url, wait_until="domcontentloaded", timeout=25000)
        profile_page.wait_for_timeout(2000)
    except Exception as e:
        print(f"Cannot open profile: {e}")
        return None

    prof = extract_profile(profile_page, member.get("industry", ""))
    if not prof["Phone"] and not prof["Email"] and not prof["Address"]:
        profile_page.wait_for_timeout(2000)
        prof = extract_profile(profile_page, member.get("industry", ""))

    industry_final = member.get("industry", "")
    if not industry_final or ">" not in industry_final:
        profile_industry = extract_industry_from_profile(profile_page)
        if profile_industry:
            industry_final = profile_industry

    final = {
        "Search City": city,
        "Name": member["name"],
        "Chapter": member["chapter"],
        "Company": member["company"],
        "City": member["city"],
        "Industry and Classification": industry_final,
        "Profile URL": url,
        "Phone": prof["Phone"],
        "Email": prof["Email"],
        "Website": prof["Website"],
        "Address": prof["Address"],
        "Professional Classification": prof["Professional Classification"],
        "Business Description": prof["Business Description"],
    }

    print(f"Done: {final['Name']} | {city}")
    print(f"Phone: {final['Phone']}")
    print(f"Email: {final['Email']}")
    print(f"Website: {final['Website']}")
    print(f"Address: {final['Address']}")
    print(f"Classification: {final['Professional Classification']}")
    print(f"Industry: {final['Industry and Classification']}")

    return final


# ================= MAIN =================
def main():
    if not BNI_EMAIL or not BNI_PASSWORD:
        raise Exception("Missing BNI_EMAIL or BNI_PASSWORD")

    init_csv()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
        context = browser.new_context()
        search_page = context.new_page()
        profile_page = context.new_page()

        login(search_page)

        members = collect_all_links_for_city(search_page, CITY_TO_SCRAPE)
        print(f"Starting full scrape for {CITY_TO_SCRAPE}. Total profiles found: {len(members)}")

        batch_rows: List[Dict] = []

        for idx, member in enumerate(members, start=1):
            print(f"Profile {idx}/{len(members)}")
            final = scrape_one_profile(profile_page, member, CITY_TO_SCRAPE)
            if not final:
                continue

            append_csv(final)
            batch_rows.append(final)

            if len(batch_rows) >= BATCH_SIZE:
                flush_google_batch(batch_rows)
                batch_rows = []

        flush_google_batch(batch_rows)

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
