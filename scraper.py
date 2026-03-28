import os
import csv
import json
import asyncio
from urllib.parse import urljoin

from playwright.async_api import async_playwright

try:
    import requests
except ImportError:
    requests = None


# =========================
# CONFIG
# =========================
LOGIN_URL = "https://www.bniconnectglobal.com/web/"
SEARCH_URL_CANDIDATES = [
    "https://www.bniconnectglobal.com/v2/dashboard",
    "https://www.bniconnectglobal.com/web/",
]

BASE_URL = "https://www.bniconnectglobal.com"
CSV_FILE = "bni_multi_city_owners.csv"

EMAIL = os.getenv("BNI_EMAIL", "").strip()
PASSWORD = os.getenv("BNI_PASSWORD", "").strip()
CITIES = [c.strip() for c in os.getenv("BNI_CITIES", "Nagpur").split(",") if c.strip()]
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
GOOGLE_WEBAPP_URL = os.getenv("GOOGLE_WEBAPP_URL", "").strip()


# =========================
# HELPERS
# =========================
def post_to_google_sheet(rows):
    if not GOOGLE_WEBAPP_URL or GOOGLE_WEBAPP_URL == "YOUR_URL":
        print("Google Web App URL not configured. Skipping sheet upload.")
        return

    if not requests:
        print("requests not installed. Skipping Google Sheet upload.")
        return

    try:
        payload = {"rows": rows}
        r = requests.post(GOOGLE_WEBAPP_URL, json=payload, timeout=60)
        print(f"Google Sheet POST status: {r.status_code}")
        print(f"Google Sheet response: {r.text[:500]}")
    except Exception as e:
        print(f"Google Sheet upload failed: {e}")


def save_csv(rows):
    file_exists = os.path.exists(CSV_FILE)
    fieldnames = [
        "City",
        "Name",
        "Chapter",
        "Company",
        "Industry",
        "Contact",
        "Email",
        "Website",
        "Address",
        "Profile URL",
    ]

    mode = "a" if file_exists else "w"
    with open(CSV_FILE, mode, newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


async def save_page_html(page, filename):
    try:
        html = await page.content()
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"Saved HTML: {filename}")
    except Exception as e:
        print(f"Could not save HTML {filename}: {e}")


async def debug_selector_counts(page):
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
    print("\n===== SELECTOR COUNTS START =====")
    for sel in selectors:
        try:
            count = await page.locator(sel).count()
            print(f"{sel} -> {count}")
        except Exception as e:
            print(f"{sel} -> ERROR: {e}")
    print("===== SELECTOR COUNTS END =====\n")


async def debug_page_preview(page):
    try:
        body_text = await page.locator("body").inner_text()
        print("===== PAGE PREVIEW START =====")
        print(body_text[:8000])
        print("===== PAGE PREVIEW END =====")
    except Exception as e:
        print(f"Could not print page preview: {e}")


async def click_first(page, selectors):
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click()
                return True
        except:
            pass
    return False


async def login(page):
    print("Opening login page...")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)

    print(f"Login page URL: {page.url}")
    await save_page_html(page, "debug_login_page.html")

    username_selectors = [
        'input[name="username"]',
        'input[name="email"]',
        'input[type="email"]',
        'input[placeholder*="Username"]',
        'input[placeholder*="Email"]',
    ]
    password_selectors = [
        'input[name="password"]',
        'input[type="password"]',
    ]

    username_selector = None
    for sel in username_selectors:
        try:
            if await page.locator(sel).count() > 0:
                username_selector = sel
                break
        except:
            pass

    password_selector = None
    for sel in password_selectors:
        try:
            if await page.locator(sel).count() > 0:
                password_selector = sel
                break
        except:
            pass

    if not username_selector or not password_selector:
        await debug_page_preview(page)
        raise Exception("Could not locate login inputs on BNI login page.")

    print(f"Using username selector: {username_selector}")
    print(f"Using password selector: {password_selector}")

    await page.fill(username_selector, EMAIL)
    await page.fill(password_selector, PASSWORD)

    login_button_selectors = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Login")',
        'button:has-text("Sign In")',
        'button:has-text("SIGN IN")',
        'button:has-text("Submit")',
    ]

    clicked = await click_first(page, login_button_selectors)
    if not clicked:
        raise Exception("Could not click login button.")

    await page.wait_for_timeout(6000)

    # Sometimes BNI stays on /web/, sometimes redirects into /v2/
    print(f"Post-login URL: {page.url}")
    await save_page_html(page, "debug_after_login.html")

    # Optional: handle popups/consent if present
    await click_first(page, [
        'button:has-text("Continue")',
        'button:has-text("OK")',
        'button:has-text("Allow")',
        'button:has-text("Close")',
        'button[aria-label="Close"]',
    ])

    print("✓ Login flow completed")


async def open_search_page(page):
    print("Opening search/dashboard page...")
    for url in SEARCH_URL_CANDIDATES:
        try:
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(4000)
            print(f"Opened candidate URL: {url}")
            print(f"Current URL: {page.url}")
            await save_page_html(page, "debug_search_landing.html")
            return
        except Exception as e:
            print(f"Failed candidate URL {url}: {e}")
            continue

    raise Exception("Could not open search/dashboard page.")


async def search_city(page, city):
    print(f"Searching city: {city}")

    # Try to locate search/filter inputs
    search_input_selectors = [
        'input[placeholder*="Search"]',
        'input[placeholder*="search"]',
        'input[placeholder*="City"]',
        'input[placeholder*="city"]',
        'input[type="search"]',
        'input[name*="search"]',
        'input[name*="city"]',
    ]

    search_box = None
    for sel in search_input_selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                # choose first visible input
                count = await loc.count()
                for i in range(count):
                    try:
                        if await loc.nth(i).is_visible():
                            search_box = loc.nth(i)
                            break
                    except:
                        pass
                if search_box:
                    break
        except:
            pass

    if not search_box:
        print("Could not find a dedicated city search input. Printing preview.")
        await debug_page_preview(page)
        await debug_selector_counts(page)
        await save_page_html(page, "debug_no_search_input.html")
        return

    await search_box.click()
    await page.keyboard.press("Control+A")
    await page.keyboard.press("Backspace")
    await search_box.fill(city)
    await page.wait_for_timeout(1500)

    # Try enter first
    try:
        await search_box.press("Enter")
    except:
        pass

    # Then try buttons
    await click_first(page, [
        'button:has-text("Search")',
        'button:has-text("Apply")',
        'button:has-text("Filter")',
        'button[type="submit"]',
    ])

    await page.wait_for_timeout(5000)
    print("✓ Search completed")
    print(f"Results page URL: {page.url}")

    await debug_selector_counts(page)
    await debug_page_preview(page)
    await save_page_html(page, "debug_results_page.html")


async def get_result_rows(page):
    candidate_selectors = [
        "tbody tr",
        "tr",
        "div[role='row']",
        ".search-result",
        ".results-card",
        ".list-group-item",
        ".card",
        "div[class*='member']",
        "div[class*='result']",
    ]

    best_selector = None
    best_count = 0

    for sel in candidate_selectors:
        try:
            count = await page.locator(sel).count()
            if count > best_count:
                best_count = count
                best_selector = sel
        except:
            pass

    if not best_selector or best_count == 0:
        return None, 0

    print(f"Best row selector: {best_selector} -> {best_count}")
    return page.locator(best_selector), best_count


async def inspect_row(row, idx):
    try:
        html = await row.evaluate("(el) => el.outerHTML")
        with open(f"debug_row_{idx+1}.html", "w", encoding="utf-8") as f:
            f.write(html)
        print(f"Saved row HTML: debug_row_{idx+1}.html")
    except Exception as e:
        print(f"Could not save row HTML {idx+1}: {e}")

    try:
        text = await row.inner_text()
        print(f"Row {idx+1} text preview:\n{text[:1000]}\n")
    except Exception as e:
        print(f"Could not read row text {idx+1}: {e}")

    try:
        links = await row.locator("a").evaluate_all(
            """els => els.map(a => ({
                text: (a.innerText || '').trim(),
                href: a.getAttribute('href')
            }))"""
        )
        print(f"Row {idx+1} links: {json.dumps(links, ensure_ascii=False)}")
    except Exception as e:
        print(f"Could not inspect row links {idx+1}: {e}")


async def extract_basic_fields_from_row(row):
    name = ""
    chapter = ""
    company = ""
    industry = ""
    city = ""

    try:
        text = await row.inner_text()
    except:
        text = ""

    lines = [x.strip() for x in text.split("\n") if x.strip()]

    if lines:
        name = lines[0]
    if len(lines) > 1:
        chapter = lines[1]
    if len(lines) > 2:
        company = lines[2]
    if len(lines) > 3:
        industry = lines[3]

    candidate_name_selectors = [
        "a",
        "strong",
        "h3",
        "h4",
        "h5",
        "b",
        '[class*="name"]',
        '[class*="title"]',
    ]
    for sel in candidate_name_selectors:
        try:
            loc = row.locator(sel)
            if await loc.count() > 0:
                val = await loc.first.inner_text()
                if val and len(val.strip()) > 1:
                    name = val.strip()
                    break
        except:
            pass

    return {
        "name": name,
        "chapter": chapter,
        "company": company,
        "industry": industry,
        "city": city,
    }


async def get_profile_link_from_row(row, current_url):
    link_selectors = [
        "a[href*='profile']",
        "a[href*='Profile']",
        "a[href*='member']",
        "a[href*='Member']",
        "a",
    ]

    for sel in link_selectors:
        try:
            loc = row.locator(sel)
            count = await loc.count()
            for i in range(count):
                href = await loc.nth(i).get_attribute("href")
                txt = ""
                try:
                    txt = (await loc.nth(i).inner_text()).strip()
                except:
                    pass

                if href and "javascript:" not in href.lower():
                    full = urljoin(current_url, href)
                    print(f"Candidate profile link found | text={txt} | href={href} | full={full}")
                    return full
        except:
            pass

    return None


async def open_profile(page, row, profile_url, name):
    if profile_url:
        try:
            print(f"Opening profile via direct URL: {profile_url}")
            await page.goto(profile_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(3500)
            print(f"✓ Profile opened via URL for: {name}")
            return True
        except Exception as e:
            print(f"Direct URL open failed for {name}: {e}")

    try:
        link = row.locator("a").first
        if await link.count() > 0:
            print(f"Trying click fallback for: {name}")
            await link.click()
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(3500)
            print(f"✓ Profile opened via click fallback for: {name}")
            return True
    except Exception as e:
        print(f"Click fallback failed for {name}: {e}")

    return False


async def extract_profile_value_by_label(page, labels):
    try:
        body_text = await page.locator("body").inner_text()
    except:
        return ""

    lines = [x.strip() for x in body_text.split("\n") if x.strip()]

    for i, line in enumerate(lines):
        for label in labels:
            if line.lower() == label.lower():
                if i + 1 < len(lines):
                    return lines[i + 1]
            if line.lower().startswith(label.lower() + ":"):
                return line.split(":", 1)[1].strip()

    return ""


async def extract_profile(page, city_hint, profile_url):
    await save_page_html(page, "debug_current_profile.html")

    name = ""
    chapter = ""
    company = ""
    industry = ""
    city = city_hint
    contact = ""
    email = ""
    website = ""
    address = ""

    for sel in ["h1", "h2", "h3", '[class*="name"]', "strong", "title"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                value = await loc.first.inner_text()
                if value and len(value.strip()) > 1:
                    name = value.strip()
                    break
        except:
            pass

    try:
        mailto = page.locator('a[href^="mailto:"]').first
        if await mailto.count() > 0:
            href = await mailto.get_attribute("href")
            if href:
                email = href.replace("mailto:", "").strip()
    except:
        pass

    try:
        tel = page.locator('a[href^="tel:"]').first
        if await tel.count() > 0:
            href = await tel.get_attribute("href")
            if href:
                contact = href.replace("tel:", "").strip()
    except:
        pass

    try:
        links = await page.locator("a").evaluate_all(
            """els => els.map(a => a.href).filter(Boolean)"""
        )
        for href in links:
            h = href.lower()
            if h.startswith("http") and "bniconnect" not in h and "mailto:" not in h and "tel:" not in h:
                website = href
                break
    except:
        pass

    if not contact:
        contact = await extract_profile_value_by_label(page, ["Phone", "Mobile", "Contact", "Telephone"])
    if not email:
        email = await extract_profile_value_by_label(page, ["Email", "E-mail", "Mail"])
    if not website:
        website = await extract_profile_value_by_label(page, ["Website", "Web", "URL"])
    if not address:
        address = await extract_profile_value_by_label(page, ["Address", "Location"])
    if not company:
        company = await extract_profile_value_by_label(page, ["Company", "Business", "Organization"])
    if not chapter:
        chapter = await extract_profile_value_by_label(page, ["Chapter"])
    if not industry:
        industry = await extract_profile_value_by_label(page, ["Industry", "Category", "Profession"])
    if not city:
        city = await extract_profile_value_by_label(page, ["City", "Location"])

    return {
        "City": city_hint or city,
        "Name": name,
        "Chapter": chapter,
        "Company": company,
        "Industry": industry,
        "Contact": contact,
        "Email": email,
        "Website": website,
        "Address": address,
        "Profile URL": profile_url or page.url,
    }


async def process_city(page, city):
    print("\n" + "=" * 70)
    print(f"Processing city: {city}")
    print("=" * 70)

    await open_search_page(page)
    await search_city(page, city)

    rows_locator, row_count = await get_result_rows(page)
    print(f"City: {city} | Round 1: visible rows = {row_count}")

    if not rows_locator or row_count == 0:
        print(f"No visible rows found for city: {city}")
        return []

    collected = []
    seen_profile_urls = set()

    max_rows = min(row_count, 20)

    for i in range(max_rows):
        try:
            if i > 0:
                await open_search_page(page)
                await search_city(page, city)
                rows_locator, row_count = await get_result_rows(page)
                if not rows_locator or i >= row_count:
                    print(f"Rows not available on rerender at index {i}")
                    continue

            row = rows_locator.nth(i)

            await row.scroll_into_view_if_needed()
            await page.wait_for_timeout(1000)

            await inspect_row(row, i)

            basic = await extract_basic_fields_from_row(row)
            name = basic["name"] or f"Row {i+1}"

            print(f"Opening profile: {name} | City: {city}")

            profile_url = await get_profile_link_from_row(row, page.url)

            if not profile_url:
                print(f"Profile link not found for: {name}")
                continue

            if profile_url in seen_profile_urls:
                print(f"Skipping duplicate profile URL: {profile_url}")
                continue

            seen_profile_urls.add(profile_url)

            opened = await open_profile(page, row, profile_url, name)
            if not opened:
                print(f"Profile did not open: {name}")
                continue

            data = await extract_profile(page, city, profile_url)

            if not data["Name"]:
                data["Name"] = basic["name"]
            if not data["Chapter"]:
                data["Chapter"] = basic["chapter"]
            if not data["Company"]:
                data["Company"] = basic["company"]
            if not data["Industry"]:
                data["Industry"] = basic["industry"]

            print(f"✓ Extracted: {data['Name']} | {data['Email']} | {data['Contact']}")
            collected.append(data)

            save_csv([data])
            post_to_google_sheet([data])

        except Exception as e:
            print(f"Error on row {i+1}: {e}")
            continue

    return collected


async def main():
    if not EMAIL or not PASSWORD:
        raise Exception("Missing BNI_EMAIL or BNI_PASSWORD in environment variables.")

    print("=" * 70)
    print("BNI Connect Multi-City Scraper — Starting")
    print(f"Cities    : {', '.join(CITIES)}")
    print(f"Email     : {EMAIL}")
    print(f"CSV       : {CSV_FILE}")
    print(f"Headless  : {HEADLESS}")
    print("=" * 70)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await login(page)

            all_rows = []
            for city in CITIES:
                rows = await process_city(page, city)
                all_rows.extend(rows)

            print("\n" + "=" * 70)
            print(f"Completed. Total extracted rows: {len(all_rows)}")
            print("=" * 70)

        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
