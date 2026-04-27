"""
Seller x City Dashboard — FDP / QAAS Query Automation Agent
============================================================

Daily automation that:
  1. Opens a persistent Chrome profile via Playwright (handles LDAP + 2FA once)
  2. Extracts auth cookies from the browser session
  3. Submits the SQL in `query.sql` to the QAAS REST API
  4. Polls for completion
  5. Downloads the CSV result
  6. Saves it to the repo root as `362c62a8adb9d17ecb5a6c9d33385822.csv`
     (the filename the Streamlit dashboard loads)
  7. Commits and pushes to GitHub

Runs unattended via Windows Task Scheduler (see `run_scraper.bat` + `setup_scheduler.bat`).
"""

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests as http_requests
import yaml
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = ROOT_DIR / "config.yaml"
QUERY_FILE = ROOT_DIR / "query.sql"

FDP_API_HEADERS = {
    "fdp-lens-api-key": "ce5234ac-a24b-4fe4-8679-2ebac0ec941c",
    "fdp-lens-app-name": "FlipQuery",
    "x-requested-with": "XMLHttpRequest",
    "cache-control": "no-cache",
}


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_credentials() -> tuple[str, str]:
    # Credentials are loaded from environment variables only — never hardcoded.
    username = os.environ.get("FDP_USERNAME")
    password = os.environ.get("FDP_PASSWORD")
    if not username or not password:
        log.error("Set FDP_USERNAME and FDP_PASSWORD env vars (see .env.example)")
        sys.exit(1)
    return username, password


def get_query() -> str:
    if not QUERY_FILE.exists():
        return ""
    text = QUERY_FILE.read_text(encoding="utf-8").strip()
    lines = [l for l in text.splitlines() if not l.strip().startswith("--")]
    text = "\n".join(lines).strip()

    # Rolling ~1 month window ending yesterday, in YYYYMMDD integer format.
    today = datetime.now()
    end_dt = today - timedelta(days=1)
    start_dt = end_dt - timedelta(days=29)  # inclusive 30-day window
    end_date = end_dt.strftime("%Y%m%d")
    start_date = start_dt.strftime("%Y%m%d")

    if "{end_date}" in text:
        text = text.replace("{end_date}", end_date)
    if "{start_date}" in text:
        text = text.replace("{start_date}", start_date)

    log.info(
        "Dynamic window: %s → %s (last 30 days, ending yesterday)",
        start_date, end_date,
    )
    log.info("Loaded query from %s (%d chars)", QUERY_FILE.name, len(text))
    return text


def _file_size(path: Path) -> str:
    size = path.stat().st_size
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


# ── Step 1: Extract cookies via Playwright ────────────────────────────────────

def _test_api_cookies(cookies: dict, config: dict) -> bool:
    """Quick check: do the extracted cookies authenticate against the API?"""
    api_cfg = config.get("api", {})
    base = api_cfg.get("base_url", "http://fdp.fkinternal.com/p/fdp/lens/lensapi/queryapi")
    test_url = f"{base}/queries"
    try:
        resp = http_requests.get(
            test_url,
            cookies=cookies,
            headers=FDP_API_HEADERS,
            timeout=15,
            allow_redirects=False,
        )
        log.info("API cookie test: status %d", resp.status_code)
        return resp.status_code != 401
    except Exception as e:
        log.warning("API cookie test failed: %s", e)
        return False


def extract_cookies(config: dict, username: str, password: str) -> dict:
    """
    Launch Playwright with persistent Chrome profile to get auth cookies.
    If cookies are stale, forces a fresh login with the browser kept open for 2FA.
    """
    browser_cfg = config.get("browser", {})
    profile_dir = ROOT_DIR / browser_cfg.get("profile_dir", ".chrome_profile")
    profile_dir.mkdir(parents=True, exist_ok=True)
    login_cfg = config.get("login", {})

    log.info("Extracting auth cookies from Chrome profile...")

    cookies: dict = {}

    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            channel="chrome",
            headless=browser_cfg.get("headless", False),
            viewport={"width": 1280, "height": 720},
            args=["--disable-blink-features=AutomationControlled"],
        )

        page = context.pages[0] if context.pages else context.new_page()
        timeout = browser_cfg.get("timeout_ms", 60_000)
        post_login_wait = login_cfg.get("post_login_timeout_ms", 180_000)

        try:
            page.goto(config["site"]["base_url"], timeout=timeout)
            time.sleep(3)
            current_url = page.url

            if "fdp.fkinternal.com" in current_url:
                log.info("Page loaded on FDP domain")
            elif "2ndFactor" in current_url:
                log.info("2FA required — complete it in the Chrome window...")
                _wait_for_fdp(page, post_login_wait)
            else:
                log.info("Login required at: %s", current_url)
                _do_login(page, login_cfg, username, password)
                time.sleep(3)
                if "2ndFactor" in page.url:
                    log.info("2FA required after login — complete it now...")
                    _wait_for_fdp(page, post_login_wait)
                elif "fdp.fkinternal.com" not in page.url:
                    _wait_for_fdp(page, post_login_wait)

            log.info("Visiting query page to refresh API cookies...")
            page.goto(config["site"]["query_url"], timeout=timeout)
            time.sleep(3)

            raw_cookies = context.cookies("http://fdp.fkinternal.com")
            for c in raw_cookies:
                cookies[c["name"]] = c["value"]
            log.info("Extracted %d cookies (names: %s)", len(cookies), list(cookies.keys()))

            if cookies and not _test_api_cookies(cookies, config):
                log.warning("Cookies are stale — forcing fresh login...")
                login_url = config["site"]["base_url"] + "/s/fdp/login"
                page.goto(login_url, timeout=timeout)
                time.sleep(2)

                current_url = page.url
                if "fdp.fkinternal.com/query" not in current_url:
                    log.info("Please complete login/2FA in the browser window...")
                    if "2ndFactor" not in current_url:
                        try:
                            _do_login(page, login_cfg, username, password)
                            time.sleep(3)
                        except Exception:
                            log.info("Auto-login failed, waiting for manual login...")
                    _wait_for_fdp(page, post_login_wait)

                page.goto(config["site"]["query_url"], timeout=timeout)
                time.sleep(3)

                cookies.clear()
                raw_cookies = context.cookies("http://fdp.fkinternal.com")
                for c in raw_cookies:
                    cookies[c["name"]] = c["value"]
                log.info("Re-extracted %d cookies after fresh login", len(cookies))

        finally:
            try:
                context.close()
            except Exception:
                pass

    if not cookies:
        log.error("No cookies extracted — auth may have failed")
        sys.exit(1)

    return cookies


def _do_login(page, login_cfg: dict, username: str, password: str) -> None:
    try:
        page.wait_for_selector(login_cfg["username_selector"], timeout=15_000)
    except Exception:
        log.error("Login form not found at: %s", page.url)
        sys.exit(1)

    page.fill(login_cfg["username_selector"], username)
    page.fill(login_cfg["password_selector"], password)

    try:
        idp_sel = login_cfg.get("idp_dropdown_selector", "select")
        if page.locator(idp_sel).count() > 0:
            page.select_option(idp_sel, label=login_cfg.get("idp_value", "Flipkart LDAP"))
    except Exception:
        pass

    page.click(login_cfg["submit_selector"])


def _wait_for_fdp(page, max_wait_ms: int) -> None:
    max_seconds = max_wait_ms // 1000
    for elapsed in range(0, max_seconds, 3):
        time.sleep(3)
        try:
            if "fdp.fkinternal.com" in page.url:
                log.info("Authenticated after ~%ds", elapsed + 3)
                return
        except Exception:
            continue
    log.error("Timed out waiting for FDP auth")
    sys.exit(1)


# ── Step 2: Submit query via API ──────────────────────────────────────────────

def submit_query(session: http_requests.Session, config: dict, sql: str) -> str:
    """POST query as multipart/form-data. Returns the handleId."""
    api_cfg = config.get("api", {})
    base = api_cfg.get("base_url", "http://fdp.fkinternal.com/p/fdp/lens/lensapi/queryapi")
    submit_url = f"{base}/queries"

    form_fields = {
        "sessionid": (None, "undefined"),
        "query": (None, sql),
        "operation": (None, "execute"),
        "appendEOF": (None, "true"),
        "sourceName": (None, api_cfg.get("source_name", "BIGQUERY")),
        "queue": (None, api_cfg.get("queue_name", "fulfillment_adhoc")),
        "rhNodeId": (None, api_cfg.get("team_name", "core-logistics-t")),
    }

    log.info("Submitting query to %s", submit_url)
    log.info(
        "Source: %s | Queue: %s | Team: %s",
        form_fields["sourceName"][1],
        form_fields["queue"][1],
        form_fields["rhNodeId"][1],
    )

    resp = session.post(submit_url, files=form_fields, timeout=120)

    log.info("Response status: %d", resp.status_code)
    log.info("Response (first 500 chars): %s", resp.text[:500])

    if resp.status_code not in (200, 201, 202):
        log.error("Query submission failed: %d — %s", resp.status_code, resp.text[:1000])
        sys.exit(1)

    try:
        data = resp.json()
        handle_id = (
            data.get("data", {}).get("handleId")
            or data.get("queryHandle", {}).get("handleId")
            or data.get("handleId")
        )
        if handle_id:
            log.info("Query submitted — handleId: %s", handle_id)
            return handle_id
    except Exception:
        pass

    handle_match = re.search(r"<handleId>([^<]+)</handleId>", resp.text)
    if handle_match:
        handle_id = handle_match.group(1)
        log.info("Query submitted — handleId: %s", handle_id)
        return handle_id

    log.error("Could not extract handleId from response: %s", resp.text[:500])
    sys.exit(1)


# ── Step 3: Poll for results ─────────────────────────────────────────────────

def poll_status(session: http_requests.Session, config: dict, handle_id: str) -> dict:
    """Poll GET /queries/{handleId} until completed or failed."""
    api_cfg = config.get("api", {})
    base = api_cfg.get("base_url", "http://fdp.fkinternal.com/p/fdp/lens/lensapi/queryapi")
    status_url = f"{base}/queries/{handle_id}"
    max_wait = api_cfg.get("max_wait_seconds", 600)
    poll_interval = api_cfg.get("poll_interval_seconds", 15)

    log.info("Polling status at %s (max %ds)", status_url, max_wait)

    elapsed = 0
    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval

        try:
            resp = session.get(status_url, timeout=30)
        except Exception as e:
            log.warning("Poll failed: %s", e)
            continue

        if resp.status_code != 200:
            log.warning("Status returned %d | Body: %s", resp.status_code, resp.text[:300])
            continue

        try:
            data = resp.json()
        except Exception:
            log.info("  [%ds] Non-JSON response: %s", elapsed, resp.text[:200])
            continue

        raw_status = data.get("status", {})
        if isinstance(raw_status, dict):
            status = str(raw_status.get("status", "")).lower()
            result_available = raw_status.get("isResultSetAvailable", False)
        else:
            status = str(raw_status).lower()
            result_available = data.get("isResultSetAvailable", False)

        log.info(
            "  [%ds/%ds] Status: %s | ResultAvailable: %s",
            elapsed, max_wait, status, result_available,
        )

        if status in ("completed", "succeeded", "success", "successful", "done", "finished"):
            log.info("Query completed after %ds!", elapsed)
            return data

        if status in ("failed", "error", "cancelled", "killed"):
            log.error("Query %s! Details: %s", status, json.dumps(data)[:500])
            sys.exit(1)

    log.error("Query timed out after %ds", max_wait)
    sys.exit(1)


# ── Step 4: Download results ─────────────────────────────────────────────────

def download_results(
    session: http_requests.Session, config: dict, handle_id: str, result_data: dict
) -> Path:
    """Download the query results as CSV."""
    api_cfg = config.get("api", {})
    base = api_cfg.get("base_url", "http://fdp.fkinternal.com/p/fdp/lens/lensapi/queryapi")

    download_dir = ROOT_DIR / "downloads"
    download_dir.mkdir(exist_ok=True)

    signed_url = result_data.get("signedUrl")
    result_set_path = result_data.get("resultSetPath")
    log.info("signedUrl: %s", str(signed_url)[:200] if signed_url else "None")
    log.info("resultSetPath: %s", str(result_set_path)[:200] if result_set_path else "None")

    download_url = (
        result_data.get("downloadUrl")
        or result_data.get("resultUrl")
        or signed_url
    )

    if not download_url:
        candidates = [
            f"{base}/queries/{handle_id}/results/download",
            f"{base}/queries/{handle_id}/download",
            f"{base}/query/{handle_id}/results/download",
            f"{base}/savedqueries/{handle_id}/results/download",
            f"{base}/queries/{handle_id}/results",
            f"http://fdp.fkinternal.com/p/fdp/lens/lensapi/queryapi/results/{handle_id}/download",
        ]

        if result_set_path:
            candidates.insert(0, f"{base}/results/download?path={result_set_path}")
            candidates.insert(0, result_set_path)

        for url in candidates:
            log.info("Trying download: %s", url)
            try:
                resp = session.get(url, timeout=60, stream=True, allow_redirects=True)
                ct = resp.headers.get("Content-Type", "")
                cl = int(resp.headers.get("Content-Length", "0") or "0")
                log.info("  -> %d | Content-Type: %s | Length: %d", resp.status_code, ct, cl)

                if resp.status_code == 200 and (
                    "csv" in ct or "octet" in ct or "text/plain" in ct or cl > 100
                ):
                    download_url = url
                    break
            except Exception as e:
                log.info("  -> error: %s", e)

    if not download_url:
        log.warning("Could not auto-discover download URL.")
        log.info("Result data keys: %s", list(result_data.keys()))
        for key in ("signedUrl", "resultSetPath", "downloadUrl", "resultUrl"):
            log.info("  %s = %s", key, str(result_data.get(key))[:300])
        log.info("Result page: http://fdp.fkinternal.com/query/result/%s", handle_id)
        sys.exit(1)

    log.info("Downloading from: %s", download_url)
    resp = session.get(download_url, timeout=600, stream=True)
    resp.raise_for_status()

    content_disp = resp.headers.get("Content-Disposition", "")
    if "filename=" in content_disp:
        # Strip quotes and directory components to prevent path traversal.
        raw_name = content_disp.split("filename=")[-1].strip('" ')
        filename = Path(raw_name).name
        if not filename or filename in (".", ".."):
            filename = f"citydash_data_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"citydash_data_{date_str}.csv"

    dest = download_dir / filename
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    log.info("Downloaded: %s (%s)", dest.name, _file_size(dest))
    return dest


# ── Step 5: Save and rename ──────────────────────────────────────────────────

def rename_output(downloaded: Path, config: dict) -> Path:
    out_cfg = config.get("output", {})
    out_folder = ROOT_DIR / out_cfg.get("folder", "data")
    out_folder.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    pattern = out_cfg.get("rename_pattern", "citydash_data_{date}")
    base_name = pattern.replace("{date}", date_str)
    ext = downloaded.suffix or f".{out_cfg.get('format', 'csv')}"

    final_path = out_folder / f"{base_name}{ext}"

    # Ensure the resolved path stays inside `out_folder` (defence-in-depth vs.
    # malicious filenames from Content-Disposition or config overrides).
    try:
        final_path.resolve().relative_to(out_folder.resolve())
    except ValueError:
        log.error("Output path escapes target directory: %s", final_path)
        sys.exit(1)

    shutil.move(str(downloaded), str(final_path))
    log.info("Saved to %s", final_path.relative_to(ROOT_DIR))
    return final_path


# ── Step 6: Git commit and push ──────────────────────────────────────────────

def git_push(file_path: Path, config: dict) -> None:
    gh_cfg = config.get("github", {})
    if not gh_cfg.get("enabled", True):
        log.info("GitHub push disabled in config")
        return

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    message = gh_cfg.get("commit_message", "chore: update city dashboard data {date}")
    message = message.replace("{date}", date_str)
    branch = gh_cfg.get("branch", "main")
    remote = gh_cfg.get("remote", "origin")
    repo_path = Path(gh_cfg.get("repo_path", "."))
    if not repo_path.is_absolute():
        repo_path = ROOT_DIR / repo_path

    # Copy to repo root so the dashboard can load it directly.
    root_copy = repo_path / file_path.name
    if file_path.resolve() != root_copy.resolve():
        shutil.copy2(str(file_path), str(root_copy))
        log.info("Copied to repo root: %s", root_copy.name)

    log.info("Git: staging %s", root_copy.name)

    # All command arguments are fixed executables / validated config values —
    # never user input from the web — passed as an argv list (no shell=True).
    commands = [
        ["git", "add", root_copy.name],
        ["git", "commit", "-m", message],
        ["git", "push", remote, branch],
    ]
    for cmd in commands:
        result = subprocess.run(
            cmd, cwd=str(repo_path), capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            combined = (result.stdout or "") + (result.stderr or "")
            if "nothing to commit" in combined:
                log.info("No new changes to commit")
                return
            log.error("Git command failed: %s\n%s", " ".join(cmd), combined)
            sys.exit(1)

    log.info("Pushed to %s/%s", remote, branch)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("  Seller x City Dashboard — FDP API Scraper Starting")
    log.info("=" * 60)

    config = load_config()
    username, password = get_credentials()
    sql = get_query()
    if not sql:
        log.error("No query found. Create query.sql or check config")
        sys.exit(1)

    log.info("Step 1/5: Authenticating...")
    cookies = extract_cookies(config, username, password)

    session = http_requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value, domain="fdp.fkinternal.com")
    session.headers.update(FDP_API_HEADERS)
    session.headers["accept"] = "application/json; q=1.0, text/*; q=0.8, */*; q=0.1"

    log.info("Step 2/5: Submitting query via API...")
    handle_id = submit_query(session, config, sql)

    log.info("Step 3/5: Polling for results...")
    result_data = poll_status(session, config, handle_id)

    log.info("Step 4/5: Downloading results...")
    downloaded = download_results(session, config, handle_id, result_data)

    log.info("Step 5/5: Saving and pushing to Git...")
    final_path = rename_output(downloaded, config)
    git_push(final_path, config)

    log.info("=" * 60)
    log.info("  Done! Data saved to %s", final_path.relative_to(ROOT_DIR))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
