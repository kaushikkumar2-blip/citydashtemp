# Seller × City Dashboard + FDP Automation Agent

Two pieces, one repository:

1. **`dashboard.py`** — a Streamlit dashboard that reads `362c62a8adb9d17ecb5a6c9d33385822.csv` and renders seller × city performance metrics.
2. **`scraper.py`** — an unattended automation agent that logs into `fdp.fkinternal.com`, runs `query.sql`, downloads the CSV, overwrites the dashboard data file, and pushes the update to GitHub on a daily schedule.

---

## How the automation works

1. **Windows Task Scheduler** triggers `run_scraper.bat` daily at the configured time.
2. **Playwright** launches Chromium with a persistent profile (`.chrome_profile/`). The first run needs you to complete LDAP + 2FA once; subsequent runs reuse the session.
3. The scraper extracts auth cookies and calls the **QAAS REST API** directly (no fragile UI scraping) to:
   - submit the SQL in `query.sql`,
   - poll for query completion,
   - download the CSV result.
4. The result is saved to `data/362c62a8adb9d17ecb5a6c9d33385822.csv`, copied to the repo root (where the dashboard loads it from), and **auto-committed + pushed** to GitHub.

---

## Setup

### 1. Install Python dependencies

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure credentials

Copy `.env.example` to `.env` and fill in your LDAP credentials:

```
FDP_USERNAME=your_username
FDP_PASSWORD=your_password
```

`.env` is git-ignored — never commit it.

### 3. Adjust the SQL if needed

`query.sql` already produces the exact columns the dashboard expects:

```
reporting_date, destination_city, seller_type, payment_type,
PHin, conv_num, zero_attempt_num, fm_created, fm_picked, fm_d0_picked,
DHin, D0_OFD, First_attempt_delivered, fac_deno,
total_delivered_attempts, total_attempts, rfr_num, rfr_deno,
Breach_Num, Breach_Den, breach_plus1_num
```

The token `{end_date}` is replaced with **yesterday's date** (YYYYMMDD) at runtime.

### 4. Check `config.yaml`

The defaults in `config.yaml` match the dashboard:

- `output.rename_pattern: "362c62a8adb9d17ecb5a6c9d33385822"` — keeps the filename the dashboard loads.
- `api.source_name`, `api.team_name`, `api.queue_name` — tune these to your FDP team/queue.
- `github.branch` — defaults to `main`; change to `master` if your repo uses that.

### 5. Do a manual first run (to complete 2FA once)

```bash
.venv\Scripts\activate
set FDP_USERNAME=your_username
set FDP_PASSWORD=your_password
python scraper.py
```

Chrome will open. Complete LDAP + 2FA. The profile in `.chrome_profile/` is reused for all future headless runs.

Tip: set `browser.headless: false` in `config.yaml` to watch it run and verify selectors are correct.

### 6. Schedule daily runs

Run `setup_scheduler.bat` **as Administrator**. It creates a Windows Task Scheduler entry (`CityDash_Daily_Scraper`) that runs daily at 08:00. To change the time, edit `RUN_TIME` in `setup_scheduler.bat` and re-run it.

Useful commands:

```bash
schtasks /query  /tn "CityDash_Daily_Scraper"
schtasks /run    /tn "CityDash_Daily_Scraper"
schtasks /delete /tn "CityDash_Daily_Scraper" /f
```

---

## Project structure

```
├── dashboard.py                               # Streamlit dashboard (existing)
├── scraper.py                                 # Automation agent
├── query.sql                                  # SQL that produces dashboard CSV
├── config.yaml                                # Scraper settings
├── run_scraper.bat                            # Daily launcher (loads .env)
├── setup_scheduler.bat                        # One-time scheduler setup
├── requirements.txt                           # Dashboard + scraper deps
├── .env.example                               # Credential template
├── .gitignore                                 # Excludes .env, .chrome_profile, logs, …
├── 362c62a8adb9d17ecb5a6c9d33385822.csv       # Dashboard data (auto-updated)
├── data/                                      # Scraper output folder
├── downloads/                                 # Temp download cache
├── logs/                                      # Daily run logs
└── .chrome_profile/                           # Persistent Chrome session
```

---

## Running the dashboard

```bash
streamlit run dashboard.py
```

The dashboard auto-reads the CSV at the repo root, so each successful scraper run automatically updates the view on next refresh.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Login fails | Check `login.*_selector` values in `config.yaml` match the actual form. |
| `No cookies extracted` | Delete `.chrome_profile/` and re-run manually to redo 2FA. |
| Query times out | Increase `api.max_wait_seconds` in `config.yaml`. |
| `Could not auto-discover download URL` | The QAAS download endpoint may have changed — inspect `result_data` keys in logs and adjust `download_results()` candidates. |
| Git push fails | Ensure `git remote -v` is set and that the configured `branch` exists on origin. |
| Dashboard still shows old data | Streamlit caches for 10 minutes (`@st.cache_data(ttl=600)`). Click "Clear cache" from the menu or wait. |

---

## Security notes

- LDAP credentials are loaded **only** from environment variables (`FDP_USERNAME`, `FDP_PASSWORD`) via `.env`; they are never stored in code, config, or logs.
- Git operations run via `subprocess.run(...)` with an argument list (no shell interpolation), so no shell injection is possible.
- The downloaded filename is sanitised (`Path(...).name`) before being written, preventing path-traversal from a malicious `Content-Disposition` header.
- The final output path is verified to stay inside the configured `data/` folder before the file is moved.
