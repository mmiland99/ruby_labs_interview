# User/Post/Comment fetching pipeline from JSONPlaceholder

## Overview
This script fetches data from the JSONPlaceholder API and writes a single `output.csv` where **each row is a comment**, with fields from **user**, **post** and **comment**.

## Project structure
```
.
├─ requirements.txt
├─ README.md
└─ src/
   └─ etl/
      ├─ __init__.py
      ├─ main.py          # Orchestrates the pipeline + logs summary + writes output.csv
      ├─ api_client.py    # Async GET JSON with retry function + exponential backoff for certain errors + logging
      ├─ fetchers.py      # Fetch users/posts/comments + bounded concurrency helpers
      ├─ validators.py    # “log and skip” validation for required fields
      ├─ transformers.py  # latest-by-id selection + flattening to CSV row dicts
      └─ csv_writer.py    # Writes CSV header once + one row per comment
```
## What it does
- Fetches `/users`
- Keeps only users with **even** `id`
- For each selected user:
  - Fetches `/users/{id}/posts` and takes the **latest 5** (by highest `id`)
- For each selected post:
  - Fetches `/posts/{id}/comments` and takes the **latest 3** (by highest `id`)
- Validates required fields; logs issues and skips invalid records
- Writes `output.csv` (one row per comment):  
  `user_id,user_name,post_id,post_title,comment_id,comment_body,comment_email`

## Assumptions & design choices
- JSONPlaceholder posts/comments don’t include dates, so “latest” is interpreted as **highest `id`**.
- `comment_body` is cleaned to a single line to avoid multiline CSV cells for each record.
- Uses **async I/O** with `aiohttp` and a single shared `ClientSession` (connection pooling).
- Retries transient request failures (timeouts, 429, 5xx) with **exponential backoff** using Tenacity.

## How to Setup:
**bash:**
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH="src"
python -m etl.main
```
**Windows PowerShell:**
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:PYTHONPATH="src"
python -m etl.main
```