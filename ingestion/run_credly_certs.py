#!/usr/bin/env python3
"""
Weekly Credly cert ingestion — CIS-HAM, CIS-SAM, CSA, CAD
Launches each cert as a parallel background process.
Sends periodic status emails every 5 minutes until all done.
Cron: 0 2 * * 1  (every Monday 2 AM UTC)

Usage:
  python3 run_credly_certs.py
  python3 run_credly_certs.py --dry-run
  python3 run_credly_certs.py --cert CIS-HAM
"""
import argparse
import logging
import os
import re
import smtplib
import subprocess
import sys
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

CERTS       = ['CIS-HAM', 'CIS-SAM', 'CSA', 'CAD']
STATUS_INTERVAL = 300   # seconds between status emails
NOTIFY_EMAIL    = 'navin@prajnov.com'

_base = os.path.dirname(os.path.abspath(__file__))
FIND_CERT     = next(p for p in [
    os.path.join(_base, 'find_cert.py'),
    os.path.join(_base, 'web_search', 'find_cert.py'),
] if os.path.exists(p))
INGESTION_DIR = _base
LOG_DIR       = os.path.join(os.path.expanduser('~'), 'logs')


# ── Email ─────────────────────────────────────────────────────────────────────

def _send_email(subject: str, body: str):
    gmail_user = os.getenv('GMAIL_EMAIL', '')
    gmail_pass = os.getenv('GMAIL_APP_PASSWORD', '')
    if not gmail_user or not gmail_pass:
        log.warning('GMAIL_EMAIL / GMAIL_APP_PASSWORD not set — skipping email')
        return
    try:
        msg = MIMEMultipart()
        msg['From']    = gmail_user
        msg['To']      = NOTIFY_EMAIL
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(gmail_user, gmail_pass)
            smtp.sendmail(gmail_user, NOTIFY_EMAIL, msg.as_string())
        log.info('Email sent: %s', subject)
    except Exception as e:
        log.warning('Email failed: %s', e)


# ── Log parsing ───────────────────────────────────────────────────────────────

def _parse_progress(log_path: str) -> dict:
    """Extract written count, total, current page, total pages from log tail."""
    result = {'written': 0, 'total': 0, 'page': 0, 'total_pages': 0, 'errors': 0, 'done': False}
    if not os.path.exists(log_path):
        return result
    try:
        with open(log_path) as f:
            content = f.read()
        m = re.search(r'Total earners:\s*(\d+)\s*across\s*(\d+)\s*pages', content)
        if m:
            result['total']       = int(m.group(1))
            result['total_pages'] = int(m.group(2))
        # Last page line
        pages = re.findall(r'page\s+(\d+)/(\d+)', content)
        if pages:
            result['page'] = int(pages[-1][0])
        # Written count from DONE line
        m2 = re.search(r"written.*?(\d+)", content.split('DONE')[-1]) if 'DONE' in content else None
        if m2:
            result['written'] = int(m2.group(1))
        else:
            result['written'] = len(re.findall(r'✓', content))
        result['errors'] = len(re.findall(r'\[ERROR\]', content))
        result['done']   = 'DONE' in content
    except Exception:
        pass
    return result


def _status_block(cert: str, p, log_path: str, start_time: float) -> str:
    prog    = _parse_progress(log_path)
    elapsed = int(time.time() - start_time)
    running = p.poll() is None

    if prog['done'] or (not running and p.returncode == 0):
        status = '✅ DONE'
    elif not running:
        status = f'❌ FAILED (rc={p.returncode})'
    else:
        status = '🔄 Running'

    pct = f"{100*prog['page']//prog['total_pages']}%" if prog['total_pages'] else 'N/A'
    eta = ''
    if running and prog['page'] > 0 and prog['total_pages'] > 0:
        rate = prog['page'] / elapsed if elapsed > 0 else 0
        remaining = (prog['total_pages'] - prog['page']) / rate if rate > 0 else 0
        eta = f"  ETA: ~{int(remaining//60)}m {int(remaining%60)}s"

    return (
        f"{cert}: {status}\n"
        f"  Earners: {prog['written']:,} / {prog['total']:,}  |  "
        f"Page: {prog['page']}/{prog['total_pages']} ({pct})  |  "
        f"Errors: {prog['errors']}  |  Elapsed: {elapsed//60}m {elapsed%60}s{eta}\n"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def run(certs: list, dry_run: bool):
    os.makedirs(LOG_DIR, exist_ok=True)
    env       = {**os.environ, 'PYTHONPATH': INGESTION_DIR}
    run_start = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    # Launch all certs in parallel
    procs = {}
    for cert in certs:
        log_path = os.path.join(LOG_DIR, f'credly_{cert.lower().replace("-","_")}.log')
        cmd      = [sys.executable, FIND_CERT, '--cert', cert] + (['--dry-run'] if dry_run else [])
        log_fh   = open(log_path, 'a')
        p        = subprocess.Popen(cmd, env=env, stdout=log_fh, stderr=log_fh)
        procs[cert] = {'proc': p, 'log': log_path, 'start': time.time()}
        log.info('Started %s — pid=%d  log=%s', cert, p.pid, log_path)

    _send_email(
        f'[Prajnov] Credly ingestion started — {run_start}',
        f'Started ingestion for: {", ".join(certs)}\n\n'
        f'Logs: {LOG_DIR}\nStatus emails every {STATUS_INTERVAL//60} minutes.\n'
    )

    # Poll until all done, sending status every STATUS_INTERVAL seconds
    last_status = time.time()
    while True:
        all_done = all(v['proc'].poll() is not None for v in procs.values())
        now      = time.time()

        if all_done or (now - last_status >= STATUS_INTERVAL):
            lines = [f'Credly Ingestion Status — {datetime.now(timezone.utc).strftime("%H:%M UTC")}\n']
            for cert, v in procs.items():
                lines.append(_status_block(cert, v['proc'], v['log'], v['start']))
            body = '\n'.join(lines)

            subject = (
                f'[Prajnov] Credly ingestion {"complete" if all_done else "in progress"} '
                f'— {datetime.now(timezone.utc).strftime("%H:%M UTC")}'
            )
            _send_email(subject, body)
            log.info('\n%s', body)
            last_status = now

        if all_done:
            break
        time.sleep(30)

    # Final summary
    results = {
        cert: ('ok' if v['proc'].returncode == 0 else f'FAILED rc={v["proc"].returncode}')
        for cert, v in procs.items()
    }
    failed = [c for c, s in results.items() if s != 'ok']
    if failed:
        log.error('Failed certs: %s', failed)
        sys.exit(1)
    log.info('All certs completed successfully.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cert', choices=CERTS, help='Run single cert only')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    run([args.cert] if args.cert else CERTS, args.dry_run)
