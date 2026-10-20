"""
Orchestrator — run all ingestion pipelines in sequence.

Usage:
  python run_all.py --dry-run --limit 50
  python run_all.py --pipelines linkedin,web_search,dice
  python run_all.py --pipelines resumes
"""
import argparse
import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).parent

CERT_SCRIPT = str(BASE / "web_search/find_cert.py")

PIPELINES = {
    "resumes":     ["python3", str(BASE / "resumes/ingest.py"), "--mode", "all"],
    "linkedin":    ["python3", str(BASE / "linkedin/ingest.py")],
    "web_search":  ["python3", str(BASE / "web_search/ingest.py")],
    "dice":        ["python3", str(BASE / "dice/ingest.py")],
    "github":      ["python3", str(BASE / "github/ingest.py")],
    "signalhire":  ["python3", str(BASE / "signalhire/ingest.py"), "--mode", "search"],
    "credly":      ["python3", str(BASE / "credly/ingest.py"), "--mode", "catalog"],
    "indeed":      ["python3", str(BASE / "indeed/ingest.py")],
    # ServiceNow cert discovery — one entry per cert, same script different --cert param
    "cert_cis_csm": ["python3", CERT_SCRIPT, "--cert", "CIS-CSM"],
    "cert_cis_vr":  ["python3", CERT_SCRIPT, "--cert", "CIS-VR"],
}

# Default run order — own data first, then discovery, then enrichment
DEFAULT_ORDER = ["resumes", "linkedin", "web_search", "dice", "github", "credly", "signalhire", "indeed"]


def run_pipeline(name: str, cmd: list, dry_run: bool, limit: int) -> bool:
    if dry_run:
        cmd.append("--dry-run")
    if limit:
        cmd += ["--limit", str(limit)]

    print(f"\n{'='*60}")
    print(f"  Running: {name}")
    print(f"  CMD: {' '.join(cmd)}")
    print(f"{'='*60}")

    result = subprocess.run(cmd, cwd=BASE)
    success = result.returncode == 0
    print(f"  {'✓' if success else '✗'} {name} exited with code {result.returncode}")
    return success


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipelines", default=",".join(DEFAULT_ORDER),
                        help="Comma-separated pipeline names to run")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    selected = args.pipelines.split(",")
    results = {}

    for name in selected:
        if name not in PIPELINES:
            print(f"[WARN] unknown pipeline: {name}, skipping")
            continue
        cmd = list(PIPELINES[name])  # copy
        results[name] = run_pipeline(name, cmd, args.dry_run, args.limit)

    print(f"\n{'='*60}")
    print("Summary:")
    for name, ok in results.items():
        print(f"  {'✓' if ok else '✗'} {name}")
    print(f"{'='*60}")

    if not all(results.values()):
        sys.exit(1)
