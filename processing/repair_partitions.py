"""
Repair Glue catalog partitions for all silver tables.
Run after enrich_linkedin.py to make new data visible in Athena.

Usage:
  python processing/repair_partitions.py
  python processing/repair_partitions.py --database prajnov_silver
"""
import argparse, os, time
import boto3

REGION   = "ap-south-1"
DATABASE = "prajnov_silver"
WORKGROUP = "prajnov"
BUCKET   = "prajnov-talent-lake"

TABLES = [
    "candidate",
    "candidate_certification",
    "candidate_skill",
    "candidate_experience",
    "candidate_education",
]


def get_athena():
    from botocore.session import get_session
    bs = get_session()
    bs.set_config_variable("profile", os.getenv("AWS_PROFILE", "prajnov"))
    return boto3.Session(botocore_session=bs).client("athena", region_name=REGION)


def run_query(athena, sql, database):
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=WORKGROUP,
    )
    qid = resp["QueryExecutionId"]
    # Poll until done
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state  = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
        print(f"    [WARN] {state}: {reason}")
    return state


def run(database):
    athena = get_athena()
    print(f"Repairing partitions in {database}...\n")
    for table in TABLES:
        sql = f"MSCK REPAIR TABLE {table}"
        print(f"  {sql} ... ", end="", flush=True)
        state = run_query(athena, sql, database)
        print(state)
    print("\nDone. Tables are now queryable in Athena.")
    print(f"\nSample query:")
    print(f"  SELECT canonical_name, current_title, location_state, linkedin_url")
    print(f"  FROM {database}.candidate")
    print(f"  WHERE location_country = 'US' AND source_cert = 'CIS-CSM'")
    print(f"  ORDER BY location_state, canonical_name;")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default=DATABASE)
    args, _ = parser.parse_known_args()
    run(args.database)
