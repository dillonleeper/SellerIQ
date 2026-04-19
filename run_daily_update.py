import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def run_step(script_name: str, label: str) -> None:
    script_path = ROOT / script_name
    print(f"\n--- {label} ---")
    subprocess.run([sys.executable, str(script_path)], cwd=ROOT, check=True)


def step_ingest_sales() -> None:
    run_step("ingest_sales_traffic.py", "Ingest sales")


def step_ingest_inventory() -> None:
    run_step("ingest_inventory.py", "Ingest inventory")


def step_ingest_catalog() -> None:
    run_step("ingest_catalog.py", "Ingest catalog")


def step_ingest_listings() -> None:
    run_step("ingest_listings.py", "Ingest listings")


def step_transform() -> None:
    run_step("build_fct_sales_daily.py", "Transform sales fact")


def main() -> None:
    print("=" * 60)
    print("Daily Update Pipeline")
    print("=" * 60)

    is_monday = datetime.now().weekday() == 0

    step_ingest_sales()
    step_ingest_inventory()

    if is_monday:
        step_ingest_catalog()
        step_ingest_listings()
    else:
        print("\nSkipping catalog and listings ingestion because today is not Monday.")

    step_transform()

    print("\n" + "=" * 60)
    print("Daily update complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()