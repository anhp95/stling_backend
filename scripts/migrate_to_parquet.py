import os
import shutil
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Configuration
DATA_ROOT = Path("d:/project/lang/data")
CSV_BACKUP_ROOT = DATA_ROOT / "csv"
PARQUET_ROOT = DATA_ROOT / "parquet"


def migrate():
    print(f"Starting migration from {DATA_ROOT}")
    print(f"  - Source CSVs will move to: {CSV_BACKUP_ROOT}")
    print(f"  - Parquet files will be generated in: {PARQUET_ROOT}")

    # Ensure output directories exist
    CSV_BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    PARQUET_ROOT.mkdir(parents=True, exist_ok=True)

    # 1. First, scan original locations for CSVs
    csv_files = []
    reserved_dirs = {CSV_BACKUP_ROOT.name, PARQUET_ROOT.name}
    for root, dirs, files in os.walk(DATA_ROOT):
        dirs[:] = [d for d in dirs if d not in reserved_dirs]
        for file in files:
            if file.lower().endswith(".csv"):
                csv_files.append(Path(root) / file)

    # 2. Also scan CSV backup root for missing parquets
    # This helps resume if files were moved but not yet converted
    for root, dirs, files in os.walk(CSV_BACKUP_ROOT):
        for file in files:
            if file.lower().endswith(".csv"):
                full_path = Path(root) / file
                rel_path = full_path.relative_to(CSV_BACKUP_ROOT)
                # Check if parquet exists in PARQUET_ROOT
                target_parquet = PARQUET_ROOT / rel_path.with_suffix(".parquet")
                if not target_parquet.exists():
                    # We add it as a "virtual" original path if it's missing
                    # The script will handle current_source logic
                    orig_path = DATA_ROOT / rel_path
                    if orig_path not in csv_files:
                        csv_files.append(orig_path)

    print(f"Found {len(csv_files)} CSV tasks to process.")

    success_count = 0
    skip_count = 0
    fail_count = 0

    for csv_path in csv_files:
        try:
            # Calculate relative path from DATA_ROOT
            rel_path = csv_path.relative_to(DATA_ROOT)

            # Define target paths
            target_csv_path = CSV_BACKUP_ROOT / rel_path
            target_parquet_path = PARQUET_ROOT / rel_path.with_suffix(".parquet")

            # Check if already done (idempotency)
            if target_csv_path.exists() and target_parquet_path.exists():
                print(f"Skipping (already migrated): {rel_path}")
                skip_count += 1
                continue

            # Check if source CSV exists (it might have been moved in a previous interrupted run)
            if not csv_path.exists():
                # If source is gone but target CSV exists, maybe we just need to generate parquet?
                if target_csv_path.exists() and not target_parquet_path.exists():
                    current_source = target_csv_path
                else:
                    print(f"Skipping (source missing): {csv_path}")
                    fail_count += 1
                    continue
            else:
                current_source = csv_path

            # Create parent directories
            target_csv_path.parent.mkdir(parents=True, exist_ok=True)
            target_parquet_path.parent.mkdir(parents=True, exist_ok=True)

            # Convert to Parquet
            print(f"Converting: {rel_path}")
            try:
                # Use pandas with pyarrow engine for robust CSV reading
                # explicit index=False to avoid saving index
                df = pd.read_csv(current_source, low_memory=False)

                # Check for empty dataframe
                if df.empty:
                    print(f"  Warning: Empty CSV {rel_path}")

                # Write Parquet
                table = pa.Table.from_pandas(df)
                pq.write_table(table, target_parquet_path)

            except Exception as e:
                print(f"  Error converting {rel_path}: {e}")
                fail_count += 1
                continue

            # Move CSV if we were working from the original location
            if current_source == csv_path:
                shutil.move(str(csv_path), str(target_csv_path))
                print(f"  Moved source to: {target_csv_path}")

            success_count += 1

        except Exception as e:
            print(f"Unexpected error processing {csv_path}: {e}")
            fail_count += 1

    print("-" * 40)
    print("Migration Complete")
    print(f"  Converted & Moved: {success_count}")
    print(f"  Skipped: {skip_count}")
    print(f"  Failed: {fail_count}")


if __name__ == "__main__":
    migrate()
