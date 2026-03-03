import pathlib
import sys
import shutil
from glob import glob

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cldfzenodo import API

# Allow sibling script imports regardless of cwd
_SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent

_DATA_ROOT = _SCRIPTS_DIR.parent / "data"

BASE_DIR = _DATA_ROOT / "csv" / "spoken_language"
PARQUET_DIR = _DATA_ROOT / "parquet" / "spoken_language"

# Gloss index parquet outputs
GLOSS_INDEX_PARQUET = _DATA_ROOT / "parquet" / "concepticon_gloss_index.parquet"
DISTINCT_GLOSS_PARQUET = _DATA_ROOT / "parquet" / "distinct_concepticon_glosses.parquet"


def check_wordlist_csv(folder: str) -> bool:
    """
    Validate that the CLDF wordlist folder contains the required CSVs and columns.
    Returns True if qualified, False otherwise.
    """
    try:
        forms_path = f"{folder}/forms.csv"
        languages_path = f"{folder}/languages.csv"
        parameters_path = f"{folder}/parameters.csv"

        for path in (forms_path, languages_path, parameters_path):
            if not pathlib.Path(path).exists():
                print(f"  [MISSING] {path}")
                return False

        form_csv = pd.read_csv(forms_path)
        language_csv = pd.read_csv(languages_path)
        parameter_csv = pd.read_csv(parameters_path)

        assert "ID" in language_csv.columns, "Missing 'ID' in languages.csv"
        assert "Language_ID" in form_csv.columns, "Missing 'Language_ID' in forms.csv"
        assert "Parameter_ID" in form_csv.columns, "Missing 'Parameter_ID' in forms.csv"
        assert "ID" in parameter_csv.columns, "Missing 'ID' in parameters.csv"
        assert (
            "Concepticon_Gloss" in parameter_csv.columns
        ), "Missing 'Concepticon_Gloss' in parameters.csv"

        return True

    except AssertionError as e:
        print(f"  [INVALID] {e}")
        return False
    except Exception as e:
        print(f"  [ERROR] Could not validate {folder}: {e}")
        return False


def convert_folder_to_parquet(folder: str, r_name: str) -> None:
    """
    Convert all CSV files in a CLDF folder to Parquet and save under PARQUET_DIR/<r_name>/.
    """
    parquet_folder = pathlib.Path(PARQUET_DIR) / r_name
    parquet_folder.mkdir(parents=True, exist_ok=True)

    csv_files = glob(f"{folder}/*.csv")
    for csv_path in csv_files:
        stem = pathlib.Path(csv_path).stem
        parquet_path = parquet_folder / f"{stem}.parquet"

        try:
            df = pd.read_csv(csv_path, low_memory=False)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_path)
            print(f"  [PARQUET] {stem}.csv → {parquet_path}")
        except Exception as e:
            print(f"  [ERROR] Failed to convert {csv_path} to parquet: {e}")


def update_gloss_index() -> None:
    """
    Build the concepticon gloss index from all downloaded datasets directly,
    and save the output directly as parquet without using intermediate CSVs.
    """
    print("[INDEX] Updating concepticon gloss index ...")

    index_data = []
    distinct_glosses = set()
    count = 0

    # Scan all datasets from the newly generated parquet folder
    if PARQUET_DIR.exists():
        for dataset_path in PARQUET_DIR.iterdir():
            if not dataset_path.is_dir():
                continue

            dataset_name = dataset_path.name
            params_parquet = dataset_path / "parameters.parquet"

            if params_parquet.exists():
                try:
                    df = pd.read_parquet(params_parquet)
                    if "Concepticon_Gloss" in df.columns:
                        # Drop duplicates & nulls within this dataset
                        glosses = df["Concepticon_Gloss"].dropna().unique()

                        for gloss in sorted(glosses):
                            index_data.append(
                                {
                                    "dataset_name": dataset_name,
                                    "Concepticon_Gloss": str(gloss),
                                }
                            )
                            distinct_glosses.add(str(gloss))
                            count += 1
                except Exception as e:
                    print(f"  [ERROR] Processing parameters for {dataset_name}: {e}")

    try:
        # Create DataFrames
        index_df = (
            pd.DataFrame(index_data)
            if index_data
            else pd.DataFrame(columns=["dataset_name", "Concepticon_Gloss"])
        )
        distinct_df = pd.DataFrame(
            {"Concepticon_Gloss": sorted(list(distinct_glosses))}
        )

        # Ensure parent directories exist
        GLOSS_INDEX_PARQUET.parent.mkdir(parents=True, exist_ok=True)
        DISTINCT_GLOSS_PARQUET.parent.mkdir(parents=True, exist_ok=True)

        # Write directly to Parquet
        pq.write_table(pa.Table.from_pandas(index_df), GLOSS_INDEX_PARQUET)
        print(f"  [PARQUET] → {GLOSS_INDEX_PARQUET} ({count} entries)")

        pq.write_table(pa.Table.from_pandas(distinct_df), DISTINCT_GLOSS_PARQUET)
        print(
            f"  [PARQUET] → {DISTINCT_GLOSS_PARQUET} ({len(distinct_glosses)} unique glosses)"
        )

    except Exception as e:
        print(f"  [ERROR] Failed to save gloss index parquets: {e}")


def fetch_zenodo_cldf():
    new_datasets_added = False

    try:
        records = API.iter_records(keyword="cldf:Wordlist")
    except Exception as e:
        print(f"[NETWORK ERROR] Could not connect to Zenodo API: {e}")
        print("Check your internet connection and try again.")
        return

    for rec in records:
        try:
            r_name = (
                rec.title.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
            )
            if rec.github_repos is not None:
                r_name = rec.github_repos.name
        except Exception as e:
            print(f"[NETWORK ERROR] Lost connection while iterating records: {e}")
            break

        doi = rec.doi
        output_dir = f"{BASE_DIR}/{r_name}"

        # ── Already downloaded ────────────────────────────────────────────────
        if pathlib.Path(output_dir).exists():
            print(f"[EXISTS] '{r_name}' already downloaded at {output_dir}")
            continue

        # ── Download ──────────────────────────────────────────────────────────
        try:
            pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
            rec.download_dataset(output_dir)
            print(f"[DOWNLOADED] '{r_name}' from {doi} → {output_dir}")
        except Exception as e:
            print(f"[ERROR] Failed to download '{r_name}' from {doi}: {e}")
            # Clean up empty dir if download failed
            if pathlib.Path(output_dir).exists() and not glob(f"{output_dir}/*"):
                shutil.rmtree(output_dir)
            continue

        # ── Remove if no CSVs at all ─────────────────────────────────────────
        if len(glob(f"{output_dir}/*.csv")) < 1:
            shutil.rmtree(output_dir)
            print(f"[REMOVED] '{r_name}' has no CSV files — discarded")
            continue

        # ── Validate ─────────────────────────────────────────────────────────
        print(f"[CHECKING] Validating '{r_name}' ...")
        if check_wordlist_csv(output_dir):
            print(f"[QUALIFIED] '{r_name}' passed validation — converting to parquet")
            convert_folder_to_parquet(output_dir, r_name)
            new_datasets_added = True
        else:
            shutil.rmtree(output_dir)
            print(f"[DISQUALIFIED] '{r_name}' did not pass validation — folder removed")

    # ── Update gloss index if any new dataset was saved ───────────────────
    if new_datasets_added:
        update_gloss_index()
    else:
        print("[INDEX] No new datasets added — gloss index unchanged.")


if __name__ == "__main__":
    fetch_zenodo_cldf()
