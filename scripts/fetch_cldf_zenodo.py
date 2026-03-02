import pathlib
import shutil
from glob import glob

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cldfzenodo import API

BASE_DIR = "../data/csv/spoken_language"
PARQUET_DIR = "../data/parquet/spoken_language"


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


def fetch_zenodo_cldf():
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
        else:
            shutil.rmtree(output_dir)
            print(f"[DISQUALIFIED] '{r_name}' did not pass validation — folder removed")


if __name__ == "__main__":
    fetch_zenodo_cldf()
