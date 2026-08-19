"""Ingest the Maya hieroglyphic script dataset into the `script` catalog category.

Reads the combined workbook + extracted glyph images from the sibling
`maya_script/` directory, copies images into `data/media/script/maya_script/`,
and writes `data/parquet/script/maya_script/script.parquet`.

Requires openpyxl (not a runtime dependency of this backend) - run via:

    uv run --with openpyxl python scripts/ingest_maya_script.py
"""

import argparse
import os
import shutil
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = SCRIPT_DIR.parent
STLING_ROOT = BACKEND_ROOT.parent

DEFAULT_XLSX = STLING_ROOT / "maya_script" / "files" / "260803_3M2_combined.xlsx"
DEFAULT_IMAGES_DIR = (
    STLING_ROOT / "maya_script" / "files" / "260803_3M2_pictures" / "images"
)
DEFAULT_OUT_DIR = BACKEND_ROOT / "data"

SOURCE_COLUMNS = [
    "objabbr",
    "blsurfpgfr",
    "blcoord",
    "picture_color",
    "class",
    "picture_filename",
    "bllogosyll",
    "blhyphen",
    "blmaya1",
    "blmaya2",
    "blengl",
    "blgraphcodes",
    "blgreg",
    "objgreg",
    "site",
    "lat",
    "lon",
]


def build_id(site, objabbr, blcoord, seen):
    base = f"{site}_{objabbr}_{blcoord}".replace(" ", "_") if blcoord else f"{site}_{objabbr}"
    count = seen.get(base, 0)
    seen[base] = count + 1
    return base if count == 0 else f"{base}_{count + 1}"


def ingest(xlsx_path: Path, images_dir: Path, out_dir: Path):
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Combined workbook not found: {xlsx_path}")
    if not images_dir.exists():
        raise FileNotFoundError(f"Images directory not found: {images_dir}")

    df = pd.read_excel(xlsx_path, sheet_name="Combined", engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    missing_cols = [c for c in SOURCE_COLUMNS if c not in df.columns]
    if missing_cols:
        print(f"WARNING: expected columns missing from source sheet: {missing_cols}")

    media_out_dir = out_dir / "media" / "script" / "maya_script"
    media_out_dir.mkdir(parents=True, exist_ok=True)

    parquet_out_dir = out_dir / "parquet" / "script" / "maya_script"
    parquet_out_dir.mkdir(parents=True, exist_ok=True)

    seen_ids = {}
    ids, names, media_urls, descriptions, dates = [], [], [], [], []
    images_copied, images_missing = 0, 0

    for _, row in df.iterrows():
        site = row.get("site")
        objabbr = row.get("objabbr")
        blcoord = row.get("blcoord")
        picture_filename = row.get("picture_filename")
        blmaya1 = row.get("blmaya1")
        blengl = row.get("blengl")
        blgreg = row.get("blgreg")
        objgreg = row.get("objgreg")

        ids.append(build_id(site, objabbr, blcoord, seen_ids))
        blcoord_label = f" [{blcoord}]" if pd.notna(blcoord) else ""
        names.append(f"{objabbr}{blcoord_label}")
        descriptions.append(
            " / ".join(str(v) for v in (blmaya1, blengl) if pd.notna(v))
        )
        dates.append(blgreg if pd.notna(blgreg) else (objgreg if pd.notna(objgreg) else None))

        media_url = None
        if pd.notna(picture_filename):
            src_image = images_dir / str(picture_filename)
            if src_image.exists():
                shutil.copy2(src_image, media_out_dir / str(picture_filename))
                media_url = f"/media/script/maya_script/{picture_filename}"
                images_copied += 1
            else:
                images_missing += 1
                print(f"WARNING: missing image file {src_image}")
        media_urls.append(media_url)

    df["ID"] = ids
    df["Name"] = names
    df["Description"] = descriptions
    df["Date"] = dates
    df["Media_URL"] = media_urls
    df["Latitude"] = df["lat"].astype(float)
    df["Longitude"] = df["lon"].astype(float)

    # Source columns are mixed-type (openpyxl yields int/float/str per-cell for
    # nominally textual columns like blcoord) - normalize to string so pyarrow
    # can write a single consistent column type.
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda v: str(v) if pd.notna(v) else None)

    parquet_path = parquet_out_dir / "script.parquet"
    df.to_parquet(parquet_path, engine="pyarrow", index=False)

    print(f"Rows ingested: {len(df)}")
    print(f"Distinct sites: {df['site'].nunique()}")
    print(f"Distinct objects: {df['objabbr'].nunique()}")
    print(f"Images copied: {images_copied}, missing: {images_missing}")
    print(f"Parquet written to: {parquet_path}")
    print(f"Media written to: {media_out_dir}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xlsx", type=Path, default=DEFAULT_XLSX)
    parser.add_argument("--images-dir", type=Path, default=DEFAULT_IMAGES_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()

    ingest(args.xlsx, args.images_dir, args.out_dir)


if __name__ == "__main__":
    main()
