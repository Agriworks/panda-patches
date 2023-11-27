from pathlib import Path
import traceback
from pipeline.patch.patchfile import PatchFile, generate_update_patches
import pandas as pd


def generate_update_patch_file(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    patch_comment: str,
    trace: bool = False,
    new_file_name: str = None,
    outpath: Path = Path("./"),
    id_columns: list = ["hhid", "redcap_event_name"],
) -> Path:
    try:
        patches = generate_update_patches(old_df, new_df, id_columns)
    except Exception as e:
        print(f"Error generating patches: {e}")
        if trace:
            traceback.print_exc()
        exit(1)

    patch_file = PatchFile(
        creations=[],
        deletions=[],
        patches=patches,
        version=1,
        meta={
            "notes": patch_comment,
        },
    )

    # Create directories if they dont exist for outpath:
    outpath.mkdir(parents=True, exist_ok=True)

    out_file_path = outpath / f"{new_file_name}.json"

    with open(out_file_path.absolute(), "w") as file_ptr:
        file_ptr.write(patch_file.get_json())

    return out_file_path
