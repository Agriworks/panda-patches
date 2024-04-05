from pathlib import Path
import traceback

import click
import pandas as pd

from panda_patches.patchfile import PatchFile, generate_update_patches


@click.command()
@click.argument(
    "old_csv",
    nargs=1,
    required=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.argument(
    "new_csv",
    nargs=1,
    required=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--outpath",
    "-o",
    default=".",
    help="This is the output path",
    type=click.Path(exists=False, path_type=Path),
)
@click.option(
    "--trace",
    "-t",
    default=False,
    help="Enable traceback printing",
    type=bool,
)
@click.option(
    "--message" "-m",
    help="The message to be included in the patch file",
    type=str,
)
def generate_patch_from_csv(
    old_csv: Path,
    new_csv: Path,
    outpath: Path = Path("./patch.json"),
    trace: bool = False,
    message: str = "Please update the notes field with a description of the patch",
):
    def get_separator(file_path: Path):
        if file_path.suffix == ".csv":
            return ","
        elif file_path.suffix == ".tsv":
            return "\t"
        else:
            raise ValueError(
                f"Unknown file format, only supports csv / tsv extensions, found: {file_path.suffix}"
            )

    try:
        old_df = pd.read_csv(old_csv.absolute(), sep=get_separator(old_csv))

        new_df = pd.read_csv(new_csv.absolute(), sep=get_separator(new_csv))
    except Exception as e:
        print(
            f"Error reading input files, please check that they are valid csv/tsv files: {e}"
        )
        exit(1)

    try:
        patches = generate_update_patches(old_df, new_df, ["hhid", "redcap_event_name"])
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
            "notes": message,
        },
    )

    # Create directories if they dont exist for outpath:
    outpath.mkdir(parents=True, exist_ok=True)

    out_file_path = outpath / f"{new_csv.stem}.json"

    with open(out_file_path.absolute(), "w") as file_ptr:
        file_ptr.write(patch_file.get_json())




