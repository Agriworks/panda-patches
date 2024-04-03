from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd

from panda_patch.update_patch import UpdatePatch


@dataclass
class PatchFile:
    """
    Container for the patch file that stores all the patches
    """

    patches: List[UpdatePatch]
    meta: Dict[str, Any]
    version: int
    deletions: Dict[str, Any]  # TODO: Wrap Up Implementation
    creations: Dict[str, Any]  # TODO: Wrap Up Implementation

    @staticmethod
    def parse_patch_file_json_dict(json_dict: Dict) -> PatchFile:
        if json_dict["version"] != 1:
            raise Exception("Did not find version=1 for the patch file format")

        ret = PatchFile(
            patches=[
                UpdatePatch.parse_patch_object_json_dict(patch_object)
                for patch_object in json_dict["patches"]
            ],
            version=json_dict["version"],
            creations=[],
            deletions=[],
            meta=json_dict["meta"],
        )

        return ret

    @staticmethod
    def parse_patch_file_from_path(path: Path) -> PatchFile:
        with open(path, "r") as file_ptr:
            patch_json = json.load(file_ptr)
            patch_file = PatchFile.parse_patch_file_json_dict(patch_json)
            return patch_file

    def get_json(self) -> str:
        """Gets the JSON of the patch file"""
        ret = {
            "patches": [patch.__dict__ for patch in self.patches],
            "version": self.version,
            "meta": self.meta,
        }
        return json.dumps(ret, indent=4)

    def apply_update_patches(self, df: pd.DataFrame):
        for patch in self.patches:
            patch.apply(df)

    def apply(self):
        raise NotImplementedError("Need to test and changes for all types of patches")

    def __eq__(self, __value: object) -> bool:
        # First check if the value is also a patchfile
        if not isinstance(__value, PatchFile):
            return False
        # Check if the length of the patches match
        if len(self.patches) != len(__value.patches):
            return False
        # Now go through each of the update patches in the new patch file and see if they are present in current patch file
        for patch in __value.patches:
            if patch not in self.patches:
                return False

        return True


def generate_update_patches(
    old_df: pd.DataFrame, new_df: pd.DataFrame, id_columns: List[str]
) -> List[UpdatePatch]:
    """
    Generate patches representing changes between two DataFrames.

    Parameters:
    - source_df (pd.DataFrame): The source DataFrame.
    - target_df (pd.DataFrame): The target DataFrame.

    Returns:
    List of Patch objects representing the changes between the DataFrames.
    """

    # Check if the DataFrames have the same columns
    if not set(new_df.columns).issubset(set(old_df.columns)):
        # Print out the missing columns
        missing_columns = set(new_df.columns) - set(old_df.columns)
        raise ValueError(
            "Source and target DataFrames must have the same columns", missing_columns
        )

    patches = []

    for _, new_row in new_df.iterrows():
        query = " & ".join(
            [
                f"{col} == '{new_row[col]}'"
                if isinstance(new_row[col], str)
                else f"{col} == {new_row[col]}"
                for col in id_columns
            ]
        )
        old_row_query_result = old_df.query(query)
        try:
            old_row = old_row_query_result.iloc[0]
        except IndexError:
            # Print the query and the result
            print("Error: Could not find a matching row in the old DataFrame")
            print(f"Query: {query}")
            print(f"Result: {old_row_query_result}")
            raise

        deltas = {}
        for column in new_df.columns:
            # Skip the ID column
            if column in id_columns:
                continue

            if pd.isna(new_row[column]) and pd.isna(old_row[column]):
                continue

            if new_row[column] != old_row[column]:
                deltas[column] = (
                    new_row[column] if not pd.isna(new_row[column]) else None
                )

        if deltas:
            patch = UpdatePatch(
                target={col: new_row[col] for col in id_columns}, deltas=deltas
            )
            patches.append(patch)

    return patches


def apply_patches(df: pd.DataFrame, patches: List[UpdatePatch]) -> pd.DataFrame:
    # Make a Copy
    temp_df = df.copy()

    # Go through each of the patches
    for patch in patches:
        patch.apply(temp_df)

    return temp_df
