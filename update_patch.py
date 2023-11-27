from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd

from pipeline.patch.patch_interface import PatchInterface
from deepdiff import DeepDiff


@dataclass
class UpdatePatch(PatchInterface):
    """
    Basic Patch object that stores the changes
    """

    target: Dict[str, Any]
    deltas: Dict[str, Any]

    @staticmethod
    def parse_patch_object_json_dict(patch_object_json_dict: Dict) -> UpdatePatch:
        ret = UpdatePatch(
            target=patch_object_json_dict["target"],
            deltas=patch_object_json_dict["deltas"],
        )

        return ret

    def apply(self, df: pd.DataFrame) -> None:
        # Select the correct row using the list of target keys and values in target
        # Fix all the column values given by deltas

        # Find the row(s) in the DataFrame that match the target keys and values
        match_mask = pd.Series(True, index=df.index)
        for key, value in self.target.items():
            match_mask &= df[key] == value

        # Apply the deltas to the matching rows
        for index, _ in df[match_mask].iterrows():
            for column, delta in self.deltas.items():
                df.at[index, column] = delta

    def __eq__(self, __value: object) -> bool:
        # Check if the object is instance of an update patch
        if isinstance(__value, UpdatePatch):
            # Check if everything matches in terms of deltas and targets
            diff = DeepDiff(self.target, __value.target)
            if diff != {}:
                return False
            diff = DeepDiff(self.deltas, __value.deltas)
            if diff != {}:
                return False

            return True

        else:
            return False
