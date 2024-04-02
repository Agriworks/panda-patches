from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd

from panda_patch.patch_interface import PatchInterface


@dataclass
class CreatePatch(PatchInterface):
    target: Dict[str, Any]
    payload: Dict[str, Any]

    def apply(self, df: pd.DataFrame) -> None:
        raise NotImplementedError("Need to finish the implementation")
        # First, check to ensure that the row doesn't already exist
        match_mask = pd.Series(True, index=df.index)
        for key, value in self.target.items():
            match_mask &= df[key] == value

        if match_mask.any():
            raise ValueError("Row with target values already exists, cannot create.")

        # Second, insert a new row with the payload data
        new_row = self.target.copy()
        new_row.update(self.payload)
        df = df.append(new_row, ignore_index=True)
