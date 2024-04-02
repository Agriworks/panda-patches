from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd

from panda_patch.patch_interface import PatchInterface


@dataclass
class DeletePatch(PatchInterface):
    target: Dict[str, Any]

    def apply(self, df: pd.DataFrame) -> None:
        # Find the row(s) in the DataFrame that match the target keys and values
        match_mask = pd.Series(True, index=df.index)
        for key, value in self.target.items():
            match_mask &= df[key] == value

        # Delete the rows(s)
        df.drop(df[match_mask].index, inplace=True)
