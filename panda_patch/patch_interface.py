import pandas as pd


class PatchInterface:
    def apply(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()
