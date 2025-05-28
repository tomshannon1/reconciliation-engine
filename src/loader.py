import pandas as pd
import os

class DataLoader:
    def __init__(self, internal_path: str, external_path: str):
        self.internal_path = internal_path
        self.external_path = external_path

    def load_internal(self) -> pd.DataFrame:
        if not os.path.exists(self.internal_path):
            raise FileNotFoundError(f"Internal data file not found: {self.internal_path}")
        df = pd.read_csv(self.internal_path, parse_dates=["date"])
        df["amount"] = df["amount"].astype(float)
        print(f"[INFO] Loaded {len(df)} internal transactions.")
        return df

    def load_external(self) -> pd.DataFrame:
        if not os.path.exists(self.external_path):
            raise FileNotFoundError(f"External data file not found: {self.external_path}")
        df = pd.read_csv(self.external_path, parse_dates=["date"])
        df["gross_amount"] = df["gross_amount"].astype(float)
        df["fee"] = df["fee"].astype(float)
        df["net_amount"] = df["net_amount"].astype(float)
        print(f"[INFO] Loaded {len(df)} external transactions.")
        return df
