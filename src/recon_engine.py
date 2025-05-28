import dask.dataframe as dd
import pandas as pd

class ReconciliationEngine:
    def __init__(
        self,
        match_on: list = None,
        internal_key: str = "amount",
        external_key: str = "net_amount",
        amount_tolerance: float = 0.0,
        date_tolerance_days: int = 0
    ):
        self.match_on = match_on or ["date"]
        self.internal_key = internal_key
        self.external_key = external_key
        self.amount_tolerance = amount_tolerance
        self.date_tolerance_days = date_tolerance_days

    def reconcile(self, internal_df: dd.DataFrame, external_df: dd.DataFrame):
        # Join only on explicit match fields (not amount or date)
        join_keys = [(k, k) for k in self.match_on]
        internal_join_cols = [i for i, _ in join_keys]
        external_join_cols = [e for _, e in join_keys]

        merged = internal_df.merge(
            external_df,
            how="outer",
            left_on=internal_join_cols,
            right_on=external_join_cols,
            suffixes=("_internal", "_external"),
            indicator=True
        )

        # Apply amount tolerance if both keys exist
        if self.amount_tolerance > 0.0 and self.internal_key in merged.columns and self.external_key in merged.columns:
            merged = merged[
                (merged[self.internal_key] - merged[self.external_key]).abs() <= self.amount_tolerance
            ]

        # Apply date tolerance if both date columns exist
        if self.date_tolerance_days > 0:
            date_cols = [col for col in merged.columns if "date" in col]
            date_internal_col = next((c for c in date_cols if c.endswith("_internal")), None)
            date_external_col = next((c for c in date_cols if c.endswith("_external")), None)
            if date_internal_col and date_external_col:
                merged = merged[
                    (dd.to_datetime(merged[date_internal_col]) - dd.to_datetime(merged[date_external_col])).dt.days.abs()
                    <= self.date_tolerance_days
                ]

        matched = merged[merged["_merge"] == "both"]
        unmatched_internal = merged[merged["_merge"] == "left_only"]
        unmatched_external = merged[merged["_merge"] == "right_only"]

        return matched, unmatched_internal, unmatched_external
