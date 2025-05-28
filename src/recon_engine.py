import dask.dataframe as dd

class ReconciliationEngine:
    def __init__(self, match_on: list = None, internal_key: str = "amount", external_key: str = "net_amount"):
        self.match_on = match_on or ["date"]
        self.internal_key = internal_key
        self.external_key = external_key

    def reconcile(self, internal_df: dd.DataFrame, external_df: dd.DataFrame):
        # Define matching keys for join
        join_keys = [(self.internal_key, self.external_key)] + [(k, k) for k in self.match_on if k not in [self.internal_key, self.external_key]]
        internal_join_cols = [i for i, _ in join_keys]
        external_join_cols = [e for _, e in join_keys]

        # Perform outer join using Dask
        merged = internal_df.merge(
            external_df,
            how="outer",
            left_on=internal_join_cols,
            right_on=external_join_cols,
            suffixes=("_internal", "_external"),
            indicator=True
        )

        matched = merged[merged["_merge"] == "both"]
        unmatched_internal = merged[merged["_merge"] == "left_only"]
        unmatched_external = merged[merged["_merge"] == "right_only"]

        return matched, unmatched_internal, unmatched_external
