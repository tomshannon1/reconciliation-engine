"""
Generate test data for reconciliation engine testing.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_test_data(
    num_records=1000,
    match_rate=0.85,
    amount_variance_rate=0.10,
    date_variance_rate=0.05,
    id_variance_rate=0.05,
    output_dir="data"
):
    """
    Generate test data for reconciliation testing with controlled variance rates.
    
    Args:
        num_records (int): Number of records to generate
        match_rate (float): Percentage of records that should match (0.0-1.0)
        amount_variance_rate (float): Percentage of matching records that should have amount variances
        date_variance_rate (float): Percentage of matching records that should have date variances
        id_variance_rate (float): Percentage of matching records that should have ID variances
        output_dir (str): Directory to save the generated CSV files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate base data
    base_dates = [datetime.now() - timedelta(days=np.random.randint(0, 30)) for _ in range(num_records)]
    base_data = {
        'date': [d.strftime('%Y-%m-%d') for d in base_dates],
        'customer_id': [f'C{str(i+1).zfill(6)}' for i in range(num_records)],
        'location_id': [f'L{str(np.random.randint(1, 100)).zfill(4)}' for _ in range(num_records)],
        'amount': np.random.uniform(10.0, 1000.0, num_records).round(2)
    }
    
    # Create internal transactions
    internal_df = pd.DataFrame(base_data)
    internal_df['transaction_id'] = [f'INT{str(i+1).zfill(8)}' for i in range(num_records)]
    
    # Create external transactions starting with copies of matching records
    num_matching = int(num_records * match_rate)
    num_non_matching = num_records - num_matching
    
    # Start with matching records
    external_matching = internal_df.head(num_matching).copy()
    
    # Apply variances to matching records
    num_amount_variances = int(num_matching * amount_variance_rate)
    num_date_variances = int(num_matching * date_variance_rate)
    num_id_variances = int(num_matching * id_variance_rate)
    
    # Amount variances
    amount_variance_indices = np.random.choice(num_matching, num_amount_variances, replace=False)
    external_matching.loc[amount_variance_indices, 'amount'] = (
        external_matching.loc[amount_variance_indices, 'amount'] * 
        np.random.uniform(0.95, 1.05, num_amount_variances)
    ).round(2)
    
    # Date variances
    date_variance_indices = np.random.choice(num_matching, num_date_variances, replace=False)
    varied_dates = pd.to_datetime(external_matching.loc[date_variance_indices, 'date']) + \
                  pd.to_timedelta(np.random.choice([-1, 1], num_date_variances), unit='D')
    external_matching.loc[date_variance_indices, 'date'] = varied_dates.dt.strftime('%Y-%m-%d')
    
    # ID variances (these will cause non-matches)
    id_variance_indices = np.random.choice(num_matching, num_id_variances, replace=False)
    external_matching.loc[id_variance_indices, 'customer_id'] = [
        f'C{str(np.random.randint(900000, 999999)).zfill(6)}'
        for _ in range(num_id_variances)
    ]
    
    # Generate non-matching records
    non_matching_dates = [datetime.now() - timedelta(days=np.random.randint(0, 30)) for _ in range(num_non_matching)]
    external_non_matching = pd.DataFrame({
        'date': [d.strftime('%Y-%m-%d') for d in non_matching_dates],
        'customer_id': [
            f'C{str(np.random.randint(900000, 999999)).zfill(6)}'
            for _ in range(num_non_matching)
        ],
        'location_id': [
            f'L{str(np.random.randint(1, 100)).zfill(4)}'
            for _ in range(num_non_matching)
        ],
        'amount': np.random.uniform(10.0, 1000.0, num_non_matching).round(2)
    })
    
    # Combine matching and non-matching records
    external_df = pd.concat([external_matching, external_non_matching], ignore_index=True)
    external_df['transaction_id'] = [f'EXT{str(i+1).zfill(8)}' for i in range(len(external_df))]
    
    # Add external-specific fields
    external_df['fee'] = (external_df['amount'] * 0.03).round(2)
    external_df['gross_amount'] = external_df['amount']
    external_df['net_amount'] = (external_df['gross_amount'] - external_df['fee']).round(2)
    
    # Shuffle the external DataFrame to mix matching and non-matching records
    external_df = external_df.sample(frac=1).reset_index(drop=True)
    
    # Save to CSV files
    internal_df.to_csv(os.path.join(output_dir, 'internal_transactions.csv'), index=False)
    external_df.to_csv(os.path.join(output_dir, 'external_transactions.csv'), index=False)
    
    return internal_df, external_df

if __name__ == '__main__':
    # Example usage
    internal_df, external_df = generate_test_data(
        num_records=1000,
        match_rate=0.85,
        amount_variance_rate=0.10,
        date_variance_rate=0.05,
        id_variance_rate=0.05
    )
