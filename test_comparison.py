# Test script to compare outputs
import csv
import os

def compare_outputs(custom_file, user_file):
    print('=== OUTPUT COMPARISON ===')
    
    # Check if files exist
    if not os.path.exists(custom_file):
        print(f'Custom file not found: {custom_file}')
        return
    if not os.path.exists(user_file):
        print(f'User file not found: {user_file}')
        return
    
    # Read custom output
    with open(custom_file, 'r') as f:
        custom_reader = csv.reader(f, delimiter='|')
        custom_header = next(custom_reader)
        custom_rows = [row for row in custom_reader]
    
    # Read user output  
    with open(user_file, 'r') as f:
        user_reader = csv.reader(f, delimiter='|')
        user_header = next(user_reader)
        user_rows = [row for row in user_reader]
    
    print(f'Custom: {len(custom_rows)} rows')
    print(f'User: {len(user_rows)} rows')
    
    # Compare headers
    print(f'Custom header: {custom_header}')
    print(f'User header: {user_header}')
    
    # Compare first few rows
    print('First 3 rows comparison:')
    for i in range(min(3, len(custom_rows), len(user_rows))):
        print(f'Custom: {custom_rows[i]}')
        print(f'User: {user_rows[i]}')
        print()

# Usage:
# compare_outputs(
#     'output_human/gwas/downstream/edges_snp_downstream_gene_snp_gene.csv',
#     'output_user_data/gwas_catalog_user/gwas/downstream/edges_snp_downstream_gene_snp_gene.csv'
# )

