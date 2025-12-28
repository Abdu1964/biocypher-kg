# Manual verification of GWAS mapping
import csv
import yaml

# Load mapping
with open('config/user_data_mapping_gwas_catalog_downstream.yaml', 'r') as f:
    mapping = yaml.safe_load(f)

print('Mapping verification:')
print('- Source column:', mapping['edges']['source_column'])
print('- Target column:', mapping['edges']['target_column'])
print('- Properties:', mapping['edges']['properties'])

# Check transforms
transforms = mapping.get('transforms', [])
for t in transforms:
    if t['field'] in ['source_id', 'target_id']:
        print(f'- {t["field"]} transform: {t["source"]} -> {t["field"]}')

# Check sample data extraction
print('\nSample data extraction test:')
with open('samples/gwas-catalog_sample.tsv', 'r') as f:
    reader = csv.reader(f, delimiter='\t')
    header = next(reader)
    
    # Find a row with DOWNSTREAM_GENE_ID
    for row in reader:
        if len(row) > 16 and row[16].strip():
            snp = row[21]  # SNPS column
            gene = row[16]  # DOWNSTREAM_GENE_ID column  
            pval = row[27] if len(row) > 27 else 'N/A'
            
            print(f'SNP: {snp}')
            print(f'Gene: {gene}')
            print(f'P-value: {pval}')
            
            # Apply transforms
            source_id = snp  # rsid -> source_id
            target_id = gene  # DOWNSTREAM_GENE_ID -> target_id
            
            print(f'Expected output: {source_id} -> {target_id}')
            break

