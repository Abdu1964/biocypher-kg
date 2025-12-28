# Synonyms Fix Summary

## Issues Fixed

### 1. ✅ Fixed `gene_alias_file_path`
**File**: `config/user_adapters_gene.yaml`
- Changed from: `./samples/your_gene_alias_file.tsv` (doesn't exist)
- Changed to: `./aux_files/Homo_sapiens.gene_info.gz` (correct file)

### 2. ✅ Updated Alias Dictionary Loading
**File**: `biocypher_metta/adapters/user_data_adapter.py`
- Now extracts Ensembl IDs from dbxrefs (like GencodeGeneAdapter)
- Uses Ensembl IDs as primary keys in alias dict
- Also includes HGNC IDs as keys

### 3. ✅ Enhanced Synonyms Lookup
**File**: `biocypher_metta/adapters/user_data_adapter.py`
- Uses raw Ensembl ID (without version) for lookup
- Falls back to HGNC ID lookup if Ensembl lookup fails
- Matches GencodeGeneAdapter's lookup logic

### 4. ✅ Removed Synonyms from Defaults
**File**: `config/user_data_mapping_gencode_gene_pipe.yaml`
- Removed `synonyms: ""` from defaults
- Synonyms will now be populated by adapter from alias file

### 5. ✅ Added ID Version Stripping
**File**: `config/user_data_mapping_gencode_gene_pipe.yaml`
- Added `format_curie: true`
- Added `strip_version: true`
- IDs will now be `ENSG00000101349` instead of `ENSG00000101349.17`

## Expected Output After Regeneration

The output should now match the custom adapter:
- ✅ Column order: `chr|end|gene_name|gene_type|id|source|source_url|start|synonyms`
- ✅ ID format: `ENSG00000101349` (no version, no prefix)
- ✅ Synonyms: JSON array with synonyms OR empty string
- ✅ Source: `GENCODE`
- ✅ Source URL: `https://www.gencodegenes.org/human/`

## Example Output

**Before**:
```
chr20|9839076|PAK5|protein_coding|ENSG00000101349.17|GENCODE|https://www.gencodegenes.org/human/|9537370|[]
```

**After** (expected):
```
chr20|9839076|PAK5|protein_coding|ENSG00000101349|GENCODE|https://www.gencodegenes.org/human/|9537370|"[""PAK5"", ""PAK7"", ""p21-activated kinase 7"", ...]"
```

## Next Steps

1. **Regenerate the output**:
   ```bash
   cd /home/eyorica/Downloads/bocypher/biocypher-kg
   uv run python create_knowledge_graph.py \
       --dbsnp-rsids aux_files/sample_dbsnp_rsids.pkl \
       --dbsnp-pos aux_files/sample_dbsnp_pos.pkl \
       --adapters-config config/user_adapters_gene.yaml \
       --output-dir output_user_data/my_custom_data_gene \
       --writer-type neo4j \
       --write-properties \
       --add-provenance \
       --species hsa
   ```

2. **Verify**:
   - Check that IDs don't have version numbers
   - Check that synonyms are populated (not empty `[]`)
   - Compare with custom adapter output

