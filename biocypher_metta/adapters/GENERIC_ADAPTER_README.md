# Generic Data Adapter

The `GenericDataAdapter` (also available as `UserDataAdapter` for backward compatibility) is a flexible, configuration-driven adapter that allows you to import your own data into BioCypher knowledge graphs without writing custom code.

## Features

- **Multiple File Formats**: Supports CSV, TSV, JSON (list and JSONL), and GTF/GFF formats
- **Compressed Files**: Automatically handles `.gz` compressed files
- **Flexible Mapping**: Use YAML or JSON configuration files to define data transformations
- **Data Transformations**: Column renaming, type coercion, filtering, and custom transformations
- **Node & Edge Support**: Import both nodes and edges from the same or different files
- **Provenance Tracking**: Automatic source, version, and URL tracking
- **Gene Aliases**: Optional support for gene synonym/alias mapping
- **CURIE ID Formatting**: Auto-detects and formats IDs like other adapters (ENSG→ENSEMBL, rs→DBSNP, etc.)
- **Genomic Location Filtering**: Filter by chromosome and position ranges (like GencodeAdapter, DBSNPAdapter)
- **Helper Functions**: Uses `to_float()` and `check_genomic_location()` helpers like other adapters
- **ID Version Stripping**: Automatically removes version numbers from IDs (e.g., ENSG00000123456.1 → ENSG00000123456)

## Quick Start

### 1. Prepare Your Data File

Your data can be in CSV, TSV, JSON, or GTF format. Example CSV:

```csv
gene_id,gene_name,score,interaction_partner
ENSG00000123456,GENE1,0.95,ENSG00000765432
ENSG00000765432,GENE2,0.87,ENSG00000123456
```

### 2. Create a Mapping Configuration

Create a YAML file (e.g., `my_data_mapping.yaml`) that describes how to transform your data:

```yaml
columns:
  rename:
    interaction_partner: partner_gene
  coerce:
    score: float
  defaults:
    source: "MyDataset"
    version: "1.0"

nodes:
  id_column: gene_id
  label_constant: gene
  properties:
    - gene_name
    - score

edges:
  source_column: gene_id
  target_column: partner_gene
  label_constant: interacts_with
  properties:
    - score
```

### 3. Configure the Adapter

Add to your `adapters_config.yaml`:

```yaml
my_custom_data:
  adapter:
    module: biocypher_metta.adapters.user_data_adapter
    cls: GenericDataAdapter  # or UserDataAdapter
    args:
      filepath: ./data/my_data.csv
      data_format: csv
      mapping_config: ./config/my_data_mapping.yaml
      source: "MyDataset"
      version: "1.0"
      source_url: "https://example.com/mydata"
      write_properties: true
      add_provenance: true
  nodes: true
  edges: true
  outdir: my_data
```

## Mapping Configuration Reference

### Column Operations

#### Rename Columns
```yaml
columns:
  rename:
    old_column_name: new_column_name
    "column with spaces": column_without_spaces
```

#### Type Coercion
```yaml
columns:
  coerce:
    score: float
    count: int
    is_active: bool
```

Supported types: `int`, `float`, `bool`

#### Default Values
```yaml
columns:
  defaults:
    source: "MyDataset"
    status: "active"
```

### Node Configuration

```yaml
nodes:
  id_column: gene_id              # Column containing node ID
  label_column: node_type          # Column containing node type (optional)
  label_constant: gene             # Fixed node type (alternative to label_column)
  
  # CURIE formatting (like other adapters)
  format_curie: true               # Auto-format IDs as CURIE (default: true)
  id_prefix: ENSEMBL               # Explicit prefix override (optional, auto-detected)
  strip_version: true              # Remove version numbers (default: true)
  
  # Genomic location (for filtering and properties)
  chr_column: chr                  # Chromosome column
  start_column: start              # Start position column
  end_column: end                  # End position column
  
  properties:                      # List of property columns to include
    - gene_name
    - score
    - description
```

**Note**: Use either `label_column` OR `label_constant`, not both.

**CURIE Auto-Detection**: The adapter automatically detects common ID patterns:
- `ENSG*`, `ENST*`, `ENSP*` → `ENSEMBL:`
- `rs*` → `DBSNP:`
- UniProtKB patterns → `UniProtKB:`
- `UBERON:*` → `UBERON:`
- `R-*` → `REACT:`
- And more...

### Edge Configuration

```yaml
edges:
  source_column: gene1             # Column containing source node ID
  target_column: gene2             # Column containing target node ID
  label_column: edge_type          # Column containing edge type (optional)
  label_constant: interacts_with   # Fixed edge type (alternative to label_column)
  
  # CURIE formatting (like other adapters)
  format_curie: true                # Auto-format IDs as CURIE (default: true)
  source_prefix: ENSEMBL            # Explicit prefix for source IDs (optional)
  target_prefix: ENSEMBL            # Explicit prefix for target IDs (optional)
  strip_version: true               # Remove version numbers (default: true)
  
  source_type_column: source_type  # Optional: specify source node type
  target_type_column: target_type   # Optional: specify target node type
  properties:                      # List of property columns to include
    - score
    - confidence
    - p_value
```

### Filters

Filter rows before processing:

```yaml
filters:
  - field: status
    op: equals
    value: active
  - field: score
    op: gt
    value: 0.5
  - field: category
    op: in
    value: [A, B, C]
```

Supported operations: `equals`, `in`, `gt` (greater than), `lt` (less than)

### Transforms

Apply transformations to data:

#### Split
Split a column into a list:
```yaml
transforms:
  - op: split
    field: tags
    delimiter: ","
```

#### Explode
Create multiple rows from a list column:
```yaml
transforms:
  - op: explode
    field: partners
```

#### Concat
Concatenate multiple columns:
```yaml
transforms:
  - op: concat
    field: full_name
    parts: [first_name, last_name]
    sep: " "
```

#### Map
Map values using a dictionary:
```yaml
transforms:
  - op: map
    field: status_code
    mapping:
      "1": "active"
      "2": "inactive"
      "3": "pending"
```

#### Format
Format a value using a template:
```yaml
transforms:
  - op: format
    field: formatted_id
    source: gene_id
    template: "GENE_{value}"
```

## Complete Example

### Input CSV (`data.csv`)
```csv
gene1,gene2,interaction_score,interaction_type,publication
ENSG00000123456,ENSG00000765432,0.95,physical,PMID:12345678
ENSG00000765432,ENSG00000987654,0.87,genetic,PMID:87654321
```

### Mapping (`mapping.yaml`)
```yaml
columns:
  rename:
    interaction_score: score
    interaction_type: type
  coerce:
    score: float
  defaults:
    source: "PPI_Database"
    version: "2.0"

nodes:
  id_column: gene1
  label_constant: gene
  properties:
    - gene1

edges:
  source_column: gene1
  target_column: gene2
  label_constant: interacts_with
  properties:
    - score
    - type
    - publication

filters:
  - field: score
    op: gt
    value: 0.8
```

### Adapter Config (`adapters_config.yaml`)
```yaml
ppi_data:
  adapter:
    module: biocypher_metta.adapters.user_data_adapter
    cls: GenericDataAdapter
    args:
      filepath: ./data.csv
      data_format: csv
      mapping_config: ./mapping.yaml
      source: "PPI_Database"
      version: "2.0"
      source_url: "https://example.com/ppi"
      # Optional: Genomic location filtering (like other adapters)
      # chr: "1"
      # start: 1000000
      # end: 2000000
  nodes: true
  edges: true
  outdir: ppi_data
```

## Advanced Features

### Gene Alias Support

If you're working with gene data and want to include synonyms:

```yaml
args:
  gene_alias_file_path: ./data/gene_info.tsv.gz
```

The adapter will automatically map gene IDs to their synonyms from the gene info file.

### Multiple Node Types

You can define multiple node types in a single mapping:

```yaml
nodes:
  gene:
    id_column: gene_id
    label_constant: gene
    properties: [gene_name]
  variant:
    id_column: variant_id
    label_constant: variant
    properties: [chr, pos, ref, alt]
```

### JSON Format Support

For JSON files, the adapter supports both:
- **JSON Array**: `[{...}, {...}]`
- **JSON Lines**: One JSON object per line

### GTF/GFF Format

For GTF/GFF files, attributes are automatically parsed:

```yaml
nodes:
  id_column: gene_id
  label_constant: gene
  properties:
    - gene_name
    - gene_type
    - chrom
    - start
    - end
```

## Troubleshooting

### Common Issues

1. **File not found**: Ensure the `filepath` is correct and relative to where you run the script
2. **Column not found**: Check that column names in mapping match exactly (case-sensitive)
3. **Type conversion errors**: Verify data types match what's expected in `coerce`
4. **Empty results**: Check filters aren't too restrictive

### Debugging Tips

- Set `write_properties: true` to see all properties in output
- Check logs for warnings about skipped records
- Verify mapping file syntax with a YAML validator
- Test with a small sample file first

## API Reference

### Constructor Parameters

- `filepath` (str, required): Path to input data file
- `data_format` (str, default='csv'): Format ('csv', 'tsv', 'json', 'gtf')
- `mapping_config` (str, optional): Path to mapping configuration file
- `gene_alias_file_path` (str, optional): Path to gene alias file
- `source` (str, optional): Source name for provenance (default: "UserData")
- `version` (str, optional): Version string (default: "1.0")
- `source_url` (str, optional): Source URL for provenance
- `write_properties` (bool, default=True): Whether to write properties
- `add_provenance` (bool, default=True): Whether to add provenance metadata

### Methods

- `get_nodes()`: Generator yielding `(node_id, label, properties)` tuples
- `get_edges()`: Generator yielding `(source_id, target_id, label, properties)` tuples

## Examples Repository

See `config/user_data_mapping_*.yaml` files for more examples:
- `user_data_mapping.yaml` - Basic example
- `user_data_mapping_gene.yaml` - Gene data example
- `user_data_mapping_cadd.yaml` - CADD scores example

## Support

For issues or questions, please check:
1. Existing mapping examples in `config/`
2. Sample data files in `samples/`
3. The main BioCypher documentation

