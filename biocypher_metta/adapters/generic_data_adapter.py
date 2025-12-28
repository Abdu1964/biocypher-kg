# Author: Generic Data Adapter for BioCypher
# A flexible, configuration-driven adapter for importing custom data

import csv
import json
import gzip
import re
from typing import Dict, List, Optional, Any, Generator, Tuple
from pathlib import Path
import yaml

from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location, to_float
from biocypher._logger import logger


class GenericDataAdapter(Adapter):
    """
    A flexible, configuration-driven adapter that allows importing custom data
    without writing custom code. Supports multiple file formats and transformations.
    
    See GENERIC_ADAPTER_README.md for full documentation.
    """
    
    # Common ID prefixes for CURIE formatting
    ID_PREFIXES = {
        'ENSG': 'ENSEMBL',
        'ENST': 'ENSEMBL',
        'ENSP': 'ENSEMBL',
        'ENSR': 'ENSEMBL',
        'ENSE': 'ENSEMBL',
        'rs': 'DBSNP',
        'UP': 'UniProtKB',
        'UBERON': 'UBERON',
        'GO': 'GO',
        'R-': 'REACT',
        'FB': 'FLYBASE',
        'DOID': 'DOID',
        'HP': 'HP',
        'CHEBI': 'CHEBI',
        'SO': 'SO',
        'CL': 'CL',
        'EFO': 'EFO',
        'MI': 'MI',
    }
    
    def __init__(
        self,
        filepath: str,
        data_format: str = 'csv',
        mapping_config: Optional[str] = None,
        gene_alias_file_path: Optional[str] = None,
        source: str = "UserData",
        version: str = "1.0",
        source_url: Optional[str] = None,
        write_properties: bool = True,
        add_provenance: bool = True,
        chr: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ):
        """
        Initialize the GenericDataAdapter.
        
        Args:
            filepath: Path to the data file
            data_format: Format of the file ('csv', 'tsv', 'json', 'gtf')
            mapping_config: Path to YAML/JSON mapping configuration
            gene_alias_file_path: Optional path to gene alias file
            source: Source name for provenance
            version: Version string
            source_url: URL of the data source
            write_properties: Whether to include properties in output
            add_provenance: Whether to add provenance metadata
            chr: Chromosome filter (optional)
            start: Start position filter (optional)
            end: End position filter (optional)
        """
        super(GenericDataAdapter, self).__init__(write_properties, add_provenance)
        
        self.filepath = filepath
        self.data_format = data_format.lower()
        self.source = source
        self.version = version
        self.source_url = source_url
        self.chr = chr
        self.start = start
        self.end = end
        
        # Load mapping configuration
        self.mapping = self._load_mapping_config(mapping_config) if mapping_config else {}
        
        # Load gene aliases if provided
        self.gene_aliases = self._load_gene_aliases(gene_alias_file_path) if gene_alias_file_path else {}
        
        # Parse mapping configuration
        self.column_config = self.mapping.get('columns', {})
        self.node_config = self.mapping.get('nodes', {})
        self.edge_config = self.mapping.get('edges', {})
        self.filters = self.mapping.get('filters', [])
        self.transforms = self.mapping.get('transforms', [])
    
    def _load_mapping_config(self, config_path: str) -> Dict:
        """Load YAML or JSON mapping configuration."""
        try:
            with open(config_path, 'r') as f:
                if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                    return yaml.safe_load(f) or {}
                elif config_path.endswith('.json'):
                    return json.load(f)
                else:
                    logger.warning(f"Unknown config format: {config_path}, attempting YAML")
                    return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Failed to load mapping config: {e}")
            return {}
    
    def _load_gene_aliases(self, alias_file_path: str) -> Dict:
        """Load gene aliases from gene_info file."""
        aliases = {}
        try:
            open_fn = gzip.open if alias_file_path.endswith('.gz') else open
            with open_fn(alias_file_path, 'rt') as f:
                reader = csv.reader(f, delimiter='\t')
                next(reader)  # Skip header
                for row in reader:
                    if len(row) > 4:
                        gene_id = row[1]
                        synonyms = row[4].split('|') if row[4] != '-' else []
                        if synonyms:
                            aliases[gene_id] = synonyms
        except Exception as e:
            logger.warning(f"Failed to load gene aliases: {e}")
        return aliases
    
    def _read_data(self) -> Generator[Dict, None, None]:
        """Read data from file based on format."""
        open_fn = gzip.open if self.filepath.endswith('.gz') else open
        
        try:
            if self.data_format in ['csv', 'tsv']:
                delimiter = ',' if self.data_format == 'csv' else '\t'
                with open_fn(self.filepath, 'rt') as f:
                    reader = csv.DictReader(f, delimiter=delimiter)
                    for row in reader:
                        yield row
            
            elif self.data_format == 'json':
                with open_fn(self.filepath, 'rt') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for item in data:
                            yield item
                    else:
                        yield data
            
            elif self.data_format == 'jsonl':
                with open_fn(self.filepath, 'rt') as f:
                    for line in f:
                        if line.strip():
                            yield json.loads(line)
            
            elif self.data_format == 'gtf':
                with open_fn(self.filepath, 'rt') as f:
                    for line in f:
                        if line.startswith('#'):
                            continue
                        row = self._parse_gtf_line(line)
                        if row:
                            yield row
            
            else:
                logger.error(f"Unsupported data format: {self.data_format}")
        
        except Exception as e:
            logger.error(f"Error reading file {self.filepath}: {e}")
    
    def _parse_gtf_line(self, line: str) -> Optional[Dict]:
        """Parse a GTF/GFF line into a dictionary."""
        try:
            parts = line.strip().split('\t')
            if len(parts) < 9:
                return None
            
            # Parse attributes
            attributes = {}
            for attr in parts[8].split(';'):
                attr = attr.strip()
                if attr:
                    key_val = attr.split(' ', 1)
                    if len(key_val) == 2:
                        key = key_val[0]
                        val = key_val[1].strip('"')
                        attributes[key] = val
            
            return {
                'seqname': parts[0],
                'source': parts[1],
                'feature': parts[2],
                'start': parts[3],
                'end': parts[4],
                'score': parts[5],
                'strand': parts[6],
                'frame': parts[7],
                **attributes
            }
        except Exception as e:
            logger.warning(f"Failed to parse GTF line: {e}")
            return None
    
    def _apply_column_transforms(self, row: Dict) -> Dict:
        """Apply column-level transformations (rename, coerce, defaults)."""
        # Apply defaults first
        defaults = self.column_config.get('defaults', {})
        for key, value in defaults.items():
            if key not in row or row[key] is None or row[key] == '':
                row[key] = value
        
        # Apply renames
        renames = self.column_config.get('rename', {})
        for old_name, new_name in renames.items():
            if old_name in row:
                row[new_name] = row.pop(old_name)
        
        # Apply type coercion
        coerce = self.column_config.get('coerce', {})
        for col, dtype in coerce.items():
            if col in row and row[col] not in [None, '', 'NA', 'N/A']:
                try:
                    if dtype == 'int':
                        row[col] = int(float(row[col]))
                    elif dtype == 'float':
                        row[col] = to_float(str(row[col]))
                    elif dtype == 'bool':
                        row[col] = str(row[col]).lower() in ['true', '1', 'yes']
                except (ValueError, TypeError) as e:
                    logger.warning(f"Type coercion failed for {col}: {e}")
        
        return row
    
    def _apply_filters(self, row: Dict) -> bool:
        """Check if row passes all filters."""
        for filter_config in self.filters:
            field = filter_config.get('field')
            op = filter_config.get('op')
            value = filter_config.get('value')
            
            if field not in row:
                return False
            
            row_value = row[field]
            
            if op == 'equals':
                if row_value != value:
                    return False
            elif op == 'in':
                if row_value not in value:
                    return False
            elif op == 'gt':
                if not (isinstance(row_value, (int, float)) and row_value > value):
                    return False
            elif op == 'lt':
                if not (isinstance(row_value, (int, float)) and row_value < value):
                    return False
        
        return True
    
    def _apply_row_transforms(self, row: Dict) -> List[Dict]:
        """Apply row-level transformations (split, explode, etc.)."""
        rows = [row]
        
        for transform in self.transforms:
            op = transform.get('op')
            
            if op == 'split':
                field = transform['field']
                delimiter = transform.get('delimiter', ',')
                for r in rows:
                    if field in r and isinstance(r[field], str):
                        r[field] = [x.strip() for x in r[field].split(delimiter)]
            
            elif op == 'explode':
                field = transform['field']
                new_rows = []
                for r in rows:
                    if field in r and isinstance(r[field], list):
                        for item in r[field]:
                            new_row = r.copy()
                            new_row[field] = item
                            new_rows.append(new_row)
                    else:
                        new_rows.append(r)
                rows = new_rows
            
            elif op == 'concat':
                field = transform['field']
                parts = transform['parts']
                sep = transform.get('sep', '')
                for r in rows:
                    values = [str(r.get(p, '')) for p in parts if p in r]
                    r[field] = sep.join(values)
            
            elif op == 'map':
                field = transform['field']
                mapping = transform['mapping']
                for r in rows:
                    if field in r and str(r[field]) in mapping:
                        r[field] = mapping[str(r[field])]
            
            elif op == 'format':
                field = transform['field']
                source = transform.get('source', field)
                template = transform['template']
                for r in rows:
                    if source in r:
                        r[field] = template.format(value=r[source])
        
        return rows
    
    def _strip_version(self, id_str: str) -> str:
        """Remove version number from IDs (e.g., ENSG00000123456.1 -> ENSG00000123456)."""
        if '.' in id_str:
            # Check if after the dot is a number (version)
            parts = id_str.rsplit('.', 1)
            if parts[1].isdigit():
                return parts[0]
        return id_str
    
    def _format_curie(self, id_str: str, explicit_prefix: Optional[str] = None, strip_version: bool = True) -> str:
        """
        Format an ID as a CURIE with appropriate prefix.
        
        Args:
            id_str: The ID string to format
            explicit_prefix: Override auto-detection with this prefix
            strip_version: Whether to strip version numbers
        
        Returns:
            Formatted CURIE string
        """
        if not id_str or id_str in ['', 'NA', 'N/A', None]:
            return id_str
        
        id_str = str(id_str)
        
        # Strip version if requested
        if strip_version:
            id_str = self._strip_version(id_str)
        
        # If already has a colon, assume it's already formatted
        if ':' in id_str:
            return id_str
        
        # Use explicit prefix if provided
        if explicit_prefix:
            return f"{explicit_prefix}:{id_str}"
        
        # Auto-detect prefix
        for pattern, prefix in self.ID_PREFIXES.items():
            if id_str.startswith(pattern):
                return f"{prefix}:{id_str}"
        
        # Return as-is if no prefix found
        return id_str
    
    def _extract_properties(self, row: Dict, property_list: List[str]) -> Dict:
        """Extract specified properties from a row."""
        props = {}
        for prop in property_list:
            if prop in row and row[prop] not in [None, '', 'NA', 'N/A']:
                props[prop] = row[prop]
        
        # Add gene aliases if available
        if 'gene_id' in row and row['gene_id'] in self.gene_aliases:
            props['synonyms'] = self.gene_aliases[row['gene_id']]
        
        return props
    
    def _check_genomic_filter(self, row: Dict, chr_col: Optional[str], start_col: Optional[str], end_col: Optional[str]) -> bool:
        """Check if row passes genomic location filter."""
        if not chr_col or chr_col not in row:
            return True
        
        curr_chr = str(row[chr_col]).replace('chr', '').replace('ch', '')
        curr_start = int(row.get(start_col, 0)) if start_col and start_col in row else 0
        curr_end = int(row.get(end_col, curr_start)) if end_col and end_col in row else curr_start
        
        return check_genomic_location(self.chr, self.start, self.end, curr_chr, curr_start, curr_end)
    
    def get_nodes(self) -> Generator[Tuple[str, str, Dict], None, None]:
        """Generate nodes from the data."""
        if not self.node_config:
            return
        
        # Check if we have multiple node types
        if isinstance(self.node_config, dict) and 'id_column' not in self.node_config:
            # Multiple node types
            for node_type, config in self.node_config.items():
                yield from self._generate_nodes(config)
        else:
            # Single node type
            yield from self._generate_nodes(self.node_config)
    
    def _generate_nodes(self, config: Dict) -> Generator[Tuple[str, str, Dict], None, None]:
        """Generate nodes based on configuration."""
        id_column = config.get('id_column')
        label_column = config.get('label_column')
        label_constant = config.get('label_constant')
        properties = config.get('properties', [])
        format_curie = config.get('format_curie', True)
        id_prefix = config.get('id_prefix')
        strip_version = config.get('strip_version', True)
        chr_column = config.get('chr_column')
        start_column = config.get('start_column')
        end_column = config.get('end_column')
        
        if not id_column:
            logger.error("Node configuration missing 'id_column'")
            return
        
        if not label_column and not label_constant:
            logger.error("Node configuration missing 'label_column' or 'label_constant'")
            return
        
        for row in self._read_data():
            # Apply column transformations
            row = self._apply_column_transforms(row)
            
            # Apply filters
            if not self._apply_filters(row):
                continue
            
            # Apply row transformations
            rows = self._apply_row_transforms(row)
            
            for processed_row in rows:
                # Check genomic location filter
                if not self._check_genomic_filter(processed_row, chr_column, start_column, end_column):
                    continue
                
                # Extract node ID
                if id_column not in processed_row:
                    continue
                
                node_id = str(processed_row[id_column])
                
                # Format as CURIE if requested
                if format_curie:
                    node_id = self._format_curie(node_id, id_prefix, strip_version)
                
                # Determine label
                if label_column and label_column in processed_row:
                    label = processed_row[label_column]
                else:
                    label = label_constant
                
                # Extract properties
                props = {}
                if self.write_properties:
                    props = self._extract_properties(processed_row, properties)
                    
                    # Add genomic location properties if configured
                    if chr_column and chr_column in processed_row:
                        props['chr'] = processed_row[chr_column]
                    if start_column and start_column in processed_row:
                        props['start'] = processed_row[start_column]
                    if end_column and end_column in processed_row:
                        props['end'] = processed_row[end_column]
                    
                    # Add provenance
                    if self.add_provenance:
                        props['source'] = self.source
                        props['version'] = self.version
                        if self.source_url:
                            props['source_url'] = self.source_url
                
                yield node_id, label, props
    
    def get_edges(self) -> Generator[Tuple[str, str, str, Dict], None, None]:
        """Generate edges from the data."""
        if not self.edge_config:
            return
        
        source_column = self.edge_config.get('source_column')
        target_column = self.edge_config.get('target_column')
        label_column = self.edge_config.get('label_column')
        label_constant = self.edge_config.get('label_constant')
        properties = self.edge_config.get('properties', [])
        format_curie = self.edge_config.get('format_curie', True)
        source_prefix = self.edge_config.get('source_prefix')
        target_prefix = self.edge_config.get('target_prefix')
        strip_version = self.edge_config.get('strip_version', True)
        
        if not source_column or not target_column:
            logger.error("Edge configuration missing 'source_column' or 'target_column'")
            return
        
        if not label_column and not label_constant:
            logger.error("Edge configuration missing 'label_column' or 'label_constant'")
            return
        
