# biocypher_metta/generic_adapters/generic_file_adapter.py
import pandas as pd
import yaml
import os
import json
import gtfparse # You MUST install this: pip install gtfparse
import datetime # For current_timestamp_iso
import pickle # For loading auxiliary data like HGNC aliases

class GenericDataSourceAdapter:
    def __init__(self, schema_filepath: str, data_filepath: str, auxiliary_data: dict = None):
        """
        Initializes the generic adapter.

        Args:
            schema_filepath: Path to the YAML schema file for this dataset.
            data_filepath: Path to the raw data file.
            auxiliary_data: A dictionary containing universal auxiliary data
                            (e.g., {'tissue_ontology_map': map_data, 'hgnc_gene_aliases': alias_data}).
        """
        if not os.path.exists(schema_filepath):
            raise FileNotFoundError(f"Schema file not found: {schema_filepath}")
        if not os.path.exists(data_filepath):
            raise FileNotFoundError(f"Data file not found: {data_filepath}")

        self.schema = self._load_yaml(schema_filepath)
        self.data_filepath = data_filepath
        self.auxiliary_data = auxiliary_data if auxiliary_data is not None else {}
        self.source_id = self.schema.get('source_id', os.path.basename(schema_filepath).replace('.yaml', ''))
        
        self.raw_data = self._load_data() # Loads data based on 'data_format' specified in schema

        if 'nodes' not in self.schema and 'relationships' not in self.schema:
            raise ValueError(f"Schema {schema_filepath} for '{self.source_id}' must define 'nodes' or 'relationships'.")

    def _load_yaml(self, path: str):
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def _load_data(self):
        data_format = self.schema.get('data_format', 'csv').lower()

        if data_format == 'csv':
            return pd.read_csv(self.data_filepath)
        elif data_format == 'tsv':
            return pd.read_csv(self.data_filepath, sep='\t')
        elif data_format == 'json':
            with open(self.data_filepath, 'r') as f:
                return json.load(f)
        elif data_format == 'gtf':
            print(f"Loading GTF file using gtfparse: {self.data_filepath}")
            try:
                df = gtfparse.read_gtf(self.data_filepath)
                # Filter for gene entries directly, as in your original adapter
                genes_df = df[df["feature"] == "gene"].copy()
                # Rename 'seqname' to 'chromosome' for consistency with schema
                if "seqname" in genes_df.columns:
                    genes_df = genes_df.rename(columns={"seqname": "chromosome"})
                return genes_df
            except Exception as e:
                raise RuntimeError(f"Failed to parse GTF file '{self.data_filepath}': {e}. "
                                   "Please ensure 'gtfparse' is installed and the GTF file is valid.")
        else:
            raise ValueError(f"Unsupported data format '{data_format}' in schema for {self.source_id}")

    def _get_property_value(self, record, prop_mapping):
        """
        Helper to get a property value from a raw data record,
        handling direct column/key names, static values, and auxiliary lookups.
        'record' can be a pandas Series (row) or a dictionary (JSON record).
        """
        if isinstance(prop_mapping, dict): # Advanced mapping (e.g., using aux data, dynamic values)
            map_type = prop_mapping.get('map_using')
            source_key_name = prop_mapping.get('column') # Column/key in raw data to extract value from

            # Handle special dynamic value types first
            if map_type == "current_timestamp_iso":
                return datetime.datetime.now().isoformat()
            
            # Get the raw value from the record based on source_key_name
            raw_value = None
            if source_key_name: # If a column/key is specified for the mapping
                if isinstance(record, pd.Series):
                    if source_key_name in record.index and pd.notna(record[source_key_name]):
                        raw_value = record[source_key_name]
                elif isinstance(record, dict):
                    if source_key_name in record:
                        raw_value = record[source_key_name]
            
            # Apply auxiliary data mappings or custom transformations
            if map_type == "hgnc_gene_aliases" and raw_value is not None:
                if 'hgnc_gene_aliases' in self.auxiliary_data:
                    lookup_map = self.auxiliary_data['hgnc_gene_aliases']
                    # The HGNC map often maps gene_name to a list of synonyms
                    return lookup_map.get(raw_value, []) # Return empty list if not found
                else:
                    print(f"Warning: 'hgnc_gene_aliases' map not provided but requested for '{source_key_name}' in schema '{self.source_id}'. Returning empty list for synonyms.")
                    return []
            elif map_type == "tissue_ontology_map" and raw_value is not None:
                if 'tissue_ontology_map' in self.auxiliary_data:
                    lookup_map = self.auxiliary_data['tissue_ontology_map']
                    return lookup_map.get(raw_value, raw_value) # Fallback to original value if not found
                else:
                    print(f"Warning: 'tissue_ontology_map' not provided but requested for '{source_key_name}' in schema '{self.source_id}'. Returning original value.")
                    return raw_value
            elif map_type == "gencode_url_builder" and raw_value is not None:
                # Assuming raw_value is the version string (e.g., "M43" or "43")
                try:
                    # Extract numeric part, handling 'M' prefix if present
                    major_version = raw_value.replace('M', '').split('.')[0] 
                    return f"https://www.gencodegenes.org/human/release-{major_version}.html"
                except Exception as e:
                    print(f"Warning: Could not build Gencode URL for version '{raw_value}' from schema '{self.source_id}': {e}. Returning generic URL.")
                    return f"https://www.gencodegenes.org/human/release-overview.html" # Fallback URL
            
            # If map_type is unknown or raw_value is None and no special handling applied
            return raw_value

        elif isinstance(prop_mapping, (str, int, float, bool, list)): # Direct column name or static value
            # Try to get value from record first (if it's a column name)
            if isinstance(record, pd.Series):
                if prop_mapping in record.index and pd.notna(record[prop_mapping]):
                    return record[prop_mapping]
            elif isinstance(record, dict):
                if prop_mapping in record:
                    return record[prop_mapping]
            
            # If not found as a column/key, treat it as a static value from the schema
            return prop_mapping
        
        return None # Default return if nothing matches or value is None/NaN

    def get_nodes(self):
        nodes = []
        for node_type, node_config in self.schema.get('nodes', {}).items():
            id_field = node_config.get('id_field')
            properties_map = node_config.get('properties', {})

            if not id_field:
                print(f"Warning: Node type '{node_type}' in {self.source_id} is missing required 'id_field'. Skipping.")
                continue

            records_to_process = []
            if isinstance(self.raw_data, pd.DataFrame):
                if id_field not in self.raw_data.columns:
                    print(f"Warning: ID field '{id_field}' not found in DataFrame for node type '{node_type}' in {self.source_id}. Skipping.")
                    continue
                records_to_process = self.raw_data.iterrows()
            elif isinstance(self.raw_data, list) and all(isinstance(i, dict) for i in self.raw_data):
                records_to_process = [(None, r) for r in self.raw_data] # Simulate iterrows for list of dicts
            else:
                print(f"Warning: Data format for '{self.source_id}' not suitable for generic node extraction. Data type: {type(self.raw_data)}. Skipping.")
                continue

            for _, record in records_to_process:
                try:
                    node_id = str(record[id_field])
                    properties = {}
                    for bio_prop_name, raw_mapping in properties_map.items():
                        val = self._get_property_value(record, raw_mapping)
                        if val is not None: # Only add property if value is not None
                            properties[bio_prop_name] = val
                    nodes.append((node_id, node_type, properties))
                except KeyError as ke:
                    print(f"Error processing record for node type '{node_type}' from schema '{self.source_id}': Missing expected key '{ke}'. Skipping record.")
                    continue
                except Exception as e:
                    print(f"Error processing record for node type '{node_type}' from schema '{self.source_id}': {e}. Skipping record.")
                    continue
        return nodes

    def get_edges(self):
        edges = []
        for rel_type, rel_config in self.schema.get('relationships', {}).items():
            source_id_field = rel_config.get('source_id_field')
            target_id_field = rel_config.get('target_id_field')
            properties_map = rel_config.get('properties', {})

            if not (source_id_field and target_id_field):
                print(f"Warning: Relationship '{rel_type}' in {self.source_id} is missing required source/target ID fields. Skipping.")
                continue

            records_to_process = []
            if isinstance(self.raw_data, pd.DataFrame):
                if not (source_id_field in self.raw_data.columns and target_id_field in self.raw_data.columns):
                    print(f"Warning: Source ('{source_id_field}') or target ('{target_id_field}') ID column not found in DataFrame for relationship '{rel_type}' in {self.source_id}. Skipping.")
                    continue
                records_to_process = self.raw_data.iterrows()
            elif isinstance(self.raw_data, list) and all(isinstance(i, dict) for i in self.raw_data):
                records_to_process = [(None, r) for r in self.raw_data]
            else:
                print(f"Warning: Data format for '{self.source_id}' not suitable for generic edge extraction. Data type: {type(self.raw_data)}. Skipping.")
                continue

            for _, record in records_to_process:
                try:
                    source_id = str(record[source_id_field])
                    target_id = str(record[target_id_field])
                    properties = {}
                    for bio_prop_name, raw_mapping in properties_map.items():
                        val = self._get_property_value(record, raw_mapping)
                        if val is not None:
                            properties[bio_prop_name] = val
                    edges.append((source_id, target_id, rel_type, properties))
                except KeyError as ke:
                    print(f"Error processing record for relationship type '{rel_type}' from schema '{self.source_id}': Missing expected key '{ke}'. Skipping record.")
                    continue
                except Exception as e:
                    print(f"Error processing record for relationship type '{rel_type}' from schema '{self.source_id}': {e}. Skipping record.")
                    continue
        return edges