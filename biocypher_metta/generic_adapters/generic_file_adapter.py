import pandas as pd
import gtfparse
import csv
from loguru import logger
import json
import os
import traceback
import yaml # <--- NEW: Import the PyYAML library

class GenericDataSourceAdapter:
    def __init__(self, schema_filepath, data_filepath, auxiliary_data=None):
        self.schema_filepath = schema_filepath
        self.data_filepath = data_filepath
        self.auxiliary_data = auxiliary_data if auxiliary_data is not None else {}
        
        # Load schema first, which defines data_format
        self.schema = self._load_schema() 
        self.data_format = self.schema.get("data_format")

        # Then load data based on the format
        self.raw_data = self._load_data() 
        if self.raw_data.empty:
            logger.warning(f"No valid raw data loaded for {self.data_filepath}. "
                           "This dataset will not contribute nodes or edges.")

        self._initialize_mappings()

    def _load_schema(self):
        """
        Loads the schema configuration for the data source.
        Uses yaml.safe_load to correctly parse YAML files.
        """
        try:
            with open(self.schema_filepath, 'r') as f:
                return yaml.safe_load(f) # <--- MODIFIED: Use yaml.safe_load
        except FileNotFoundError:
            logger.error(f"Schema file not found: {self.schema_filepath}")
            raise # Re-raise if the schema file itself is missing, as it's critical
        except yaml.YAMLError as e: # <--- MODIFIED: Catch YAML specific errors
            logger.error(f"Error parsing schema file '{self.schema_filepath}': {e}.")
            logger.error(f"Please ensure '{self.schema_filepath}' is a valid YAML file.")
            raise # Re-raise if schema parsing fails, as it's critical

    def _load_data(self):
        """
        Loads raw data from the specified data_filepath based on the data_format.
        Includes robust error handling to prevent script crashes due to malformed files.
        """
        logger.info(f"Attempting to load {self.data_format} file: {self.data_filepath}")
        try:
            if self.data_format == "gtf":
                # gtfparse.read_gtf handles .gz files automatically
                df = gtfparse.read_gtf(self.data_filepath)
                logger.info(f"Successfully loaded GTF file: {self.data_filepath}")
                return df
            elif self.data_format == "csv" or self.data_format == "tsv":
                delimiter = self.schema.get("delimiter", "," if self.data_format == "csv" else "\t")
                # Adding 'comment' parameter to skip comment lines (often starting with '#')
                df = pd.read_csv(self.data_filepath, sep=delimiter, comment='#')
                logger.info(f"Successfully loaded {self.data_format} file: {self.data_filepath}")
                return df
            else:
                logger.error(f"Unsupported data format: '{self.data_format}' specified in schema for '{self.data_filepath}'. "
                             "Supported formats are 'gtf', 'csv', 'tsv'.")
                return pd.DataFrame() # Return empty DataFrame for unsupported format

        except FileNotFoundError:
            logger.error(f"Data file not found at '{self.data_filepath}'. Please ensure the path is correct.")
            return pd.DataFrame() # Return empty DataFrame on file not found
        except Exception as e:
            # Catch any other exception during loading (e.g., TypeError from gtfparse, parsing errors from pandas)
            logger.error(f"Failed to parse {self.data_format} file '{self.data_filepath}': {e}.")
            logger.debug(f"Full traceback for parsing error:\n{traceback.format_exc()}")
            logger.warning(f"Returning empty DataFrame for '{self.data_filepath}' due to parsing error. "
                           "This dataset will be skipped. Please check the file's validity and content, "
                           "especially for malformed lines in GTF attributes or incorrect delimiters in CSV/TSV.")
            return pd.DataFrame() # <--- CRITICAL: Returns empty DataFrame to prevent crash

    def _initialize_mappings(self):
        # 'node_mappings' and 'edge_mappings' should align with the 'nodes' and 'relationships' keys in your generic schema
        self.node_mappings = self.schema.get("nodes", {})
        self.edge_mappings = self.schema.get("relationships", {})

    def get_nodes(self):
        """
        Generates BioCypher node representations from the loaded raw data.
        Includes row-level error handling.
        """
        if self.raw_data is None or self.raw_data.empty:
            logger.info("No raw data available for node extraction.")
            return # Yield nothing if no data was loaded

        for data_type, mapping in self.node_mappings.items():
            filter_column = mapping.get("filter_column") # For GTF, often 'feature'
            filter_value = mapping.get("filter_value")   # For GTF, often 'gene'
            id_field = mapping.get("id_field")           # New: Using 'id_field' as per your schema

            df_filtered = self.raw_data
            if filter_column and filter_value:
                if filter_column not in df_filtered.columns:
                    logger.warning(f"Filter column '{filter_column}' not found for node type '{data_type}' from '{self.data_filepath}'. Skipping filter.")
                else:
                    df_filtered = self.raw_data[self.raw_data[filter_column] == filter_value]

            if not id_field:
                logger.error(f"Missing 'id_field' in schema for node type '{data_type}' from '{self.data_filepath}'. Skipping node extraction for this type.")
                continue

            if id_field not in df_filtered.columns:
                logger.error(f"ID field '{id_field}' not found in filtered data for node type '{data_type}' from '{self.data_filepath}'. Skipping node extraction for this type.")
                continue

            properties_map = mapping.get("properties", {})

            for idx, row in df_filtered.iterrows():
                try:
                    node_id = str(row[id_field]) # Ensure node_id is always a string

                    node_props = {}
                    for prop_key, prop_value_source in properties_map.items():
                        if isinstance(prop_value_source, dict):
                            # Handle special mapping types
                            if prop_value_source.get("map_using") == "auxiliary_lookup":
                                aux_data_key = prop_value_source.get("aux_data_key") # Key to find data in self.auxiliary_data
                                lookup_key_column = prop_value_source.get("column")  # Column in GTF to use as lookup key
                                default_value = prop_value_source.get("default")
                                
                                if aux_data_key and lookup_key_column and lookup_key_column in row:
                                    lookup_val = row[lookup_key_column]
                                    # Handle comma-separated keys if necessary (e.g., multiple gene names)
                                    if isinstance(lookup_val, str) and ',' in lookup_val:
                                        lookup_keys = [k.strip() for k in lookup_val.split(',')]
                                    else:
                                        lookup_keys = [lookup_val]

                                    found_value = None
                                    for k in lookup_keys:
                                        if k in self.auxiliary_data.get(aux_data_key, {}):
                                            found_value = self.auxiliary_data[aux_data_key][k]
                                            break
                                    node_props[prop_key] = found_value if found_value is not None else default_value
                                else:
                                    node_props[prop_key] = default_value # Or handle as missing
                            elif prop_value_source.get("map_using") == "gencode_url_builder":
                                column = prop_value_source.get("column")
                                if column and column in row:
                                    # Example: Build URL from version or gene_id
                                    version = str(row[column])
                                    node_props[prop_key] = f"https://www.gencodegenes.org/human/release_{version}.html"
                                else:
                                    node_props[prop_key] = None # Or a default URL
                            elif prop_value_source.get("map_using") == "current_timestamp_iso":
                                from datetime import datetime
                                node_props[prop_key] = datetime.now().isoformat()
                            else:
                                # Handle other complex mappings if they arise
                                logger.warning(f"Unsupported complex property mapping for '{prop_key}': {prop_value_source}")
                                node_props[prop_key] = None # Default for unsupported complex mapping
                        elif prop_value_source in row: # Direct column mapping
                            value = row[prop_value_source]
                            # Handle NaN/empty strings from pandas
                            if pd.isna(value) or (isinstance(value, str) and value.strip() == ""):
                                node_props[prop_key] = None
                            else:
                                node_props[prop_key] = value
                        else: # Static value
                            node_props[prop_key] = prop_value_source 

                    # Add default 'source' property if not already present
                    if 'source' not in node_props:
                        node_props['source'] = os.path.basename(self.data_filepath)

                    yield node_id, data_type, node_props

                except Exception as row_e:
                    logger.warning(f"Skipping node for row {idx} from {self.data_filepath} (ID: {node_id if 'node_id' in locals() else 'N/A'}) due to error: {row_e}. "
                                   f"Row data: {row.to_dict()}")
                    logger.debug(f"Full traceback for row {idx}:\n{traceback.format_exc()}")
                    continue # Continue to the next row

    def get_edges(self):
        """
        Generates BioCypher edge representations from the loaded raw data.
        Includes row-level error handling.
        """
        if self.raw_data is None or self.raw_data.empty:
            logger.info("No raw data available for edge extraction.")
            return # Yield nothing if no data was loaded

        for edge_type, mapping in self.edge_mappings.items():
            filter_column = mapping.get("filter_column")
            filter_value = mapping.get("filter_value")
            source_id_field = mapping.get("source_id_field") # New: Using 'source_id_field'
            target_id_field = mapping.get("target_id_field") # New: Using 'target_id_field'

            df_filtered = self.raw_data
            if filter_column and filter_value:
                if filter_column not in df_filtered.columns:
                    logger.warning(f"Filter column '{filter_column}' not found for edge type '{edge_type}' from '{self.data_filepath}'. Skipping filter.")
                else:
                    df_filtered = self.raw_data[self.raw_data[filter_column] == filter_value]

            if not source_id_field or not target_id_field:
                logger.error(f"Missing 'source_id_field' or 'target_id_field' in schema for edge type '{edge_type}' from '{self.data_filepath}'. Skipping edge extraction for this type.")
                continue
            
            if source_id_field not in df_filtered.columns or target_id_field not in df_filtered.columns:
                logger.error(f"Source or target ID field missing in filtered data for edge type '{edge_type}' from '{self.data_filepath}'. Skipping edge extraction for this type.")
                continue

            properties_map = mapping.get("properties", {})

            for idx, row in df_filtered.iterrows():
                try:
                    source_id = str(row[source_id_field])
                    target_id = str(row[target_id_field])
                    
                    edge_props = {}
                    for prop_key, prop_value_source in properties_map.items():
                        # This section can be expanded for complex edge property mappings like nodes
                        if prop_value_source in row:
                            value = row[prop_value_source]
                            if pd.isna(value) or (isinstance(value, str) and value.strip() == ""):
                                edge_props[prop_key] = None
                            else:
                                edge_props[prop_key] = value
                        else:
                            edge_props[prop_key] = prop_value_source # Static value

                    # Add default 'source' property if not already present
                    if 'source' not in edge_props:
                        edge_props['source'] = os.path.basename(self.data_filepath)

                    yield None, source_id, target_id, edge_type, edge_props

                except Exception as row_e:
                    logger.warning(f"Skipping edge for row {idx} from {self.data_filepath} (Source: {source_id if 'source_id' in locals() else 'N/A'}, Target: {target_id if 'target_id' in locals() else 'N/A'}) due to error: {row_e}. "
                                   f"Row data: {row.to_dict()}")
                    logger.debug(f"Full traceback for row {idx}:\n{traceback.format_exc()}")
                    continue # Continue to the next row