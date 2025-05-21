# create_generic_kg.py
"""
Knowledge graph generation through BioCypher script using generic adapters.
"""
from datetime import date
import datetime # ADDED: Import datetime module
from pathlib import Path
import os # ADDED: Import os for path checking in generic adapter

from biocypher import BioCypher
# Import YOUR CUSTOM WRITERS from biocypher_metta
from biocypher_metta.metta_writer import MeTTaWriter
from biocypher_metta.prolog_writer import PrologWriter
from biocypher_metta.neo4j_csv_writer import Neo4jCSVWriter
from biocypher_metta.parquet_writer import ParquetWriter
from biocypher._logger import logger
import typer
import yaml
import importlib # For reflection (though mostly for non-generic adapters now)
from typing_extensions import Annotated
import pickle
import json
from collections import Counter, defaultdict
from typing import Union, List, Optional
import pandas as pd # Needed for checking DataFrame from gtfparse
import traceback # ADDED: For more detailed error reporting

# Import your new generic adapter
from biocypher_metta.generic_adapters.generic_file_adapter import GenericDataSourceAdapter

app = typer.Typer()

# Function to choose the writer class based on user input
def get_writer(writer_type: str, output_dir: Path):
    # Pass schema and biocypher config paths directly here
    schema_config_path = "config/schema_config.yaml"
    biocypher_config_path = "config/biocypher_config.yaml"

    if writer_type == 'metta':
        return MeTTaWriter(schema_config=schema_config_path,
                           biocypher_config=biocypher_config_path,
                           output_dir=output_dir)
    elif writer_type == 'prolog':
        return PrologWriter(schema_config=schema_config_path,
                            biocypher_config=biocypher_config_path,
                            output_dir=output_dir)
    elif writer_type == 'neo4j':
        return Neo4jCSVWriter(schema_config=schema_config_path,
                              biocypher_config=biocypher_config_path,
                              output_dir=output_dir)
    elif writer_type == 'parquet':
        return ParquetWriter(
            schema_config=schema_config_path,
            biocypher_config=biocypher_config_path,
            output_dir=output_dir,
            buffer_size=10000, # Default for now
            overwrite=True     # Default for now
        )
    else:
        raise ValueError(f"Unknown writer type: {writer_type}")

def preprocess_schema():
    def convert_input_labels(label, replace_char="_"):
        return label.replace(" ", replace_char)

    # Use BioCypher for schema processing, similar to your existing script
    bcy = BioCypher(
        schema_config_path="config/schema_config.yaml", biocypher_config_path="config/biocypher_config.yaml"
    )
    schema = bcy._get_ontology_mapping()._extend_schema() # Private method, be aware of updates
    edge_node_types = {}

    for k, v in schema.items():
        if v["represented_as"] == "edge":
            source_type = v.get("source", None)
            target_type = v.get("target", None)

            if source_type is not None and target_type is not None:
                label = convert_input_labels(v["input_label"])
                source_type = convert_input_labels(source_type)
                target_type = convert_input_labels(target_type)
                output_label = v.get("output_label", None)

                edge_node_types[label.lower()] = {
                    "source": source_type.lower(),
                    "target": target_type.lower(),
                    "output_label": output_label.lower() if output_label else None,
                }

    return edge_node_types

def gather_graph_info(nodes_count, nodes_props, edges_count, schema_dict, output_dir):
    graph_info = {
        'node_count': sum(nodes_count.values()),
        'edge_count': sum(edges_count.values()),
        'dataset_count': 0, # Will be updated later
        'data_size': '',
        'top_entities': [{'name': node, 'count': count} for node, count in nodes_count.items()],
        'top_connections': [],
        'frequent_relationships': [],
        'schema': {'nodes': [], 'edges': []},
        'datasets': [] # Will be updated later
    }

    predicate_count = Counter()
    relations_frequency = Counter()
    possible_connections = defaultdict(set)

    for edge, count in edges_count.items():
        # Ensure the key exists in schema_dict before accessing it
        if edge.lower() in schema_dict:
            label = schema_dict[edge.lower()]['output_label'] or edge
            source = schema_dict[edge.lower()]['source']
            target = schema_dict[edge.lower()]['target']

            predicate_count[label] += count
            relations_frequency[f'{source}|{target}'] += count
            possible_connections[f'{source}|{target}'].add(label)
        else:
            logger.warning(f"Edge type '{edge}' not found in schema_dict. Skipping for graph info.")
            # For robustness, add to predicate_count anyway, using edge as label
            predicate_count[edge] += count
            # Cannot infer source/target without schema, so skip relations_frequency and possible_connections

    graph_info['top_connections'] = [{'name': predicate, 'count': count} for predicate, count in predicate_count.items()]
    graph_info['frequent_relationships'] = [{'entities': rel.split('|'), 'count': count} for rel, count in relations_frequency.items()]

    for node, props in nodes_props.items():
        graph_info['schema']['nodes'].append({'data': {'name': node, 'properties': list(props)}})

    for conn, pos_connections in possible_connections.items():
        source, target = conn.split('|')
        graph_info['schema']['edges'].append({'data': {'source': source, 'target': target, 'possible_connections': list(pos_connections)}})

    # Calculate data size
    total_size = sum(file.stat().st_size for file in Path(output_dir).rglob('*') if file.is_file())
    total_size_gb = total_size / (1024 ** 3)  # 1GB == 1024^3
    graph_info['data_size'] = f"{total_size_gb:.2f} GB"

    return graph_info

# MODIFIED: process_adapters to handle both custom and generic adapters
def process_adapters(adapters_to_process: dict, dbsnp_rsids_dict, dbsnp_pos_dict, hgnc_aliases_dict, tissue_ontology_map_dict, writer, write_properties, add_provenance, schema_dict):
    nodes_count = Counter()
    nodes_props = defaultdict(set)
    edges_count = Counter()
    datasets_dict = {}

    # Iterate over the correctly extracted datasets
    for c, adapter_config in adapters_to_process.items(): # ITERATION IS NOW CORRECT
        writer.clear_counts() # Reset counter for this adapter
        logger.info(f"Running adapter: {c}")
        
        dataset_name = None # Initialize
        version = None
        source_url = None
        adapter = None # Initialize adapter to None

        # Determine if it's a generic adapter or a custom one
        if adapter_config.get("type") == "generic_file_adapter":
            # --- Handle GenericDataSourceAdapter ---
            schema_filepath = adapter_config.get("schema_filepath")
            data_filepath = adapter_config.get("data_filepath")

            if not schema_filepath or not data_filepath:
                logger.error(f"Skipping generic dataset '{c}': Missing 'schema_filepath' or 'data_filepath' in its configuration.")
                continue
            if not os.path.exists(data_filepath):
                logger.error(f"Skipping generic dataset '{c}': Data file not found at '{data_filepath}'.")
                continue

            # Pass all universal auxiliary data to the generic adapter
            universal_aux_data = {
                'hgnc_gene_aliases': hgnc_aliases_dict,
                'tissue_ontology_map': tissue_ontology_map_dict,
                # Add dbsnp maps if GenericDataSourceAdapter were to use them
                # 'dbsnp_rsids_map': dbsnp_rsids_dict,
                # 'dbsnp_pos_map': dbsnp_pos_dict,
            }

            try:
                adapter = GenericDataSourceAdapter(schema_filepath, data_filepath, auxiliary_data=universal_aux_data)
                
                # Check if data was loaded correctly (especially for GTF)
                if not isinstance(adapter.raw_data, pd.DataFrame) or adapter.raw_data.empty:
                    raise RuntimeError(
                        f"Adapter for '{c}' loaded no data or invalid data. "
                        f"Check your data file ('{data_filepath}') and the associated generic schema ('{schema_filepath}')."
                    )
                
                dataset_name = c # Use the dataset name from adapters_config_generic.yaml
                source_url = data_filepath # Use data path as URL for provenance for generic files

            except Exception as e:
                logger.error(f"Error initializing GenericDataSourceAdapter for '{c}': {e}")
                traceback.print_exc()
                continue

        else:
            # --- Handle Original Custom Adapters (using importlib) ---
            if "adapter" not in adapter_config:
                logger.error(f"Skipping adapter '{c}': Missing 'adapter' configuration key for non-generic type.")
                continue

            adapter_module = importlib.import_module(adapter_config["adapter"]["module"])
            adapter_cls = getattr(adapter_module, adapter_config["adapter"]["cls"])
            ctr_args = adapter_config["adapter"]["args"]

            if "dbsnp_rsid_map" in ctr_args:
                ctr_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
            if "dbsnp_pos_map" in ctr_args:
                ctr_args["dbsnp_pos_map"] = dbsnp_pos_dict
            
            # These are arguments *for the custom adapter's constructor*
            # They might or might not be used by a specific custom adapter
            ctr_args["write_properties"] = write_properties
            ctr_args["add_provenance"] = add_provenance

            try:
                adapter = adapter_cls(**ctr_args)
                dataset_name = getattr(adapter, 'source', c) # Use source if available, else config key
                version = getattr(adapter, 'version', None)
                source_url = getattr(adapter, 'source_url', None)

            except Exception as e:
                logger.error(f"Error initializing custom adapter '{c}': {e}")
                traceback.print_exc()
                continue
        
        # Common processing for both generic and custom adapters
        if adapter: # Only proceed if adapter was successfully initialized
            outdir = adapter_config.get("outdir", "") # Use outdir from config, default to empty string

            if dataset_name is None:
                logger.warning(f"Dataset name is None for adapter: {c}. Ensure 'source' is defined or inferred.")
            else:
                if dataset_name not in datasets_dict:
                    datasets_dict[dataset_name] = {
                        "name": dataset_name,
                        "version": version,
                        "url": source_url,
                        "nodes": set(),
                        "edges": set(),
                        "imported_on": str(date.today())
                    }
            
            # The 'nodes' and 'edges' keys determine if we call get_nodes/get_edges
            # These are specific to the adapters_config.yaml format, not the generic one
            # For generic adapter, we always assume we want to get nodes/edges
            # So, we check 'nodes' and 'edges' in the adapter_config, if not present
            # for generic adapters, default to True
            # This logic might need refinement if 'nodes' and 'edges' keys are ALWAYS expected
            # in the generic adapter config. For now, default to True for generic type.
            write_nodes_flag = adapter_config.get("nodes", True) if adapter_config.get("type") == "generic_file_adapter" else adapter_config.get("nodes", False)
            write_edges_flag = adapter_config.get("edges", True) if adapter_config.get("type") == "generic_file_adapter" else adapter_config.get("edges", False)


            if write_nodes_flag:
                try:
                    nodes = adapter.get_nodes()
                    freq, props = writer.write_nodes(nodes, path_prefix=outdir)
                    for node_label in freq:
                        nodes_count[node_label] += freq[node_label]
                        if dataset_name is not None:
                            datasets_dict[dataset_name]['nodes'].add(node_label)
                    for node_label in props:
                        nodes_props[node_label] = nodes_props[node_label].union(props[node_label])
                    logger.info(f"Wrote {len(nodes)} nodes for dataset: {dataset_name}")
                except Exception as e:
                    logger.error(f"Error processing nodes for adapter '{c}': {e}")
                    traceback.print_exc()

            if write_edges_flag:
                try:
                    edges = adapter.get_edges()
                    freq = writer.write_edges(edges, path_prefix=outdir)
                    for edge_label in freq:
                        edges_count[edge_label] += freq[edge_label]
                        # Use schema_dict for output_label for consistency, but handle missing keys
                        label = schema_dict.get(edge_label.lower(), {}).get('output_label', edge_label)
                        if dataset_name is not None:
                            datasets_dict[dataset_name]['edges'].add(label)
                    logger.info(f"Wrote {len(edges)} edges for dataset: {dataset_name}")
                except Exception as e:
                    logger.error(f"Error processing edges for adapter '{c}': {e}")
                    traceback.print_exc()

    return nodes_count, nodes_props, edges_count, datasets_dict

# Run build
@app.command()
def main(output_dir: Annotated[Path, typer.Option(exists=True, file_okay=False, dir_okay=True, help="Base output directory for the KG files. A timestamped subdirectory will be created inside.")],
         adapters_config: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False, help="Path to the generic adapters configuration YAML (Default: config/adapters_config_generic.yaml).")],
         hgnc_aliases: Annotated[Optional[Path], typer.Option(exists=True, file_okay=True, dir_okay=False, help="Path to HGNC gene alias mapping file (e.g., hgnc_gene_data/hgnc_data.pkl).")] = None, # Corrected to Optional and default to None
         dbsnp_rsids: Annotated[Optional[Path], typer.Option(exists=True, file_okay=True, dir_okay=False, help="Path to dbsnp rsids map (optional, for specific custom adapters).")] = None,
         dbsnp_pos: Annotated[Optional[Path], typer.Option(exists=True, file_okay=True, dir_okay=False, help="Path to dbsnp pos map (optional, for specific custom adapters).")] = None,
         tissue_ontology_map: Annotated[Optional[Path], typer.Option(exists=True, file_okay=True, dir_okay=False, help="Path to auxiliary tissue-to-ontology mapping file (used for ABC data).")] = None,
         writer_type: str = typer.Option(default="neo4j", help="Choose writer type: metta, prolog, neo4j, parquet"),
         write_properties: bool = typer.Option(True, help="Write properties to nodes and edges"),
         no_add_provenance: bool = typer.Option(False, "--no-add-provenance", help="Do not add provenance to nodes and edges (default: add provenance)."), # Renamed for clarity and to match CLI
         buffer_size: int = typer.Option(10000, help="Buffer size for Parquet writer"),
         overwrite: bool = typer.Option(True, help="Overwrite existing Parquet files"),
         include_adapters: Optional[List[str]] = typer.Option(
             None,
             help="Specific adapters to include (space-separated, default: all). Use dataset names from adapters_config.yaml.",
             case_sensitive=False,
         )):
    """
    Main function for creating a knowledge graph using generic or custom adapters.
    """
    add_provenance = not no_add_provenance # Convert no_add_provenance to add_provenance

    # Start biocypher
    dbsnp_rsids_dict = None
    if dbsnp_rsids:
        logger.info("Loading dbsnp rsids map")
        dbsnp_rsids_dict = pickle.load(open(dbsnp_rsids, 'rb'))
    else:
        logger.info("No dbsnp rsids map provided.")

    dbsnp_pos_dict = None
    if dbsnp_pos:
        logger.info("Loading dbsnp pos map")
        dbsnp_pos_dict = pickle.load(open(dbsnp_pos, 'rb'))
    else:
        logger.info("No dbsnp pos map provided.")

    hgnc_aliases_dict = None
    if hgnc_aliases:
        logger.info("Loading HGNC aliases map")
        hgnc_aliases_dict = pickle.load(open(hgnc_aliases, 'rb'))
    else:
        logger.info("No HGNC aliases map provided.")

    tissue_ontology_map_dict = None
    if tissue_ontology_map:
        logger.info("Loading tissue ontology map")
        tissue_ontology_map_dict = pickle.load(open(tissue_ontology_map, 'rb'))
    else:
        logger.info("No tissue ontology map provided.")

    # Create the timestamped output directory
    current_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    final_output_dir = output_dir / f"generic_kg_output_{current_timestamp}"
    final_output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output will be in: {final_output_dir}")

    # Choose the writer based on user input or default to 'neo4j' (as per your example)
    bc_writer = get_writer(writer_type, final_output_dir)
    logger.info(f"Using {writer_type} writer")

    if writer_type == 'parquet':
        bc_writer.buffer_size = buffer_size
        bc_writer.overwrite = overwrite

    schema_dict = preprocess_schema() # Get schema for graph info

    with open(adapters_config, "r") as fp:
        try:
            full_config = yaml.safe_load(fp)
            # IMPORTANT CHANGE: Get the 'datasets' dictionary to iterate over
            adapters_to_process = full_config.get('datasets', {})
        except yaml.YAMLError as e:
            logger.error("Error while trying to load adapter config")
            logger.error(e)
            raise typer.Exit(1)

    # Filter adapters if specific ones are requested
    if include_adapters:
        original_count = len(adapters_to_process) # Now correctly references the datasets
        include_lower = [a.lower() for a in include_adapters]
        adapters_to_process = {
            k: v for k, v in adapters_to_process.items()
            if k.lower() in include_lower
        }
        if not adapters_to_process:
            # Show all available top-level dataset keys from the full config if filtering yields nothing
            available = "\n".join(f" - {a}" for a in full_config.get('datasets', {}).keys())
            logger.error(f"No matching adapters found. Available datasets:\n{available}")
            raise typer.Exit(1)
        
        logger.info(f"Filtered to {len(adapters_to_process)}/{original_count} datasets")

    # Run adapters with the correctly extracted adapters_to_process
    nodes_count, nodes_props, edges_count, datasets_dict = process_adapters(
        adapters_to_process, dbsnp_rsids_dict, dbsnp_pos_dict, hgnc_aliases_dict, tissue_ontology_map_dict,
        bc_writer, write_properties, add_provenance, schema_dict
    )

    # Finalize the writer - your existing logic
    if hasattr(bc_writer, 'finalize'):
        bc_writer.finalize()
    else:
        logger.warning(f"Writer type '{writer_type}' does not have a 'finalize' method.")


    # Gather graph info
    graph_info = gather_graph_info(nodes_count, nodes_props, edges_count, schema_dict, final_output_dir)

    for dataset in datasets_dict:
        # Convert sets to lists for JSON serialization
        datasets_dict[dataset]["nodes"] = list(datasets_dict[dataset]["nodes"])
        datasets_dict[dataset]["edges"] = list(datasets_dict[dataset]["edges"])
        graph_info['datasets'].append(datasets_dict[dataset])

    graph_info["dataset_count"] = len(graph_info['datasets'])

    # Write the graph info to JSON
    graph_info_json = json.dumps(graph_info, indent=2)
    file_path = f"{final_output_dir}/graph_info.json"
    with open(file_path, "w") as f:
        f.write(graph_info_json)

    logger.info("Done")

if __name__ == "__main__":
    app()