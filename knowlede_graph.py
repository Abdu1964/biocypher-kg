"""
Knowledge graph generation through BioCypher script (MongoDB only)
"""

from datetime import date
from biocypher import BioCypher
from biocypher_metta.Mongodb_Writer import MongodbWriter
from biocypher._logger import logger
import typer
import yaml
import importlib
from typing_extensions import Annotated
import pickle
import json
from collections import Counter, defaultdict
from pymongo import MongoClient
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pathlib import Path

app = typer.Typer()

# Load MongoDB credentials
def get_mongo_client():
    load_dotenv()
    # Get MongoDB credentials from environment
    username = urllib.parse.quote_plus(os.getenv("MONGODB_USERNAME"))
    password = urllib.parse.quote_plus(os.getenv("MONGODB_PASSWORD"))
    cluster = os.getenv("MONGODB_CLUSTER")
    appname = os.getenv("MONGODB_APPNAME")

# Construct the URI
uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority&appName={appname}"

def fetch_from_mongodb(db_name, node_prefix="nodes_", edge_prefix="edges_"):
    """Fetch and log collection counts from MongoDB"""
    client = get_mongo_client()
    db = client[db_name]

    # Log node collections
    nodes_collection = [col for col in db.list_collection_names() if col.startswith(node_prefix)]
    for collection in nodes_collection:
        label = collection.replace(node_prefix, "")
        count = db[collection].count_documents({})
        logger.info(f"MongoDB: {count} nodes found in collection: {collection}")

    # Log edge collections
    edges_collection = [col for col in db.list_collection_names() if col.startswith(edge_prefix)]
    for collection in edges_collection:
        label = collection.replace(edge_prefix, "")
        count = db[collection].count_documents({})
        logger.info(f"MongoDB: {count} edges found in collection: {collection}")

def get_writer(writer_type: str, db_name: str):
    """Get the appropriate writer instance"""
    if writer_type.lower() != 'mongodb':
        raise ValueError("Only 'mongodb' writer type is supported")
    
    return MongodbWriter(
        schema_config="config/schema_config.yaml",
        biocypher_config="config/biocypher_config.yaml",
        output_dir=db_name
    )

def preprocess_schema():
    """Process the schema configuration and extract edge relationships"""
    def convert_input_labels(label):
        return label.replace(" ", "_")

    bcy = BioCypher(
        schema_config_path="config/schema_config.yaml", 
        biocypher_config_path="config/biocypher_config.yaml"
    )
    schema = bcy._get_ontology_mapping()._extend_schema()
    edge_node_types = {}

    for k, v in schema.items():
        if v["represented_as"] == "edge":
            label = convert_input_labels(v["input_label"])
            source_type = convert_input_labels(v.get("source", ""))
            target_type = convert_input_labels(v.get("target", ""))
            output_label = v.get("output_label", None)

            edge_node_types[label.lower()] = {
                "source": source_type.lower(),
                "target": target_type.lower(),
                "output_label": output_label.lower() if output_label else None,
            }

    return edge_node_types

def process_adapters(adapters_dict, dbsnp_rsids_dict, dbsnp_pos_dict, writer, write_properties, add_provenance, schema_dict):
    """Process all adapters and write nodes/edges to MongoDB"""
    nodes_count = Counter()
    edges_count = Counter()

    for adapter_name, adapter_data in adapters_dict.items():
        logger.info(f"Running adapter: {adapter_name}")
        
        try:
            # Initialize adapter
            adapter_module = importlib.import_module(adapter_data["adapter"]["module"])
            adapter_cls = getattr(adapter_module, adapter_data["adapter"]["cls"])
            adapter = adapter_cls(
                **adapter_data["adapter"]["args"],
                write_properties=write_properties,
                add_provenance=add_provenance
            )

            # Process nodes
            if adapter_data["nodes"]:
                nodes = adapter.get_nodes()
                freq, _ = writer.write_nodes(nodes, write_properties=write_properties)
                nodes_count.update(freq)

            # Process edges
            if adapter_data["edges"]:
                edges = adapter.get_edges()
                freq = writer.write_edges(edges, write_properties=write_properties)
                edges_count.update(freq)

        except Exception as e:
            logger.error(f"Error processing adapter {adapter_name}: {str(e)}")
            continue

    return nodes_count, edges_count

@app.command()
def main(
    db_name: str,
    adapters_config: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
    dbsnp_rsids: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
    dbsnp_pos: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
    writer_type: str = typer.Option("mongodb", help="Choose writer type (Only MongoDB supported)"),
    write_properties: bool = typer.Option(True, help="Write properties to nodes and edges"),
    add_provenance: bool = typer.Option(True, help="Add provenance to nodes and edges"),
    clean_db: bool = typer.Option(False, help="Clear existing database before writing new data")
):
    """Generate a Knowledge Graph and store in MongoDB."""
    
    # Load required data files
    logger.info("Loading dbsnp rsids map")
    dbsnp_rsids_dict = pickle.load(open(dbsnp_rsids, 'rb'))
    logger.info("Loading dbsnp pos map")
    dbsnp_pos_dict = pickle.load(open(dbsnp_pos, 'rb'))

    # Initialize writer
    writer = get_writer(writer_type, db_name)
    logger.info(f"Using {writer_type} writer (MongoDB only)")

    # Clean database if requested
    if clean_db:
        writer.client.drop_database(db_name)
        logger.info(f"Cleared existing database {db_name}")

    # Load schema
    schema_dict = preprocess_schema()

    # Load adapters configuration
    try:
        with open(adapters_config, "r") as fp:
            adapters_dict = yaml.safe_load(fp)
    except yaml.YAMLError as e:
        logger.error("Error while trying to load adapter config")
        raise typer.Exit(code=1) from e

    # Process adapters and write to MongoDB
    nodes_count, edges_count = process_adapters(
        adapters_dict, 
        dbsnp_rsids_dict, 
        dbsnp_pos_dict, 
        writer, 
        write_properties, 
        add_provenance, 
        schema_dict
    )

    # Log results
    logger.info(f"Total nodes written to MongoDB: {sum(nodes_count.values())}")
    logger.info(f"Total edges written to MongoDB: {sum(edges_count.values())}")

    # Fetch and log collection stats
    fetch_from_mongodb(db_name)
    logger.info("Knowledge Graph stored in MongoDB successfully")

if __name__ == "__main__":
    app()