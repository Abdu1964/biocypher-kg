from collections import Counter, defaultdict
import json
from biocypher._logger import logger
import rdflib
from pathlib import Path
from biocypher_metta import BaseWriter
from pymongo import MongoClient
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv() 

class MongodbWriter(BaseWriter):
    def __init__(self, schema_config, biocypher_config, output_dir):
        super().__init__(schema_config, biocypher_config, output_dir)
        mongo_uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(mongo_uri)
        self.db = self.client[str(output_dir)]

        self.csv_delimiter = '|'
        self.array_delimiter = ';'
        self.translation_table = str.maketrans({
            self.csv_delimiter: '', 
            self.array_delimiter: ' ', 
            "'": "",
            '"': ""
        })
        self.ontologies = set(['go', 'bto', 'efo', 'cl', 'clo', 'uberon'])
        self.create_edge_types()
        self.batch_size = 10000

    def create_edge_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                edge_type = self.convert_input_labels(k)
                source_type = v.get("source", None)
                target_type = v.get("target", None)

                if source_type is not None and target_type is not None:
                    if isinstance(v["input_label"], list):
                        label = self.convert_input_labels(v["input_label"][0])
                        source_type = self.convert_input_labels(source_type[0])
                        target_type = self.convert_input_labels(target_type[0])
                    else:
                        label = self.convert_input_labels(v["input_label"])
                        source_type = self.convert_input_labels(source_type)
                        target_type = self.convert_input_labels(target_type)
                    output_label = v.get("output_label", label)

                    self.edge_node_types[label.lower()] = {
                        "source": source_type.lower(),
                        "target": target_type.lower(),
                        "output_label": output_label.lower()
                    }

    def preprocess_value(self, value):
        value_type = type(value)
        if value_type is list:
            return json.dumps([self.preprocess_value(item) for item in value])
        if value_type is rdflib.term.Literal:
            return str(value).translate(self.translation_table)
        if value_type is str:
            return value.translate(self.translation_table)
        return value

    def convert_input_labels(self, label):
        return label.lower().replace(" ", "_")

    def preprocess_id(self, prev_id):
        replace_map = str.maketrans({' ': '_', ':':'_'})
        return prev_id.lower().strip().translate(replace_map)

    def insert_mongodb_batch(self, collection_name, data_batch):
        """Insert a batch of data into MongoDB collection"""
        if data_batch:
            collection = self.db[collection_name]
            collection.insert_many(data_batch)
            logger.info(f"Inserted {len(data_batch)} documents into MongoDB collection: {collection_name}")

    def write_nodes(self, nodes, path_prefix=None, adapter_name=None, write_properties=True):
        node_batches = defaultdict(list)
        node_freq = defaultdict(int)
        
        try:
            logger.info(f"Processing nodes for MongoDB insertion... (Adapter: {adapter_name})")
            
            for node in nodes:
                try:
                    id, label, properties = node
                    if "." in label:
                        label = label.split(".")[1]
                    label = label.lower()
                    node_freq[label] += 1
                    
                    node_data = {'id': self.preprocess_id(id)}
                    
                    # Only include properties if write_properties is True
                    if write_properties:
                        for key, value in properties.items():
                            node_data[key] = self.preprocess_value(value)
                    
                    node_batches[label].append(node_data)
                    
                    if len(node_batches[label]) >= self.batch_size:
                        self.insert_mongodb_batch(f"nodes_{label}", node_batches[label])
                        node_batches[label] = []
                
                except Exception as node_error:
                    logger.error(f"Skipping node due to error: {str(node_error)}")
                    continue  # Skip this node and move to the next

            # Insert any remaining nodes in the batch
            for label, batch in node_batches.items():
                if batch:
                    self.insert_mongodb_batch(f"nodes_{label}", batch)
            
            logger.info(f"Finished inserting nodes for adapter {adapter_name}. Node counts: {dict(node_freq)}")

        except Exception as e:
            logger.error(f"Error processing nodes for adapter {adapter_name}: {str(e)}. Skipping adapter.")
            return {}, {}

        return node_freq, {}

    def write_edges(self, edges, path_prefix=None, adapter_name=None, write_properties=True):
        edge_batches = defaultdict(list)
        edge_freq = defaultdict(int)

        try:
            logger.info(f"Processing edges for MongoDB insertion... (Adapter: {adapter_name})")

            for edge in edges:
                try:
                    source_id, target_id, label, properties = edge
                    label = label.lower()
                    edge_freq[label] += 1

                    edge_info = self.edge_node_types.get(label)
                    if not edge_info:
                        logger.warning(f"Skipping edge type {label} as it is not found in schema.")
                        continue
                    
                    source_type = edge_info["source"]
                    target_type = edge_info["target"]
                    
                    if source_type == "ontology_term":
                        source_type = self.preprocess_id(source_id).split('_')[0]
                    if target_type == "ontology_term":
                        target_type = self.preprocess_id(target_id).split('_')[0]
                    
                    edge_label = edge_info.get("output_label", label)
                    
                    edge_data = {
                        'source_id': self.preprocess_id(source_id),
                        'target_id': self.preprocess_id(target_id),
                        'source_type': source_type,
                        'target_type': target_type,
                        'label': edge_label
                    }
                    
                    # Only include properties if write_properties is True
                    if write_properties:
                        for key, value in properties.items():
                            edge_data[key] = self.preprocess_value(value)
                    
                    edge_batches[edge_label].append(edge_data)
                    
                    if len(edge_batches[edge_label]) >= self.batch_size:
                        self.insert_mongodb_batch(f"edges_{edge_label}", edge_batches[edge_label])
                        edge_batches[edge_label] = []
                
                except Exception as edge_error:
                    logger.error(f"Skipping edge due to error: {str(edge_error)}")
                    continue  # Skip this edge and continue processing

            # Insert any remaining edges in the batch
            for edge_label, batch in edge_batches.items():
                if batch:
                    self.insert_mongodb_batch(f"edges_{edge_label}", batch)
            
            logger.info(f"Finished inserting edges for adapter {adapter_name}. Edge counts: {dict(edge_freq)}")

        except Exception as e:
            logger.error(f"Error processing edges for adapter {adapter_name}: {str(e)}. Skipping adapter.")
            return {}

        return edge_freq

    def get_output_path(self, prefix=None, adapter_name=None):
        # Maintained for compatibility but not used for MongoDB storage
        if prefix:
            output_dir = self.output_path / prefix
        elif adapter_name:
            output_dir = self.output_path / adapter_name
        else:
            output_dir = self.output_path
            
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir