import json
import csv
from collections import defaultdict, Counter
from pathlib import Path
import rdflib
from biocypher_metta import BaseWriter
from biocypher._logger import logger

class Neo4jCSVWriter(BaseWriter):
    def __init__(self, schema_config, biocypher_config, output_dir):
        super().__init__(schema_config, biocypher_config, output_dir)
        self.csv_delimiter = '|'
        self.array_delimiter = ';'
        self.create_edge_types()

        self.excluded_properties = []
        self.translation_table = str.maketrans({
            self.csv_delimiter: '',
            self.array_delimiter: ' ',
            "'": "",
            '"': ""
        })
        self.ontologies = set(['go', 'bto', 'efo', 'cl', 'clo', 'uberon'])

    def create_edge_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                edge_type = self.convert_input_labels(k)
                source_type = v.get("source", None)
                target_type = v.get("target", None)

                if source_type and target_type:
                    label = self.convert_input_labels(v["input_label"] if isinstance(v["input_label"], str) else v["input_label"][0])
                    source_type = self.convert_input_labels(source_type[0] if isinstance(source_type, list) else source_type)
                    target_type = self.convert_input_labels(target_type[0] if isinstance(target_type, list) else target_type)
                    output_label = v.get("output_label", None)

                    self.edge_node_types[label.lower()] = {
                        "source": source_type.lower(),
                        "target": target_type.lower(),
                        "output_label": output_label.lower() if output_label else None
                    }

    def preprocess_value(self, value):
        """Preprocess the values to clean and format them."""
        if isinstance(value, list):
            return json.dumps([self.preprocess_value(item) for item in value])
        
        if isinstance(value, rdflib.term.Literal):
            return str(value).translate(self.translation_table)
        
        if isinstance(value, str):
            return value.translate(self.translation_table)
        
        return value

    def convert_input_labels(self, label):
        """Convert input labels to a standard format."""
        return label.lower().replace(" ", "_")

    def preprocess_id(self, prev_id):
        """Preprocess and standardize IDs."""
        replace_map = str.maketrans({' ': '_', ':':'_'})
        return prev_id.lower().strip().translate(replace_map)

    def write_chunk(self, chunk, headers, file_path, csv_delimiter):
        """Write data chunks to a CSV file."""
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=csv_delimiter)
            for row in chunk:
                processed_row = [self.preprocess_value(row.get(header, '')) for header in headers]
                writer.writerow(processed_row)
            csvfile.flush()

    def write_to_csv(self, data, file_path, chunk_size=1000):
        """Write data to a CSV file in chunks to reduce memory usage."""
        headers = set()
        for entry in data:
            headers.update(entry.keys())
    
        headers = sorted(list(headers))  
        if 'id' in headers:
            headers.remove('id')
            headers = ['id'] + headers 
    
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=self.csv_delimiter)
            writer.writerow(headers)
            csvfile.flush() 

        # Writing data in chunks to reduce memory usage
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i+chunk_size]
            self.write_chunk(chunk, headers, file_path, self.csv_delimiter)

    def write_nodes(self, nodes, path_prefix=None, adapter_name=None):
        """Write node data to CSV and generate Cypher queries."""
        output_dir = self.output_path / (path_prefix or adapter_name or "")
        output_dir.mkdir(parents=True, exist_ok=True)

        node_groups = defaultdict(list)
        node_freq = Counter()
        node_props = defaultdict(set)

        for node in nodes:
            id, label, properties = node
            if "." in label:
                label = label.split(".")[1]
            label = label.lower()
            node_freq[label] += 1
            node_props[label] = node_props[label].union(properties.keys())
            node_groups[label].append({'id': self.preprocess_id(id), 'label': label, **properties})

        # Write node data to CSV and generate Cypher queries
        for label, node_data in node_groups.items():
            csv_file_path = output_dir / f"nodes_{label}.csv"
            cypher_file_path = output_dir / f"nodes_{label}.cypher"
            self.write_to_csv(node_data, csv_file_path)

            absolute_path = csv_file_path.resolve().as_posix()
            additional_label = ":ontology_term" if label in self.ontologies else ""
            with open(cypher_file_path, 'w') as f:
                cypher_query = f"""
CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
    "MERGE (n:{label}{additional_label} {{id: row.id}})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {{batchSize:1000, parallel:true, concurrency:4}}
)
YIELD batches, total
RETURN batches, total;
                """
                f.write(cypher_query)

        logger.info(f"Finished writing out all node import queries for: {output_dir}")
        return node_freq, node_props

    def write_edges(self, edges, path_prefix=None, adapter_name=None):
        """Write edge data to CSV and generate Cypher queries."""
        output_dir = self.output_path / (path_prefix or adapter_name or "")
        output_dir.mkdir(parents=True, exist_ok=True)

        edge_groups = defaultdict(list)
        edges_freq = Counter()

        for edge in edges:
            source_id, target_id, label, properties = edge
            label = label.lower()
            edges_freq[label] += 1

            source_type = self.edge_node_types[label]["source"]
            target_type = self.edge_node_types[label]["target"]
            if source_type == "ontology_term":
                source_type = self.preprocess_id(source_id).split('_')[0]
            if target_type == "ontology_term":
                target_type = self.preprocess_id(target_id).split('_')[0]

            output_label = self.edge_node_types[label]["output_label"] or label

            key = (label, source_type, target_type)
            edge_groups[key].append({
                'source_type': source_type,
                'source_id': self.preprocess_id(source_id),
                'target_type': target_type,
                'target_id': self.preprocess_id(target_id),
                'label': output_label,
                **properties
            })

        # Write edges data in chunks
        for (label, source_type, target_type), edge_data in edge_groups.items():
            file_suffix = f"{label}_{source_type}_{target_type}".lower()
            csv_file_path = output_dir / f"edges_{file_suffix}.csv"
            cypher_file_path = output_dir / f"edges_{file_suffix}.cypher"

            self.write_to_csv(edge_data, csv_file_path)

            absolute_path = csv_file_path.resolve().as_posix()
            with open(cypher_file_path, 'w') as f:
                cypher_query = f"""
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
        "MATCH (source:{source_type} {{id: row.source_id}})
        MATCH (target:{target_type} {{id: row.target_id}})
        MERGE (source)-[r:{label}]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {{batchSize:1000}}
    )
    YIELD batches, total
    RETURN batches, total;
                """
                f.write(cypher_query)

        logger.info(f"Finished writing out all edge import queries for: {output_dir}")
        return edges_freq
