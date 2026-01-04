
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////home/eyorica/Downloads/bocypher/biocypher-kg/output_human_mini/cell_line_ontology/edges_clo_subclass_of_cell_line_cell_line.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:cell_line {id: row.source_id})
    MATCH (target:cell_line {id: row.target_id})
    MERGE (source)-[r:is_a]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
