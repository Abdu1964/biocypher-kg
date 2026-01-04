
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////home/eyorica/Downloads/bocypher/biocypher-kg/output_human_mini/uberon/edges_uberon_subclass_of_anatomy_anatomy.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:anatomy {id: row.source_id})
    MATCH (target:anatomy {id: row.target_id})
    MERGE (source)-[r:is_a]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
