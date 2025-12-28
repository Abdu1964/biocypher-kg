
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////home/eyorica/Downloads/bocypher/biocypher-kg/output_human/gencode/gene/edges_transcribes_to_gene_transcript.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:gene {id: row.source_id})
    MATCH (target:transcript {id: row.target_id})
    MERGE (source)-[r:transcribes_to]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
