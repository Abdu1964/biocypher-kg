
CREATE CONSTRAINT IF NOT EXISTS FOR (n:cell_type) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////home/eyorica/Downloads/bocypher/biocypher-kg/output_human/cell_ontology/nodes_cell_type.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:cell_type {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
