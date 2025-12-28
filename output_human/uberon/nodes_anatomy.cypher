
CREATE CONSTRAINT IF NOT EXISTS FOR (n:anatomy) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////home/eyorica/Downloads/bocypher/biocypher-kg/output_human/uberon/nodes_anatomy.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:anatomy {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
