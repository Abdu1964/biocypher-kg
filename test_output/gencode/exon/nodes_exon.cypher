
CREATE CONSTRAINT IF NOT EXISTS FOR (n:exon) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////home/eyorica/Downloads/bocypher/biocypher-kg/test_output/gencode/exon/nodes_exon.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:exon {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
