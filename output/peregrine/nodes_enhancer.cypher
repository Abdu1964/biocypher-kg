
CREATE CONSTRAINT IF NOT EXISTS FOR (n:enhancer) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////mnt/c/Users/AbduM/Desktop/Abdu/iCog/Bio-cypher-KG/biocypher-kg/output/peregrine/nodes_enhancer.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:enhancer {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
                