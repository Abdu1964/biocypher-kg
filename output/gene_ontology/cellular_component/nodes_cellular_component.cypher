
CREATE CONSTRAINT IF NOT EXISTS FOR (n:cellular_component) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////mnt/c/Users/AbduM/Desktop/Abdu/iCog/Bio-cypher-KG/biocypher-kg/output/gene_ontology/cellular_component/nodes_cellular_component.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MERGE (n:cellular_component {id: row.id})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {batchSize:1000, parallel:true, concurrency:4}
)
YIELD batches, total
RETURN batches, total;
                