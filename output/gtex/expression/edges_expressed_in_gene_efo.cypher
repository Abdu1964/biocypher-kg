
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////mnt/c/Users/AbduM/Desktop/Abdu/iCog/Bio-cypher-KG/biocypher-kg/output/gtex/expression/edges_expressed_in_gene_efo.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:gene {id: row.source_id})
        MATCH (target:efo {id: row.target_id})
        MERGE (source)-[r:expressed_in]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
            