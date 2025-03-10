
    CALL apoc.periodic.iterate(
        "LOAD CSV WITH HEADERS FROM 'file:////mnt/c/Users/AbduM/Desktop/Abdu/iCog/Bio-cypher-KG/biocypher-kg/output/uniprot/edges_translation_of_protein_transcript.csv' AS row FIELDTERMINATOR '|' RETURN row",
        "MATCH (source:protein {id: row.source_id})
        MATCH (target:transcript {id: row.target_id})
        MERGE (source)-[r:translation_of]->(target)
        SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
        {batchSize:1000}
    )
    YIELD batches, total
    RETURN batches, total;
                