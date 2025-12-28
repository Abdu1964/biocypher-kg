#!/bin/bash
# Script to generate knowledge graph from GTF file using GenericDataAdapter

cd "$(dirname "$0")"

# Use sample dbSNP files (not needed for GTF but required by script)
DBSNP_RSIDS="${DBSNP_RSIDS:-aux_files/sample_dbsnp_rsids.pkl}"
DBSNP_POS="${DBSNP_POS:-aux_files/sample_dbsnp_pos.pkl}"

# Run the knowledge graph generation
echo "🚀 Generating knowledge graph from GTF file..."
echo "📄 Input: samples/gencode_sample.gtf.gz"
echo "📁 Output: output_gencode_gtf/"
echo ""

uv run python create_knowledge_graph.py \
    --dbsnp-rsids "$DBSNP_RSIDS" \
    --dbsnp-pos "$DBSNP_POS" \
    --adapters-config config/user_adapters_gencode_gtf.yaml \
    --output-dir output_gencode_gtf \
    --writer-type neo4j \
    --write-properties \
    --add-provenance \
    --species hsa

echo ""
echo "✅ Knowledge graph generation complete!"
echo "📁 Output directory: output_gencode_gtf/"

