#!/usr/bin/env python3
"""
Automated Mapping Updates Script

This script updates all biological identifier mapping files used by BioCypher adapters.
It follows the same pattern as HGNCSymbolProcessor for automated updates.

Usage:
    python update_mappings.py [--force] [--mapping-type TYPE]

Options:
    --force: Force update all mappings regardless of timestamps
    --mapping-type: Update only specific mapping type (hgnc, ensembl_uniprot, gene_info)
"""

import argparse
import sys
import os
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from biocypher_metta.adapters.hgnc_processor import HGNCSymbolProcessor
from biocypher_metta.adapters.ensembl_uniprot_processor import EnsemblUniprotProcessor
from biocypher_metta.adapters.gene_info_processor import GeneInfoProcessor

def update_hgnc_mappings(force=False):
    """Update HGNC symbol mappings"""
    print("\n" + "="*50)
    print("UPDATING HGNC MAPPINGS")
    print("="*50)

    processor = HGNCSymbolProcessor(
        pickle_file_path='aux_files/hgnc_complete_symbol_map.pkl',
        version_file_path='aux_files/hgnc_version.txt'
    )

    if force:
        processor.update_hgnc_data()
    else:
        processor.update_hgnc_data() 

    return True

def update_ensembl_uniprot_mappings(force=False):
    """Update Ensembl to UniProt mappings"""
    print("\n" + "="*50)
    print("UPDATING ENSEMBL-UNIPROT MAPPINGS")
    print("="*50)

    processor = EnsemblUniprotProcessor(
        pickle_file_path='aux_files/string_ensembl_uniprot_map.pkl',
        version_file_path='aux_files/string_ensembl_uniprot_version.txt'
    )

    if force:
        # Force update by removing version file
        if os.path.exists(processor.version_file_path):
            os.remove(processor.version_file_path)
        processor.update_mapping_data()
    else:
        processor.update_mapping_data()

    return True

def update_gene_info_mappings(force=False):
    """Update gene info derived mappings"""
    print("\n" + "="*50)
    print("UPDATING GENE INFO MAPPINGS")
    print("="*50)

    processor = GeneInfoProcessor()

    if force:
        # Force update by removing version file
        if os.path.exists(processor.version_file_path):
            os.remove(processor.version_file_path)
        processor.update_mapping_data()
    else:
        processor.update_mapping_data()

    return True

def main():
    parser = argparse.ArgumentParser(description='Update biological identifier mappings')
    parser.add_argument('--force', action='store_true',
                       help='Force update all mappings regardless of timestamps')
    parser.add_argument('--mapping-type', choices=['hgnc', 'ensembl_uniprot', 'gene_info'],
                       help='Update only specific mapping type')

    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    os.chdir(project_root)

    success = True

    try:
        if args.mapping_type == 'hgnc':
            success &= update_hgnc_mappings(args.force)
        elif args.mapping_type == 'ensembl_uniprot':
            success &= update_ensembl_uniprot_mappings(args.force)
        elif args.mapping_type == 'gene_info':
            success &= update_gene_info_mappings(args.force)
        else:
            # Update all mappings
            success &= update_hgnc_mappings(args.force)
            success &= update_ensembl_uniprot_mappings(args.force)
            success &= update_gene_info_mappings(args.force)

        if success:
            print("\n" + "="*50)
            print("✅ ALL MAPPING UPDATES COMPLETED SUCCESSFULLY")
            print("="*50)
        else:
            print("\n" + "="*50)
            print("❌ SOME MAPPING UPDATES FAILED")
            print("="*50)
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()