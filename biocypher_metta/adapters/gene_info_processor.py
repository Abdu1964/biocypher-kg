import pickle
import os
import gzip
import csv
from typing import Dict
from datetime import datetime, timedelta

class GeneInfoProcessor:
    def __init__(self, gene_info_path: str = None, pickle_dir: str = None):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        if gene_info_path is None:
            gene_info_path = os.path.join(base_dir, 'aux_files', 'Homo_sapiens.gene_info.gz')
        if pickle_dir is None:
            pickle_dir = os.path.join(base_dir, 'aux_files')
            
        self.gene_info_path = os.path.abspath(gene_info_path)
        self.pickle_dir = os.path.abspath(pickle_dir)
        self.update_interval = timedelta(hours=168)  # Weekly updates (NCBI gene_info updates monthly)
        self.last_update_check = None
        self.last_check_result = None

        # Mapping file paths
        self.entrez_to_ensembl_path = os.path.join(self.pickle_dir, 'entrez_to_ensembl.pkl')
        self.ensembl_to_entrez_path = os.path.join(self.pickle_dir, 'ensembl_to_entrez.pkl')
        self.hgnc_to_ensembl_path = os.path.join(self.pickle_dir, 'hgnc_to_ensembl.pkl')
        self.ensembl_to_hgnc_path = os.path.join(self.pickle_dir, 'ensembl_to_hgnc.pkl')
        self.symbol_to_ensembl_path = os.path.join(self.pickle_dir, 'hgnc_symbol_map.pkl')
        self.ensembl_to_pos_path = os.path.join(self.pickle_dir, 'ensembl_to_pos.pkl')

        self.version_file_path = os.path.join(self.pickle_dir, 'gene_info_version.txt')

    def check_update_needed(self) -> bool:
        """Check if we need to update the data based on the last update time"""
        current_time = datetime.now()

        if self.last_update_check and (current_time - self.last_update_check) < timedelta(hours=24):
            return self.last_check_result

        self.last_update_check = current_time

        if not os.path.exists(self.version_file_path):
            print("Gene info mappings: Version file not found. Update needed.")
            self.last_check_result = True
            return True

        # Check if gene_info file exists
        if not os.path.exists(self.gene_info_path):
            print("Gene info mappings: Gene info file not found. Update needed.")
            self.last_check_result = True
            return True

        with open(self.version_file_path, 'r') as f:
            last_update_str = f.read().strip()

        try:
            last_update = datetime.fromisoformat(last_update_str)
            time_since_update = current_time - last_update
            update_needed = time_since_update > self.update_interval

            if update_needed:
                print(f"Gene info mappings: Last updated {time_since_update.days} days ago. Update needed.")
            else:
                print(f"Gene info mappings: Last updated {time_since_update.days} days ago. No update needed.")

            self.last_check_result = update_needed
            return update_needed
        except ValueError:
            print("Gene info mappings: Invalid date format in version file. Forcing update.")
            self.last_check_result = True
            return True

    def save_update_time(self):
        """Save the current time as the last update time"""
        os.makedirs(os.path.dirname(self.version_file_path), exist_ok=True)
        current_time = datetime.now().isoformat()
        with open(self.version_file_path, 'w') as f:
            f.write(current_time)
        print(f"Gene info mappings: Saved update time: {current_time}")

    def update_mapping_data(self):
        """Update gene info derived mappings"""
        if not os.path.exists(self.gene_info_path):
            print(f"Gene info mappings: Gene info file not found at {self.gene_info_path}")
            return

        if not self.check_update_needed() and os.path.exists(self.entrez_to_ensembl_path):
            print("Gene info mappings: Using existing data.")
            return

        print("Gene info mappings: Updating from gene_info file...")

        try:
            entrez_to_ensembl = {}
            ensembl_to_entrez = {}
            hgnc_to_ensembl = {}
            ensembl_to_hgnc = {}
            symbol_to_ensembl = {}
            ensembl_to_symbol = {}
            ensembl_to_pos = {}

            with gzip.open(self.gene_info_path, 'rt') as tsv_file:
                reader = csv.DictReader(tsv_file, delimiter='\t')
                for row in reader:
                    gene_id = row.get('GeneID')
                    symbol = row.get('Symbol')
                    dbxrefs = row.get('dbXrefs', '').split('|')
                    chromosome = row.get('chromosome')
                    start_pos = row.get('start_position_on_the_genomic_accession')
                    end_pos = row.get('end_position_on_the_genomic_accession')

                    ensembl_id = None
                    for xref in dbxrefs:
                        if xref.startswith('Ensembl:'):
                            ensembl_id = xref.split(':')[1]
                            break

                    hgnc_id = None
                    for xref in dbxrefs:
                        if xref.startswith('HGNC:'):
                            hgnc_id = xref.split(':')[1]
                            break

                    if ensembl_id:
                        if gene_id:
                            entrez_to_ensembl[gene_id] = ensembl_id
                            ensembl_to_entrez[ensembl_id] = gene_id

                        if hgnc_id:
                            hgnc_to_ensembl[hgnc_id] = ensembl_id
                            ensembl_to_hgnc[ensembl_id] = hgnc_id

                        if symbol:
                            symbol_to_ensembl[symbol] = ensembl_id
                            ensembl_to_symbol[ensembl_id] = symbol

                        if chromosome and start_pos and end_pos:
                            try:
                                ensembl_to_pos[ensembl_id] = {
                                    'chr': chromosome,
                                    'start': int(start_pos),
                                    'end': int(end_pos)
                                }
                            except ValueError:
                                pass  # Skip if positions are not numeric

          
            mappings = {
                self.entrez_to_ensembl_path: entrez_to_ensembl,
                self.ensembl_to_entrez_path: ensembl_to_entrez,
                self.hgnc_to_ensembl_path: hgnc_to_ensembl,
                self.ensembl_to_hgnc_path: ensembl_to_hgnc,
                self.symbol_to_ensembl_path: symbol_to_ensembl,
                self.ensembl_to_pos_path: ensembl_to_pos,
            }

            os.makedirs(self.pickle_dir, exist_ok=True)
            for filepath, data in mappings.items():
                with open(filepath, 'wb') as f:
                    pickle.dump(data, f)
                print(f"Gene info mappings: Updated {filepath} with {len(data)} mappings")

            self.save_update_time()
            print("Gene info mappings: All mappings updated successfully")

        except Exception as e:
            print(f"Gene info mappings: Error processing data: {e}")

    def load_mappings(self):
        """Load all mappings from pickle files"""
        mappings = {}
        mapping_files = [
            ('entrez_to_ensembl', self.entrez_to_ensembl_path),
            ('ensembl_to_entrez', self.ensembl_to_entrez_path),
            ('hgnc_to_ensembl', self.hgnc_to_ensembl_path),
            ('ensembl_to_hgnc', self.ensembl_to_hgnc_path),
            ('symbol_to_ensembl', self.symbol_to_ensembl_path),
            ('ensembl_to_pos', self.ensembl_to_pos_path),
        ]

        for name, filepath in mapping_files:
            if os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    mappings[name] = pickle.load(f)
            else:
                mappings[name] = {}

        return mappings