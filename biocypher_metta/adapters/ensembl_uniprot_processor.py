import requests
import pickle
import os
from typing import Dict
from datetime import datetime, timedelta
from Bio import SeqIO
import gzip

class EnsemblUniprotProcessor:
    def __init__(self, pickle_file_path: str = './aux_files/string_ensembl_uniprot_map.pkl',
                 version_file_path: str = './aux_files/string_ensembl_uniprot_version.txt'):
        self.pickle_file_path = pickle_file_path
        self.version_file_path = version_file_path
        self.ensembl_uniprot_map: Dict[str, str] = {}
        self.update_interval = timedelta(hours=168)  # Update weekly (UniProt releases ~monthly)
        self.last_update_check = None
        self.last_check_result = None
        self.uniprot_url = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_sprot_human.dat.gz"

    def check_update_needed(self) -> bool:
        """Check if we need to update the data based on the last update time"""
        current_time = datetime.now()

        # Check if we have a cached result from the last check
        if self.last_update_check and (current_time - self.last_update_check) < timedelta(hours=24):
            return self.last_check_result

        self.last_update_check = current_time

        if not os.path.exists(self.version_file_path):
            print("Ensembl-UniProt mapping: Version file not found. Update needed.")
            self.last_check_result = True
            return True

        with open(self.version_file_path, 'r') as f:
            last_update_str = f.read().strip()

        try:
            last_update = datetime.fromisoformat(last_update_str)
            time_since_update = current_time - last_update
            update_needed = time_since_update > self.update_interval

            if update_needed:
                print(f"Ensembl-UniProt mapping: Last updated {time_since_update.days} days ago. Update needed.")
            else:
                print(f"Ensembl-UniProt mapping: Last updated {time_since_update.days} days ago. No update needed.")

            self.last_check_result = update_needed
            return update_needed
        except ValueError:
            print("Ensembl-UniProt mapping: Invalid date format in version file. Forcing update.")
            self.last_check_result = True
            return True

    def save_update_time(self):
        """Save the current time as the last update time"""
        os.makedirs(os.path.dirname(self.version_file_path), exist_ok=True)
        current_time = datetime.now().isoformat()
        with open(self.version_file_path, 'w') as f:
            f.write(current_time)
        print(f"Ensembl-UniProt mapping: Saved update time: {current_time}")

    def update_mapping_data(self):
        """Update Ensembl to UniProt mapping data"""
        if not self.check_update_needed() and os.path.exists(self.pickle_file_path):
            print("Ensembl-UniProt mapping: Using existing data.")
            self.load_data()
            return

        print("Ensembl-UniProt mapping: Updating from UniProt...")

        try:
            response = requests.get(self.uniprot_url, stream=True, timeout=60)
            response.raise_for_status()

            ensembl_uniprot_ids = {}
            with gzip.open(response.raw, 'rt') as input_file:
                records = SeqIO.parse(input_file, 'swiss')
                for record in records:
                    dbxrefs = record.dbxrefs
                    for item in dbxrefs:
                        if item.startswith('STRING'):
                            try:
                                ensembl_id = item.split(':')[-1].split('.')[1] if '.' in item.split(':')[-1] else None
                                uniprot_id = record.id
                                if ensembl_id and uniprot_id:
                                    ensembl_uniprot_ids[ensembl_id] = uniprot_id
                            except (IndexError, AttributeError):
                                continue

            self.ensembl_uniprot_map = ensembl_uniprot_ids
            self.save_data()
            self.save_update_time()
            print(f"Ensembl-UniProt mapping: Updated with {len(ensembl_uniprot_ids)} mappings")

        except requests.exceptions.RequestException as e:
            print(f"Ensembl-UniProt mapping: Error occurred while fetching data: {e}")
            if os.path.exists(self.pickle_file_path):
                print("Ensembl-UniProt mapping: Using local database instead.")
                self.load_data()
            else:
                print("Ensembl-UniProt mapping: Local database not found. Cannot proceed without data.")
        except Exception as e:
            print(f"Ensembl-UniProt mapping: Error processing data: {e}")
            if os.path.exists(self.pickle_file_path):
                self.load_data()

    def save_data(self):
        """Save processed data to pickle file"""
        os.makedirs(os.path.dirname(self.pickle_file_path), exist_ok=True)
        with open(self.pickle_file_path, 'wb') as f:
            pickle.dump(self.ensembl_uniprot_map, f)
        print(f"Ensembl-UniProt mapping: Saved data to {self.pickle_file_path}")

    def load_data(self):
        """Load processed data from pickle file"""
        with open(self.pickle_file_path, 'rb') as f:
            self.ensembl_uniprot_map = pickle.load(f)
        print(f"Ensembl-UniProt mapping: Loaded data from {self.pickle_file_path}")

    def get_uniprot_id(self, ensembl_id: str) -> str:
        """Get UniProt ID for an Ensembl ID"""
        return self.ensembl_uniprot_map.get(ensembl_id)