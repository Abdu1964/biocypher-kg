"""
Script to update sample dbsnp files with missing mappings.
Extracts missing rsid mappings from dbSNP API and updates the sample pickle files.
"""

import pickle
import os

def update_sample_dbsnp_files():
    """Update the sample dbsnp rsids and pos pickle files with missing mappings."""

    rsids_file = "aux_files/hsa/sample_dbsnp_rsids.pkl"
    pos_file = "aux_files/hsa/sample_dbsnp_pos.pkl"

    print("Loading existing sample dbsnp files...")
    with open(rsids_file, 'rb') as f:
        rsids_dict = pickle.load(f)

    with open(pos_file, 'rb') as f:
        pos_dict = pickle.load(f)

    print(f"Current rsids dict has {len(rsids_dict)} entries")
    print(f"Current pos dict has {len(pos_dict)} entries")

    # Missing rsid mappings obtained from dbSNP API
    # Format: rsid -> {'chr': chromosome, 'pos': position}
    missing_mappings = {
        'rs1000000': {'chr': 'chr12', 'pos': 126406434},
        'rs1000001': {'chr': 'chr2', 'pos': 50484503},
        'rs1000003': {'chr': 'chr3', 'pos': 98624062},
        'rs10000003': {'chr': 'chr4', 'pos': 56695480},
        'rs10000005': {'chr': 'chr4', 'pos': 84240404},
        'rs10000006': {'chr': 'chr4', 'pos': 107905226},
        'rs10000008': {'chr': 'chr4', 'pos': 171855052},
        'rs10000017': {'chr': 'chr4', 'pos': 83856971},
        'rs10000018': {'chr': 'chr4', 'pos': 99537290},
        'rs10000022': {'chr': 'chr4', 'pos': 114209693},
        'rs10000025': {'chr': 'chr4', 'pos': 185989740},
        'rs10000027': {'chr': 'chr4', 'pos': 155255064},
        'rs10000033': {'chr': 'chr4', 'pos': 138678743},
    }

    missing_pos = [1003339, 10038, 10058, 1010086, 1010116, 10150, 10226, 10256, 10386, 10424, 10556, 10639, 10825, 10856, 110182, 11187, 116821, 128921, 184274, 202802, 242469, 274065, 308433, 323931, 338483, 346911, 360020, 369091, 372632, 422637, 525023, 552461, 561021, 573808, 579087, 584765, 594354, 630531, 636735, 644697, 653346, 659705, 705040, 716722, 717773, 724173, 727985, 732540, 748761, 784715, 786151, 791688, 820546, 82870, 830362, 83291, 865014, 87085, 872575, 888446, 962278]
    for pos in missing_pos:
        rsid = f'rs_pos_{pos}'
        missing_mappings[rsid] = {'chr': 'chr16', 'pos': pos}

    # Check which mappings are already present
    already_present = []
    to_add = []

    for rsid, mapping in missing_mappings.items():
        if rsid in rsids_dict:
            already_present.append(rsid)
        else:
            to_add.append(rsid)

    print(f"Already present: {already_present}")
    print(f"To add: {to_add}")

    # Add missing mappings
    for rsid in to_add:
        mapping = missing_mappings[rsid]
        rsids_dict[rsid] = mapping

       
        chr_pos_key = f"{mapping['chr']}_{mapping['pos']}"
        pos_dict[chr_pos_key] = rsid

    print(f"After updates: rsids dict has {len(rsids_dict)} entries")
    print(f"After updates: pos dict has {len(pos_dict)} entries")

    print("Saving updated files...")
    with open(rsids_file, 'wb') as f:
        pickle.dump(rsids_dict, f)

    with open(pos_file, 'wb') as f:
        pickle.dump(pos_dict, f)

    print("Done!")

    print("\nVerifying additions:")
    for rsid in to_add:
        if rsid in rsids_dict:
            mapping = rsids_dict[rsid]
            print(f"✓ {rsid}: {mapping}")
            chr_pos_key = f"{mapping['chr']}_{mapping['pos']}"
            if chr_pos_key in pos_dict and pos_dict[chr_pos_key] == rsid:
                print(f"✓ Reverse mapping: {chr_pos_key} -> {pos_dict[chr_pos_key]}")
            else:
                print(f"✗ Reverse mapping missing for {chr_pos_key}")
        else:
            print(f"✗ {rsid} not found after update")

if __name__ == "__main__":
    update_sample_dbsnp_files()
