import csv
import gzip
import pickle
from pathlib import Path
from collections import defaultdict, Counter

from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location

# Human data:
# https://epd.expasy.org/ftp/epdnew/H_sapiens/

# Example EPD bed input file:
##CHRM Start  End   Id  Score Strand -  -
# chr1 959245 959305 NOC2L_1 900 - 959245 959256
# chr1 960583 960643 KLHL17_1 900 + 960632 960643
# chr1 966432 966492 PLEKHN1_1 900 + 966481 966492
# chr1 976670 976730 PERM1_1 900 - 976670 976681


# Fly data:
# https://epd.expasy.org/ftp/epdnew/D_melanogaster/

# Mouse data:
# https://epd.expasy.org/ftp/epdnew/M_musculus/ 

# Rat data:
# https://epd.expasy.org/ftp/epdnew/R_norvegicus/


class EPDAdapter(Adapter):
    INDEX = {'chr' : 0, 'coord_start' : 1, 'coord_end' : 2, 'gene_id' : 3}

    def __init__(
        self,
        filepath,
        hgnc_to_ensembl_map=None,
        write_properties=False,
        add_provenance=False,
        taxon_id=None,
        label=None,
        type='promoter',
        delimiter=' ',
        chr=None,
        start=None,
        end=None,
        flybase_gene_info_path=None,
        strict_gene_symbol_check=False,
    ):
        self.filepath = filepath
        self.hgnc_to_ensembl_map = None
        if hgnc_to_ensembl_map:
            self.hgnc_to_ensembl_map = pickle.load(open(hgnc_to_ensembl_map, 'rb'))
        self.type = type
        if label is None:
            # Matches config/*/schema_config.yaml: promoter nodes use input_label "promoter",
            # promoter→gene edges use input_label "promoter_gene".
            label = 'promoter_gene' if str(type).lower() != 'promoter' else 'promoter'
        self.label = label
        self.delimiter = delimiter
        self.chr = chr
        self.start = start
        self.end = end
        self.taxon_id = taxon_id
        self.strict_gene_symbol_check = strict_gene_symbol_check
        self.source = 'EPD'
        self.version = '006'
        self.source_url = 'https://epd.expasy.org/ftp/epdnew/H_sapiens/'

        self.flybase_symbol_to_fbgn = None
        if self._is_dmel_taxon_id(self.taxon_id):
            self.source_url = 'https://epd.expasy.org/ftp/epdnew/D_melanogaster/current/Dm_EPDnew.bed'
            if flybase_gene_info_path is None:
                repo_root = Path(__file__).resolve().parents[2]
                flybase_gene_info_path = repo_root / 'aux_files/dmel/Drosophila_melanogaster.gene_info.gz'
            (
                self.flybase_primary_symbol_to_fbgn,
                self.flybase_symbol_to_fbgn,
                self.flybase_ambiguous_symbols,
                self.flybase_upper_primary_symbol_to_fbgn,
                self.flybase_upper_symbol_to_fbgn,
                self.flybase_upper_ambiguous_symbols,
            ) = self._load_flybase_symbol_to_fbgn(flybase_gene_info_path)

        super(EPDAdapter, self).__init__(write_properties, add_provenance)

    @staticmethod
    def _is_dmel_taxon_id(taxon_id) -> bool:
        return str(taxon_id).strip() == '7227'

    @staticmethod
    def _load_flybase_symbol_to_fbgn(gene_info_gz_path):
        """Load a mapping of FlyBase gene symbols to FBgn IDs from NCBI gene_info.gz.

        Expected file: Drosophila_melanogaster.gene_info.gz (tab-delimited, header starts with '#').
        """
        gene_info_gz_path = Path(gene_info_gz_path)
        if not gene_info_gz_path.exists():
            raise FileNotFoundError(
                f"FlyBase gene_info.gz not found at: {gene_info_gz_path}. "
                "Provide flybase_gene_info_path pointing to Drosophila_melanogaster.gene_info.gz."
            )

        primary_symbol_to_fbgn = {}
        synonym_to_fbgns = defaultdict(set)
        ambiguous = {}
        with gzip.open(gene_info_gz_path, 'rt') as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter='\t')
            for row in reader:
                tax_key = 'tax_id' if 'tax_id' in row else '#tax_id'
                if str(row.get(tax_key, '')).strip() != '7227':
                    continue

                symbol = (row.get('Symbol') or '').strip()
                if not symbol or symbol == '-':
                    continue

                synonyms_raw = (row.get('Synonyms') or '').strip()
                synonyms = []
                if synonyms_raw and synonyms_raw != '-':
                    synonyms = [s.strip() for s in synonyms_raw.split('|') if s.strip()]

                dbxrefs = (row.get('dbXrefs') or '').split('|')
                fbgn = next(
                    (xref.split(':', 1)[1] for xref in dbxrefs if xref.startswith('FLYBASE:FBgn')),
                    None,
                )
                if not fbgn:
                    continue

                existing_primary = primary_symbol_to_fbgn.get(symbol)
                if existing_primary is None:
                    primary_symbol_to_fbgn[symbol] = fbgn
                elif existing_primary != fbgn:
                    # Extremely rare, but keep track just in case.
                    ambiguous.setdefault(symbol, set()).update({existing_primary, fbgn})

                for token in synonyms:
                    synonym_to_fbgns[token].add(fbgn)

        # Deterministic resolution rule:
        # - If an EPD token equals an official FlyBase Symbol, always use that FBgn.
        # - Otherwise, allow synonym matches only when the synonym maps uniquely.
        symbol_to_fbgn = dict(primary_symbol_to_fbgn)
        for token, fbgns in synonym_to_fbgns.items():
            if token in primary_symbol_to_fbgn:
                continue
            if len(fbgns) == 1:
                symbol_to_fbgn[token] = next(iter(fbgns))
            else:
                ambiguous.setdefault(token, set()).update(fbgns)

        # Build uppercase lookup tables for case-insensitive fallback, but only
        # use them when they resolve uniquely.
        upper_primary = {}
        upper_primary_ambiguous = defaultdict(set)
        for token, fbgn in primary_symbol_to_fbgn.items():
            key = token.upper()
            existing = upper_primary.get(key)
            if existing is None:
                upper_primary[key] = fbgn
            elif existing != fbgn:
                upper_primary_ambiguous[key].update({existing, fbgn})

        upper_any = {}
        upper_any_ambiguous = defaultdict(set)
        for token, fbgn in symbol_to_fbgn.items():
            key = token.upper()
            existing = upper_any.get(key)
            if existing is None:
                upper_any[key] = fbgn
            elif existing != fbgn:
                upper_any_ambiguous[key].update({existing, fbgn})

        # Merge ambiguous sets into stable lists
        ambiguous = {k: sorted(v) for k, v in ambiguous.items()}
        upper_primary_ambiguous = {k: sorted(v) for k, v in upper_primary_ambiguous.items()}
        upper_any_ambiguous = {k: sorted(v) for k, v in upper_any_ambiguous.items()}
        return (
            dict(primary_symbol_to_fbgn),
            symbol_to_fbgn,
            ambiguous,
            upper_primary,
            upper_any,
            {**upper_primary_ambiguous, **upper_any_ambiguous},
        )

    def _iter_epd_bed_rows(self):
        opener = gzip.open if str(self.filepath).endswith('.gz') else open
        with opener(self.filepath, 'rt') as f:
            for raw_line in f:
                raw_line = raw_line.strip()
                if not raw_line or raw_line.startswith('#'):
                    continue

                
                if self.delimiter and self.delimiter not in (' ', '\t'):
                    parts = [p for p in raw_line.split(self.delimiter) if p != '']
                else:
                    parts = raw_line.split()

                if len(parts) <= EPDAdapter.INDEX['gene_id']:
                    continue
                yield parts

    def _resolve_gene_curie_from_epd_token(self, epd_gene_token):
        gene_symbol = (epd_gene_token or '').split('_', 1)[0].strip()
        if not gene_symbol:
            return None, gene_symbol

        if self._is_dmel_taxon_id(self.taxon_id):
            fbgn = (self.flybase_primary_symbol_to_fbgn or {}).get(gene_symbol)
            if fbgn:
                return f"FlyBase:{fbgn}", gene_symbol

            if gene_symbol in (self.flybase_ambiguous_symbols or {}):
                return None, gene_symbol

            fbgn = (self.flybase_symbol_to_fbgn or {}).get(gene_symbol)
            if fbgn:
                return f"FlyBase:{fbgn}", gene_symbol

            upper = gene_symbol.upper()
            if upper in (self.flybase_upper_ambiguous_symbols or {}):
                return None, gene_symbol
            fbgn = (self.flybase_upper_primary_symbol_to_fbgn or {}).get(upper)
            if fbgn:
                return f"FlyBase:{fbgn}", gene_symbol
            fbgn = (self.flybase_upper_symbol_to_fbgn or {}).get(upper)
            if fbgn:
                return f"FlyBase:{fbgn}", gene_symbol

            return None, gene_symbol

        if not self.hgnc_to_ensembl_map:
            return None, gene_symbol

        ensembl = self.hgnc_to_ensembl_map.get(gene_symbol.upper())
        if not ensembl:
            return None, gene_symbol
        return f"ENSEMBL:{ensembl}", gene_symbol

    def get_nodes(self):
        """
        Build a node for each promoter in the EPD BED file
        """
        for line in self._iter_epd_bed_rows():
            chr = line[EPDAdapter.INDEX['chr']]
            coord_start = int(line[EPDAdapter.INDEX['coord_start']]) + 1  # +1 since it is 0 indexed coordinate
            coord_end = int(line[EPDAdapter.INDEX['coord_end']])
            promoter_id = f"SO:{build_regulatory_region_id(chr, coord_start, coord_end)}"

            if check_genomic_location(self.chr, self.start, self.end, chr, coord_start, coord_end):
                props = {}
                if self.write_properties:
                    props['chr'] = chr
                    props['start'] = coord_start
                    props['end'] = coord_end
                    props['taxon_id'] = f'{self.taxon_id}'

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                yield promoter_id, self.label, props

    def get_edges(self):
        """
        Build an edge for each promoter-gene interaction in the EPD BED file.
        """
        missing_counts = Counter()
        ambiguous_counts = Counter()
        for line in self._iter_epd_bed_rows():
            chr = line[EPDAdapter.INDEX['chr']]
            coord_start = int(line[EPDAdapter.INDEX['coord_start']]) + 1  # +1 since it is 0 indexed coordinate
            coord_end = int(line[EPDAdapter.INDEX['coord_end']])

            gene_curie, gene_symbol = self._resolve_gene_curie_from_epd_token(line[EPDAdapter.INDEX['gene_id']])
            if gene_curie is None:
                if self._is_dmel_taxon_id(self.taxon_id) and gene_symbol in (self.flybase_ambiguous_symbols or {}):
                    ambiguous_counts[gene_symbol] += 1
                else:
                    missing_counts[gene_symbol] += 1
                if self.strict_gene_symbol_check:
                    raise KeyError(
                        f"EPD gene symbol '{gene_symbol}' not resolvable for taxon_id={self.taxon_id}."
                    )
                continue

            if check_genomic_location(self.chr, self.start, self.end, chr, coord_start, coord_end):
                promoter_id = f"SO:{build_regulatory_region_id(chr, coord_start, coord_end)}"
                props = {}
                if self.write_properties:
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                yield promoter_id, gene_curie, self.label, props

        if self._is_dmel_taxon_id(self.taxon_id):
            if ambiguous_counts:
                top = sorted(ambiguous_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:20]
                top_pretty = [
                    {
                        'symbol': sym,
                        'epd_occurrences': n,
                        'fbgn_candidates': (self.flybase_ambiguous_symbols or {}).get(sym, []),
                    }
                    for sym, n in top
                ]
                print(
                    f"[EPDAdapter] Ambiguous FlyBase symbols (multiple FBgn): {len(ambiguous_counts)} distinct. "
                    f"Top 20 by EPD occurrences: {top_pretty}"
                )
            if missing_counts:
                top = sorted((kv for kv in missing_counts.items() if kv[0]), key=lambda kv: (-kv[1], kv[0]))[:20]
                top_pretty = [{'symbol': sym, 'epd_occurrences': n} for sym, n in top]
                print(
                    f"[EPDAdapter] EPD symbols not found in FlyBase gene_info: {len(missing_counts)} distinct. "
                    f"Top 20 by EPD occurrences: {top_pretty}"
                )

