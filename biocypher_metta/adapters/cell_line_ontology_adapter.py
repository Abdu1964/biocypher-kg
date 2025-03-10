from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class CellLineOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'clo': 'http://purl.obolibrary.org/obo/clo.owl'
    }
    
    def __init__(self, write_properties, add_provenance, ontology, type, label='clo', dry_run=False, add_description=False, cache_dir=None):
        super(CellLineOntologyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for Cell Line Ontology.
        """
        return 'Cell Line Ontology', 'http://purl.obolibrary.org/obo/clo.owl'
