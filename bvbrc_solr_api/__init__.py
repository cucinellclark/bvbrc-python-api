from types import SimpleNamespace

from .core.http_client import create_context, run as run_internal
from .resources.antibiotics import Antibiotics
from .resources.bioset import Bioset
from .resources.bioset_result import BiosetResult
from .resources.enzyme_class_ref import EnzymeClassRef
from .resources.epitope import Epitope
from .resources.epitope_assay import EpitopeAssay
from .resources.experiment import Experiment
from .resources.feature_sequence import FeatureSequence
from .resources.gene_ontology_ref import GeneOntologyRef
from .resources.genome import Genome
from .resources.genome_amr import GenomeAmr
from .resources.genome_feature import GenomeFeature
from .resources.genome_sequence import GenomeSequence
from .resources.id_ref import IdRef
from .resources.pathway import Pathway
from .resources.pathway_ref import PathwayRef
from .resources.ppi import Ppi
from .resources.protein_feature import ProteinFeature
from .resources.protein_family_ref import ProteinFamilyRef
from .resources.protein_structure import ProteinStructure
from .resources.sequence_feature import SequenceFeature
from .resources.sequence_feature_vt import SequenceFeatureVt
from .resources.serology import Serology
from .resources.misc_niaid_sgc import MiscNiaidSgc
from .resources.spike_lineage import SpikeLineage
from .resources.spike_variant import SpikeVariant
from .resources.sp_gene import SpGene
from .resources.sp_gene_ref import SpGeneRef
from .resources.strain import Strain
from .resources.structured_assertion import StructuredAssertion
from .resources.subsystem import Subsystem
from .resources.subsystem_ref import SubsystemRef
from .resources.surveillance import Surveillance
from .resources.taxonomy import Taxonomy


def create_client(context_overrides: dict | None = None):
  """Factory to create a client with shared context."""
  ctx = create_context(context_overrides or {})
  return SimpleNamespace(
    antibiotics=Antibiotics(ctx),
    bioset=Bioset(ctx),
    bioset_result=BiosetResult(ctx),
    enzyme_class_ref=EnzymeClassRef(ctx),
    epitope=Epitope(ctx),
    epitope_assay=EpitopeAssay(ctx),
    experiment=Experiment(ctx),
    feature_sequence=FeatureSequence(ctx),
    gene_ontology_ref=GeneOntologyRef(ctx),
    genome=Genome(ctx),
    genome_amr=GenomeAmr(ctx),
    genome_feature=GenomeFeature(ctx),
    genome_sequence=GenomeSequence(ctx),
    id_ref=IdRef(ctx),
    pathway=Pathway(ctx),
    pathway_ref=PathwayRef(ctx),
    ppi=Ppi(ctx),
    protein_feature=ProteinFeature(ctx),
    protein_family_ref=ProteinFamilyRef(ctx),
    protein_structure=ProteinStructure(ctx),
    sequence_feature=SequenceFeature(ctx),
    sequence_feature_vt=SequenceFeatureVt(ctx),
    serology=Serology(ctx),
    misc_niaid_sgc=MiscNiaidSgc(ctx),
    spike_lineage=SpikeLineage(ctx),
    spike_variant=SpikeVariant(ctx),
    sp_gene=SpGene(ctx),
    sp_gene_ref=SpGeneRef(ctx),
    strain=Strain(ctx),
    structured_assertion=StructuredAssertion(ctx),
    subsystem=Subsystem(ctx),
    subsystem_ref=SubsystemRef(ctx),
    surveillance=Surveillance(ctx),
    taxonomy=Taxonomy(ctx),
  )


def query(core: str, filter: str = "", options: dict | None = None):
  """Generic query function against any core.

  Args:
    core: The collection/core name, e.g. "genome".
    filter: RQL filter string (e.g. "eq(genome_id,123.45)").
    options: dict with optional keys: select (list[str]), sort (str), limit (int), http_download (bool).
  """
  ctx = create_context({})
  return run_internal(core, filter, options or {}, ctx["base_url"], ctx["headers"])


__all__ = [
  "create_client",
  "query",
  "Antibiotics",
  "Bioset",
  "BiosetResult",
  "EnzymeClassRef",
  "Epitope",
  "EpitopeAssay",
  "Experiment",
  "FeatureSequence",
  "GeneOntologyRef",
  "Genome",
  "GenomeAmr",
  "GenomeFeature",
  "GenomeSequence",
  "IdRef",
  "Pathway",
  "PathwayRef",
  "Ppi",
  "ProteinFeature",
  "ProteinFamilyRef",
  "ProteinStructure",
  "SequenceFeature",
  "SequenceFeatureVt",
  "Serology",
  "MiscNiaidSgc",
  "SpikeLineage",
  "SpikeVariant",
  "SpGene",
  "SpGeneRef",
  "Strain",
  "StructuredAssertion",
  "Subsystem",
  "SubsystemRef",
  "Surveillance",
  "Taxonomy",
]


