from types import SimpleNamespace

from .core.http_client import create_context, run as run_internal
from .resources.genome import Genome
from .resources.genome_feature import GenomeFeature


def create_client(context_overrides: dict | None = None):
  """Factory to create a client with shared context."""
  ctx = create_context(context_overrides or {})
  return SimpleNamespace(
    genome=Genome(ctx),
    genome_feature=GenomeFeature(ctx),
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
  "Genome",
  "GenomeFeature",
]


