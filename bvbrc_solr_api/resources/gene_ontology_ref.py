from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb
from ..core.solr_query_builder import qb as solrqb
from ..core.solr_http_client import create_solr_context
from ..core.cursor import CursorPager


class GeneOntologyRef:
  def __init__(self, context: Dict[str, Any]):
    self._ctx = context

  def get_by_id(self, go_id: str, options: Dict[str, Any] | None = None):
    return run("gene_ontology_ref", qb.eq("go_id", go_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def query_by(self, filters: Dict[str, Any] | None = None, options: Dict[str, Any] | None = None):
    return run("gene_ontology_ref", qb.build_and_from(filters or {}), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_go_name(self, go_name: str, options: Dict[str, Any] | None = None):
    return run("gene_ontology_ref", qb.eq("go_name", go_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_definition(self, definition: str, options: Dict[str, Any] | None = None):
    return run("gene_ontology_ref", qb.eq("definition", definition), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_ontology(self, ontology: str, options: Dict[str, Any] | None = None):
    return run("gene_ontology_ref", qb.eq("ontology", ontology), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_inserted_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("gene_ontology_ref", qb.and_(qb.gte("date_inserted", start_date), qb.lte("date_inserted", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_modified_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("gene_ontology_ref", qb.and_(qb.gte("date_modified", start_date), qb.lte("date_modified", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def search_by_keyword(self, keyword: str, options: Dict[str, Any] | None = None):
    return run("gene_ontology_ref", f"keyword({quote(keyword)})", options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_all(self, options: Dict[str, Any] | None = None):
    return run("gene_ontology_ref", "", options or {}, self._ctx["base_url"], self._ctx["headers"])

  # Solr cursor-based streaming (Option B implementation)
  def stream_all_solr(
    self,
    *,
    rows: int = 1000,
    sort: str | None = None,
    unique_key: str | None = "go_id",
    fields: list[str] | None = None,
    q_expr: str | None = None,
    fq: list[str] | None = None,
    start_cursor: str = "*",
    context_overrides: Dict[str, Any] | None = None,
  ) -> CursorPager:
    # Combine base context with optional overrides to build Solr context
    merged_ctx: Dict[str, Any] = {}
    merged_ctx.update(self._ctx)
    if context_overrides:
      merged_ctx.update(context_overrides)
    solr_ctx = create_solr_context(merged_ctx)

    base_params = solrqb.build_params(
      q_expr=q_expr or "*:*",
      fq_list=fq or None,
      fields=fields or None,
      # sort, rows, cursorMark handled by CursorPager for iteration
    )

    return CursorPager(
      collection="gene_ontology_ref",
      base_params=base_params,
      base_url=solr_ctx["solr_base_url"],
      headers=solr_ctx.get("headers"),
      auth=solr_ctx.get("auth"),
      rows=rows,
      sort=f"{unique_key} asc",
      unique_key=unique_key,
      start_cursor=start_cursor,
      timeout=solr_ctx.get("timeout", 60.0),
    )


__all__ = ["GeneOntologyRef"]


