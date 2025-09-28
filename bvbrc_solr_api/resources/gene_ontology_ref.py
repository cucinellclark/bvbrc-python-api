from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb


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


__all__ = ["GeneOntologyRef"]


