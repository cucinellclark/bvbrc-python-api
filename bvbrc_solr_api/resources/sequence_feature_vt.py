from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb


class SequenceFeatureVt:
  def __init__(self, context: Dict[str, Any]):
    self._ctx = context

  def get_by_id(self, id: str, options: Dict[str, Any] | None = None):
    return run("sequence_feature_vt", qb.eq("id", id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def query_by(self, filters: Dict[str, Any] | None = None, options: Dict[str, Any] | None = None):
    return run("sequence_feature_vt", qb.build_and_from(filters or {}), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_sf_category(self, sf_category: str, options: Dict[str, Any] | None = None):
    return run("sequence_feature_vt", qb.eq("sf_category", sf_category), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_genome_id(self, genome_id: str, options: Dict[str, Any] | None = None):
    return run("sequence_feature_vt", qb.eq("genome_id", genome_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_taxon_id(self, taxon_id: int, options: Dict[str, Any] | None = None):
    return run("sequence_feature_vt", qb.eq("taxon_id", taxon_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_inserted_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("sequence_feature_vt", qb.and_(qb.gte("date_inserted", start_date), qb.lte("date_inserted", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_modified_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("sequence_feature_vt", qb.and_(qb.gte("date_modified", start_date), qb.lte("date_modified", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def search_by_keyword(self, keyword: str, options: Dict[str, Any] | None = None):
    return run("sequence_feature_vt", f"keyword({quote(keyword)})", options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_all(self, options: Dict[str, Any] | None = None):
    return run("sequence_feature_vt", "", options or {}, self._ctx["base_url"], self._ctx["headers"])


__all__ = ["SequenceFeatureVt"]


