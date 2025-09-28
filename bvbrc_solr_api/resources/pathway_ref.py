from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb


class PathwayRef:
  def __init__(self, context: Dict[str, Any]):
    self._ctx = context

  def get_by_id(self, id: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("id", id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def query_by(self, filters: Dict[str, Any] | None = None, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.build_and_from(filters or {}), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_ec_number(self, ec_number: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("ec_number", ec_number), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_ec_description(self, ec_description: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("ec_description", ec_description), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_map_location(self, map_location: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("map_location", map_location), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_map_name(self, map_name: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("map_name", map_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_map_type(self, map_type: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("map_type", map_type), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_occurrence(self, occurrence: int, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("occurrence", occurrence), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_pathway_class(self, pathway_class: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("pathway_class", pathway_class), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_pathway_id(self, pathway_id: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("pathway_id", pathway_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_pathway_name(self, pathway_name: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.eq("pathway_name", pathway_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_occurrence_range(self, min_occurrence: int, max_occurrence: int, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.and_(qb.gte("occurrence", min_occurrence), qb.lte("occurrence", max_occurrence)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_inserted_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.and_(qb.gte("date_inserted", start_date), qb.lte("date_inserted", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_modified_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", qb.and_(qb.gte("date_modified", start_date), qb.lte("date_modified", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def search_by_keyword(self, keyword: str, options: Dict[str, Any] | None = None):
    return run("pathway_ref", f"keyword({quote(keyword)})", options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_all(self, options: Dict[str, Any] | None = None):
    return run("pathway_ref", "", options or {}, self._ctx["base_url"], self._ctx["headers"])


__all__ = ["PathwayRef"]


