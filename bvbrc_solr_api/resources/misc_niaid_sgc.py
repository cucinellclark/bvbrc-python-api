from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb


class MiscNiaidSgc:
  def __init__(self, context: Dict[str, Any]):
    self._ctx = context

  def get_by_id(self, target_id: str, options: Dict[str, Any] | None = None):
    return run("misc_niaid_sgc", qb.eq("target_id", target_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def query_by(self, filters: Dict[str, Any] | None = None, options: Dict[str, Any] | None = None):
    return run("misc_niaid_sgc", qb.build_and_from(filters or {}), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_genus(self, genus: str, options: Dict[str, Any] | None = None):
    return run("misc_niaid_sgc", qb.eq("genus", genus), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_species(self, species: str, options: Dict[str, Any] | None = None):
    return run("misc_niaid_sgc", qb.eq("species", species), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_taxon_id(self, taxon_id: int, options: Dict[str, Any] | None = None):
    return run("misc_niaid_sgc", qb.eq("taxon_id", taxon_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_inserted_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("misc_niaid_sgc", qb.and_(qb.gte("date_inserted", start_date), qb.lte("date_inserted", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_modified_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("misc_niaid_sgc", qb.and_(qb.gte("date_modified", start_date), qb.lte("date_modified", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def search_by_keyword(self, keyword: str, options: Dict[str, Any] | None = None):
    return run("misc_niaid_sgc", f"keyword({quote(keyword)})", options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_all(self, options: Dict[str, Any] | None = None):
    return run("misc_niaid_sgc", "", options or {}, self._ctx["base_url"], self._ctx["headers"])


__all__ = ["MiscNiaidSgc"]


