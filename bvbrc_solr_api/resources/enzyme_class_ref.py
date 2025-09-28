from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb


class EnzymeClassRef:
  def __init__(self, context: Dict[str, Any]):
    self._ctx = context

  def get_by_id(self, ec_number: str, options: Dict[str, Any] | None = None):
    return run("enzyme_class_ref", qb.eq("ec_number", ec_number), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def query_by(self, filters: Dict[str, Any] | None = None, options: Dict[str, Any] | None = None):
    return run("enzyme_class_ref", qb.build_and_from(filters or {}), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_ec_description(self, ec_description: str, options: Dict[str, Any] | None = None):
    return run("enzyme_class_ref", qb.eq("ec_description", ec_description), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_go(self, go_term: str, options: Dict[str, Any] | None = None):
    return run("enzyme_class_ref", qb.eq("go", go_term), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_version(self, version: int, options: Dict[str, Any] | None = None):
    return run("enzyme_class_ref", qb.eq("_version_", version), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_inserted_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    filters = [qb.gt("date_inserted", start_date), qb.lt("date_inserted", end_date)]
    return run("enzyme_class_ref", qb.and_(*filters), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_modified_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    filters = [qb.gt("date_modified", start_date), qb.lt("date_modified", end_date)]
    return run("enzyme_class_ref", qb.and_(*filters), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def search_by_keyword(self, keyword: str, options: Dict[str, Any] | None = None):
    return run("enzyme_class_ref", f"keyword({quote(keyword)})", options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_all(self, options: Dict[str, Any] | None = None):
    return run("enzyme_class_ref", "", options or {}, self._ctx["base_url"], self._ctx["headers"])


__all__ = ["EnzymeClassRef"]
