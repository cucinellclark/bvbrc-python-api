from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb


class SubsystemRef:
  def __init__(self, context: Dict[str, Any]):
    self._ctx = context

  def get_by_id(self, id: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.eq("id", id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def query_by(self, filters: Dict[str, Any] | None = None, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.build_and_from(filters or {}), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_class(self, class_name: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.eq("class", class_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_description(self, description: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.eq("description", description), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_role(self, role: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.eq("role", role), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_role_id(self, role_id: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.eq("role_id", role_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_subsystem_id(self, subsystem_id: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.eq("subsystem_id", subsystem_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_subsystem_name(self, subsystem_name: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.eq("subsystem_name", subsystem_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_superclass(self, superclass: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.eq("superclass", superclass), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_inserted_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.and_(qb.gte("date_inserted", start_date), qb.lte("date_inserted", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_modified_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", qb.and_(qb.gte("date_modified", start_date), qb.lte("date_modified", end_date)), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def search_by_keyword(self, keyword: str, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", f"keyword({quote(keyword)})", options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_all(self, options: Dict[str, Any] | None = None):
    return run("subsystem_ref", "", options or {}, self._ctx["base_url"], self._ctx["headers"])


__all__ = ["SubsystemRef"]


