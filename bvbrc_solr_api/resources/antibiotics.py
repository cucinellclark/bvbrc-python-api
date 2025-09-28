from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb


class Antibiotics:
  def __init__(self, context: Dict[str, Any]):
    self._ctx = context

  def get_by_pubchem_cid(self, pubchem_cid: str, options: Dict[str, Any] | None = None):
    return run("antibiotics", qb.eq("pubchem_cid", pubchem_cid), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def query_by(self, filters: Dict[str, Any] | None = None, options: Dict[str, Any] | None = None):
    return run("antibiotics", qb.build_and_from(filters or {}), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def search_by_keyword(self, keyword: str, options: Dict[str, Any] | None = None):
    return run("antibiotics", f"keyword({quote(keyword)})", options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_antibiotic_name(self, antibiotic_name: str, options: Dict[str, Any] | None = None):
    return run("antibiotics", qb.eq("antibiotic_name", antibiotic_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_cas_id(self, cas_id: str, options: Dict[str, Any] | None = None):
    return run("antibiotics", qb.eq("cas_id", cas_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_molecular_formula(self, molecular_formula: str, options: Dict[str, Any] | None = None):
    return run("antibiotics", qb.eq("molecular_formula", molecular_formula), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_atc_classification(self, atc_classification: str, options: Dict[str, Any] | None = None):
    return run("antibiotics", qb.eq("atc_classification", atc_classification), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_mechanism_of_action(self, mechanism_of_action: str, options: Dict[str, Any] | None = None):
    return run("antibiotics", qb.eq("mechanism_of_action", mechanism_of_action), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_pharmacological_class(self, pharmacological_class: str, options: Dict[str, Any] | None = None):
    return run("antibiotics", qb.eq("pharmacological_classes", pharmacological_class), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_synonym(self, synonym: str, options: Dict[str, Any] | None = None):
    return run("antibiotics", qb.eq("synonyms", synonym), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_molecular_weight_range(self, min_weight: float, max_weight: float, options: Dict[str, Any] | None = None):
    filters = [qb.gt("molecular_weight", min_weight), qb.lt("molecular_weight", max_weight)]
    return run("antibiotics", qb.and_(*filters), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    filters = [qb.gt("date_inserted", start_date), qb.lt("date_inserted", end_date)]
    return run("antibiotics", qb.and_(*filters), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_all(self, options: Dict[str, Any] | None = None):
    return run("antibiotics", "", options or {}, self._ctx["base_url"], self._ctx["headers"])


__all__ = ["Antibiotics"]
