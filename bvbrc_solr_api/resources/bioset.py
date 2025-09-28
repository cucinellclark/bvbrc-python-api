from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb


class Bioset:
  def __init__(self, context: Dict[str, Any]):
    self._ctx = context

  def get_by_id(self, bioset_id: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("bioset_id", bioset_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def query_by(self, filters: Dict[str, Any] | None = None, options: Dict[str, Any] | None = None):
    return run("bioset", qb.build_and_from(filters or {}), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_bioset_name(self, bioset_name: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("bioset_name", bioset_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_bioset_type(self, bioset_type: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("bioset_type", bioset_type), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_exp_id(self, exp_id: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("exp_id", exp_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_exp_name(self, exp_name: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("exp_name", exp_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_exp_type(self, exp_type: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("exp_type", exp_type), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_organism(self, organism: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("organism", organism), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_strain(self, strain: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("strain", strain), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_taxon_id(self, taxon_id: int, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("taxon_id", taxon_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_entity_type(self, entity_type: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("entity_type", entity_type), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_result_type(self, result_type: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("result_type", result_type), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_analysis_method(self, analysis_method: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("analysis_method", analysis_method), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_analysis_group_1(self, analysis_group_1: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("analysis_group_1", analysis_group_1), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_analysis_group_2(self, analysis_group_2: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("analysis_group_2", analysis_group_2), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_treatment_type(self, treatment_type: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("treatment_type", treatment_type), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_treatment_name(self, treatment_name: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("treatment_name", treatment_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_study_name(self, study_name: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("study_name", study_name), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_study_pi(self, study_pi: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("study_pi", study_pi), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_study_institution(self, study_institution: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("study_institution", study_institution), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_genome_id(self, genome_id: str, options: Dict[str, Any] | None = None):
    return run("bioset", qb.eq("genome_id", genome_id), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_date_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    filters = [qb.gt("date_inserted", start_date), qb.lt("date_inserted", end_date)]
    return run("bioset", qb.and_(*filters), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_by_modified_date_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    filters = [qb.gt("date_modified", start_date), qb.lt("date_modified", end_date)]
    return run("bioset", qb.and_(*filters), options or {}, self._ctx["base_url"], self._ctx["headers"])

  def search_by_keyword(self, keyword: str, options: Dict[str, Any] | None = None):
    return run("bioset", f"keyword({quote(keyword)})", options or {}, self._ctx["base_url"], self._ctx["headers"])

  def get_all(self, options: Dict[str, Any] | None = None):
    return run("bioset", "", options or {}, self._ctx["base_url"], self._ctx["headers"])


__all__ = ["Bioset"]
