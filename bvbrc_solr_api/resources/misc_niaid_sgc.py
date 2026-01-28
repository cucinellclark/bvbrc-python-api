from __future__ import annotations

from typing import Any, Dict
import httpx
from urllib.parse import quote

from ..core.http_client import run
from ..core.query_builder import qb
from ..core.solr_query_builder import qb as solrqb
from ..core.solr_http_client import create_solr_context
from ..core.cursor import CursorPager


class MiscNiaidSgc:
  def __init__(self, context: Dict[str, Any], client: httpx.AsyncClient):
    self._ctx = context
    self._client = client

  async def get_by_id(self, target_id: str, options: Dict[str, Any] | None = None):
    return await run("misc_niaid_sgc", qb.eq("target_id", target_id), options or {}, self._client, self._ctx["base_url"], self._ctx["headers"])

  async def query_by(self, filters: Dict[str, Any] | None = None, options: Dict[str, Any] | None = None):
    return await run("misc_niaid_sgc", qb.build_and_from(filters or {}), options or {}, self._client, self._ctx["base_url"], self._ctx["headers"])

  async def get_by_genus(self, genus: str, options: Dict[str, Any] | None = None):
    return await run("misc_niaid_sgc", qb.eq("genus", genus), options or {}, self._client, self._ctx["base_url"], self._ctx["headers"])

  async def get_by_species(self, species: str, options: Dict[str, Any] | None = None):
    return await run("misc_niaid_sgc", qb.eq("species", species), options or {}, self._client, self._ctx["base_url"], self._ctx["headers"])

  async def get_by_taxon_id(self, taxon_id: int, options: Dict[str, Any] | None = None):
    return await run("misc_niaid_sgc", qb.eq("taxon_id", taxon_id), options or {}, self._client, self._ctx["base_url"], self._ctx["headers"])

  async def get_by_date_inserted_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return await run("misc_niaid_sgc", qb.and_(qb.gte("date_inserted", start_date), qb.lte("date_inserted", end_date)), options or {}, self._client, self._ctx["base_url"], self._ctx["headers"])

  async def get_by_date_modified_range(self, start_date: str, end_date: str, options: Dict[str, Any] | None = None):
    return await run("misc_niaid_sgc", qb.and_(qb.gte("date_modified", start_date), qb.lte("date_modified", end_date)), options or {}, self._client, self._ctx["base_url"], self._ctx["headers"])

  async def search_by_keyword(self, keyword: str, options: Dict[str, Any] | None = None):
    return await run("misc_niaid_sgc", f"keyword({quote(keyword)})", options or {}, self._client, self._ctx["base_url"], self._ctx["headers"])

  async def get_all(self, options: Dict[str, Any] | None = None):
    return await run("misc_niaid_sgc", "", options or {}, self._client, self._ctx["base_url"], self._ctx["headers"])

  # Solr cursor-based streaming (Option B implementation)
  def stream_all_solr(
    self,
    *,
    rows: int = 1000,
    sort: str | None = None,
    unique_key: str | None = "target_id",
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
      client=self._client,
      collection="misc_niaid_sgc",
      base_params=base_params,
      base_url=solr_ctx["solr_base_url"],
      headers=solr_ctx.get("headers"),
      auth=solr_ctx.get("auth"),
      rows=rows,
      sort=f'{unique_key} asc',
      unique_key=unique_key,
      start_cursor=start_cursor,
      timeout=solr_ctx.get("timeout"),
    )


__all__ = ["MiscNiaidSgc"]


