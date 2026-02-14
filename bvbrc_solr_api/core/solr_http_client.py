from __future__ import annotations

from typing import Any, Dict, Optional

import httpx


# Default Solr base URL; should point at the Solr root, not the generic API root
DEFAULT_SOLR_BASE_URL = "https://www.bv-brc.org/api-bulk/"


def create_solr_context(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
  overrides = overrides or {}
  base_url = overrides.get("solr_base_url") or overrides.get("solrBaseUrl") or DEFAULT_SOLR_BASE_URL
  headers = dict(overrides.get("headers") or {})
  auth = overrides.get("auth")
  timeout = overrides.get("timeout", 60.0)
  return {
    "solr_base_url": base_url.rstrip("/"),
    "headers": headers,
    "auth": auth,
    "timeout": timeout,
  }


def _prepare_request(
  collection: str,
  params: Dict[str, Any],
  base_url: Optional[str],
  headers: Optional[Dict[str, str]],
) -> tuple[str, Dict[str, Any], Dict[str, str]]:
  # Solr select endpoint
  # Direct collection access like the working curl example
  solr_base = (base_url or DEFAULT_SOLR_BASE_URL).rstrip("/")
  url = f"{solr_base}/{collection}/"
  final_headers = dict(headers or {})
  
  # Add BV-BRC specific headers for Solr queries
  final_headers["Accept"] = "application/solr+json"
  final_headers["Content-Type"] = "application/solrquery+x-www-form-urlencoded"

  # Ensure JSON response like legacy clients did (wt=json)
  if "wt" not in params:
    params = dict(params)
    params["wt"] = "json"
  return url, params, final_headers


def select(collection: str, params: Dict[str, Any], base_url: Optional[str] = None, headers: Optional[Dict[str, str]] = None, auth: Any = None, timeout: float = 60.0) -> Dict[str, Any]:
  """Synchronous Solr select helper kept for backward compatibility."""
  url, req_params, final_headers = _prepare_request(collection, params, base_url, headers)

  with httpx.Client() as client:
    response = client.post(url, data=req_params, headers=final_headers, auth=auth, timeout=timeout)
    response.raise_for_status()
    return response.json()


async def async_select(
  collection: str,
  params: Dict[str, Any],
  *,
  client: Optional[httpx.AsyncClient] = None,
  base_url: Optional[str] = None,
  headers: Optional[Dict[str, str]] = None,
  auth: Any = None,
  timeout: float = 60.0,
) -> Dict[str, Any]:
  """Async Solr select helper supporting optional shared AsyncClient."""
  url, req_params, final_headers = _prepare_request(collection, params, base_url, headers)

  if client is not None:
    response = await client.post(url, data=req_params, headers=final_headers, auth=auth, timeout=timeout)
    response.raise_for_status()
    return response.json()

  async with httpx.AsyncClient() as local_client:
    response = await local_client.post(url, data=req_params, headers=final_headers, auth=auth, timeout=timeout)
    response.raise_for_status()
    return response.json()


__all__ = [
  "create_solr_context",
  "async_select",
  "select",
]


