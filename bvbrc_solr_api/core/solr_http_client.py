from __future__ import annotations

from typing import Any, Dict, Optional

import httpx


# Default Solr base URL; should point at the Solr root, not the generic API root
DEFAULT_SOLR_BASE_URL = "https://www.bv-brc.org/api/"


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


def select(collection: str, params: Dict[str, Any], base_url: Optional[str] = None, headers: Optional[Dict[str, str]] = None, auth: Any = None, timeout: float = 60.0) -> Dict[str, Any]:
  # Solr select endpoint
  # Direct collection access like the working curl example
  solr_base = (base_url or DEFAULT_SOLR_BASE_URL).rstrip("/")
  url = f"{solr_base}/{collection}/"
  final_headers = dict(headers or {})
  
  # Add Solr-specific headers for cursor queries
  final_headers["Accept"] = "application/solr+json"
  final_headers["Content-Type"] = "application/solrquery+x-www-form-urlencoded"

  # Ensure JSON response like legacy clients did (wt=json)
  if "wt" not in params:
    params = dict(params)
    params["wt"] = "json"

  with httpx.Client() as client:
    response = client.get(url, params=params, headers=final_headers, auth=auth, timeout=timeout)
    response.raise_for_status()
    return response.json()


__all__ = [
  "create_solr_context",
  "select",
]


