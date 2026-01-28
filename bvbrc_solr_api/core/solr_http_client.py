from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from ..exceptions import BVBRCHTTPError, BVBRCConnectionError, BVBRCTimeoutError


# Default Solr base URL; should point at the Solr root, not the generic API root
DEFAULT_SOLR_BASE_URL = "https://www.bv-brc.org/api-bulk/"

# Connection pool settings for optimal async performance
LIMITS = httpx.Limits(
  max_keepalive_connections=20,
  max_connections=100,
  keepalive_expiry=30.0
)

# Default timeout configuration
DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)


def create_solr_context(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
  overrides = overrides or {}
  base_url = overrides.get("solr_base_url") or overrides.get("solrBaseUrl") or DEFAULT_SOLR_BASE_URL
  headers = dict(overrides.get("headers") or {})
  auth = overrides.get("auth")
  timeout = overrides.get("timeout", DEFAULT_TIMEOUT)
  return {
    "solr_base_url": base_url.rstrip("/"),
    "headers": headers,
    "auth": auth,
    "timeout": timeout,
  }


async def select(
  collection: str, 
  params: Dict[str, Any], 
  client: httpx.AsyncClient,
  base_url: Optional[str] = None, 
  headers: Optional[Dict[str, str]] = None, 
  auth: Any = None, 
  timeout: float | httpx.Timeout | None = None
) -> Dict[str, Any]:
  """
  Async Solr select endpoint.
  
  Args:
    collection: Solr collection name
    params: Query parameters
    client: Shared AsyncClient instance
    base_url: Override base URL
    headers: Additional headers
    auth: Authentication credentials
    timeout: Request timeout
    
  Returns:
    JSON response from Solr
    
  Raises:
    httpx.HTTPStatusError: For HTTP error responses
    httpx.RequestError: For connection/network errors
  """
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

  try:
    response = await client.post(
      url, 
      data=params, 
      headers=final_headers, 
      auth=auth, 
      timeout=timeout or DEFAULT_TIMEOUT
    )
    response.raise_for_status()
    return response.json()
  except httpx.HTTPStatusError as e:
    error_msg = f"Solr HTTP error for collection '{collection}'"
    print(f"{error_msg}: {e.response.status_code}")
    print(f"Response text: {e.response.text[:500]}")  # First 500 chars
    raise BVBRCHTTPError(
      status_code=e.response.status_code,
      message=error_msg,
      response_text=e.response.text
    ) from e
  except httpx.TimeoutException as e:
    error_msg = f"Request to Solr collection '{collection}' timed out"
    print(error_msg)
    raise BVBRCTimeoutError(error_msg) from e
  except httpx.RequestError as e:
    error_msg = f"Connection error for Solr collection '{collection}': {type(e).__name__}"
    print(f"{error_msg} - {str(e)}")
    raise BVBRCConnectionError(error_msg, original_error=e) from e


__all__ = [
  "create_solr_context",
  "select",
  "LIMITS",
  "DEFAULT_TIMEOUT",
]
