from __future__ import annotations

from typing import Any, Dict, Iterable

import httpx

from ..exceptions import BVBRCHTTPError, BVBRCConnectionError, BVBRCTimeoutError


DEFAULT_BASE_URL = "https://www.bv-brc.org/api-bulk"
DEFAULT_HEADERS = {
  "Accept": "application/json",
  "Content-Type": "application/rqlquery+x-www-form-urlencoded",
}

# Connection pool settings for optimal async performance
LIMITS = httpx.Limits(
  max_keepalive_connections=20,
  max_connections=100,
  keepalive_expiry=30.0
)

# Default timeout configuration
DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)


def create_context(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
  overrides = overrides or {}
  base_url = overrides.get("base_url") or overrides.get("baseUrl") or DEFAULT_BASE_URL
  headers = dict(DEFAULT_HEADERS)
  headers.update(overrides.get("headers") or {})
  timeout = overrides.get("timeout", DEFAULT_TIMEOUT)
  return {
    "base_url": base_url,
    "headers": headers,
    "timeout": timeout,
  }


def _build_body(filter: str, options: Dict[str, Any]) -> str:
  from .query_builder import select as qb_select, sort as qb_sort, limit as qb_limit, http_download as qb_http_download

  select_fields: Iterable[str] | None = options.get("select")
  sort_expr: str | None = options.get("sort")
  limit_value: int | None = options.get("limit")
  http_download: bool = bool(options.get("http_download", False))

  if http_download and not sort_expr:
    raise ValueError("sort parameter is required when http_download is true")

  final_limit = limit_value if isinstance(limit_value, int) else 1000

  params: list[str] = []
  if filter:
    params.append(filter)
  if select_fields:
    params.append(qb_select(list(select_fields)))
  if sort_expr:
    params.append(qb_sort(sort_expr))
  if isinstance(final_limit, int):
    params.append(qb_limit(final_limit))
  if http_download:
    params.append(qb_http_download(True))

  return "&".join([p for p in params if p])


async def run(
  core_name: str, 
  filter: str, 
  options: Dict[str, Any] | None, 
  client: httpx.AsyncClient,
  base_url: str | None, 
  headers: Dict[str, str] | None,
  timeout: float | httpx.Timeout | None = None
) -> Dict[str, Any]:
  """
  Async RQL query execution.
  
  Args:
    core_name: Core/collection name
    filter: RQL filter string
    options: Query options (select, sort, limit, etc.)
    client: Shared AsyncClient instance
    base_url: API base URL
    headers: Request headers
    timeout: Request timeout
    
  Returns:
    JSON response from API
    
  Raises:
    httpx.HTTPStatusError: For HTTP error responses
    httpx.RequestError: For connection/network errors
  """
  options = options or {}
  url = f"{(base_url or DEFAULT_BASE_URL).rstrip('/')}/{core_name}/"
  body = _build_body(filter, options)
  final_headers = headers or DEFAULT_HEADERS

  try:
    response = await client.post(
      url, 
      content=body, 
      headers=final_headers, 
      timeout=timeout or DEFAULT_TIMEOUT
    )
    response.raise_for_status()
    return response.json()
  except httpx.HTTPStatusError as e:
    error_msg = f"HTTP error for core '{core_name}'"
    print(f"{error_msg}: {e.response.status_code}")
    print(f"Response text: {e.response.text[:500]}")  # First 500 chars
    raise BVBRCHTTPError(
      status_code=e.response.status_code,
      message=error_msg,
      response_text=e.response.text
    ) from e
  except httpx.TimeoutException as e:
    error_msg = f"Request to core '{core_name}' timed out"
    print(error_msg)
    raise BVBRCTimeoutError(error_msg) from e
  except httpx.RequestError as e:
    error_msg = f"Connection error for core '{core_name}': {type(e).__name__}"
    print(f"{error_msg} - {str(e)}")
    raise BVBRCConnectionError(error_msg, original_error=e) from e


__all__ = [
  "create_context",
  "run",
  "LIMITS",
  "DEFAULT_TIMEOUT",
]
