"""Custom exceptions for BV-BRC Solr API client."""

from __future__ import annotations

from typing import Any


class BVBRCError(Exception):
  """Base exception for all BV-BRC API errors."""
  pass


class BVBRCHTTPError(BVBRCError):
  """Exception raised for HTTP errors from the BV-BRC API."""
  
  def __init__(self, status_code: int, message: str, response_text: str | None = None):
    self.status_code = status_code
    self.message = message
    self.response_text = response_text
    super().__init__(f"HTTP {status_code}: {message}")


class BVBRCConnectionError(BVBRCError):
  """Exception raised for connection errors to the BV-BRC API."""
  
  def __init__(self, message: str, original_error: Exception | None = None):
    self.message = message
    self.original_error = original_error
    super().__init__(f"Connection error: {message}")


class BVBRCTimeoutError(BVBRCError):
  """Exception raised when a request times out."""
  
  def __init__(self, message: str = "Request timed out"):
    self.message = message
    super().__init__(message)


class BVBRCQueryError(BVBRCError):
  """Exception raised for invalid queries or query building errors."""
  
  def __init__(self, message: str):
    self.message = message
    super().__init__(f"Query error: {message}")


__all__ = [
  "BVBRCError",
  "BVBRCHTTPError",
  "BVBRCConnectionError",
  "BVBRCTimeoutError",
  "BVBRCQueryError",
]

