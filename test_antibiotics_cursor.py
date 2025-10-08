#!/usr/bin/env python3

import argparse
import json
import sys
from typing import List

from bvbrc_solr_api import create_client


def parse_args():
  parser = argparse.ArgumentParser(description="Stream antibiotics via Solr cursor")
  parser.add_argument("--solr-base-url", dest="solr_base_url", default=None, help="Override Solr base URL (default: https://www.bv-brc.org)")
  parser.add_argument("--q", dest="q_expr", default="*:*", help="Solr q expression (default: *:*)")
  parser.add_argument("--fq", dest="fq", action="append", default=None, help="Add a filter query (repeatable)")
  parser.add_argument("--fields", dest="fields", default=None, help="Comma-separated fields to return")
  parser.add_argument("--rows", dest="rows", type=int, default=1000, help="Rows per page (default: 1000)")
  parser.add_argument("--sort", dest="sort", default=None, help="Sort expression (e.g., 'id asc'). Defaults to unique key asc")
  parser.add_argument("--unique-key", dest="unique_key", default="pubchem_cid", help="Unique key field for stable sort (default: pubchem_cid)")
  parser.add_argument("--start-cursor", dest="start_cursor", default="*", help="Starting cursorMark (default: *)")
  parser.add_argument("--timeout", dest="timeout", type=float, default=60.0, help="HTTP timeout seconds (default: 60)")
  parser.add_argument("--max-docs", dest="max_docs", type=int, default=20, help="Max docs to print before stopping (default: 20; use -1 for no limit)")
  parser.add_argument("--pretty", dest="pretty", type=int, default=2, help="Indentation for JSON pretty print (default: 2)")
  return parser.parse_args()


def parse_fields(fields_arg: str | None) -> List[str] | None:
  if not fields_arg:
    return None
  fields: List[str] = [f.strip() for f in fields_arg.split(",") if f.strip()]
  return fields or None


def main():
  args = parse_args()

  # Build context overrides for Solr
  ctx_overrides = {"timeout": args.timeout}
  if args.solr_base_url:
    ctx_overrides["solr_base_url"] = args.solr_base_url

  client = create_client({})

  fields = parse_fields(args.fields)
  fq_list = list(args.fq) if args.fq else None

  try:
    pager = client.antibiotics.stream_all_solr(
      rows=args.rows,
      sort=args.sort,
      unique_key=args.unique_key,
      fields=fields,
      q_expr=args.q_expr,
      fq=fq_list,
      start_cursor=args.start_cursor,
      context_overrides=ctx_overrides,
    )

    printed = 0
    for doc in pager.iter_docs():
      print(json.dumps(doc, indent=args.pretty))
      printed += 1
      if args.max_docs >= 0 and printed >= args.max_docs:
        break

    print(f"\nPrinted {printed} document(s).", file=sys.stderr)
  except Exception as exc:
    print(f"Error: {exc}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
  main()


