#!/usr/bin/env python3

import argparse
import json
import sys
from typing import List

from bvbrc_solr_api import create_client


def parse_args():
  parser = argparse.ArgumentParser(description="Quick tester for bvbrc-solr-python-api")
  parser.add_argument("--base-url", dest="base_url", default=None, help="Override base URL (default: https://www.bv-brc.org/api)")
  parser.add_argument("--collection", choices=["genome", "genome_feature", "both"], default="both", help="Which collection(s) to query")
  parser.add_argument("--limit", type=int, default=1, help="Limit rows returned (default: 1)")
  parser.add_argument("--sort", default=None, help="Sort expression, e.g., 'genome_id asc'")
  parser.add_argument("--select", default=None, help="Comma-separated list of fields to select")

  # Optional targeted filters
  parser.add_argument("--genome-id", dest="genome_id", default=None, help="Filter genome by genome_id")
  parser.add_argument("--feature-id", dest="feature_id", default=None, help="Filter genome_feature by feature_id")
  parser.add_argument("--protein-id", dest="protein_id", default=None, help="Filter genome_feature by protein_id")

  return parser.parse_args()


def build_options(limit: int, sort: str | None, select: str | None):
  options = {"limit": limit}
  if sort:
    options["sort"] = sort
  if select:
    fields: List[str] = [f.strip() for f in select.split(",") if f.strip()]
    if fields:
      options["select"] = fields
  return options


def print_rows(title: str, rows):
  print(f"\n=== {title} ===")
  if isinstance(rows, list):
    print(f"rows: {len(rows)}")
    if rows:
      print(json.dumps(rows[0], indent=2))
  else:
    print(type(rows))
    try:
      print(json.dumps(rows, indent=2))
    except Exception:
      print(rows)


def main():
  args = parse_args()
  options = build_options(args.limit, args.sort, args.select)

  ctx_overrides = {}
  if args.base_url:
    ctx_overrides["base_url"] = args.base_url

  client = create_client(ctx_overrides)

  try:
    if args.collection in ("genome", "both"):
      if args.genome_id:
        rows = client.genome.get_by_id(args.genome_id, options)
        print_rows("genome.get_by_id", rows)
      else:
        rows = client.genome.get_all(options)
        print_rows("genome.get_all", rows)

    if args.collection in ("genome_feature", "both"):
      if args.feature_id:
        rows = client.genome_feature.get_by_id(args.feature_id, options)
        print_rows("genome_feature.get_by_id", rows)
      elif args.protein_id:
        rows = client.genome_feature.get_by_protein_id(args.protein_id, options)
        print_rows("genome_feature.get_by_protein_id", rows)
      else:
        rows = client.genome_feature.get_all(options)
        print_rows("genome_feature.get_all", rows)
  except Exception as exc:
    print(f"Error: {exc}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
  main()


