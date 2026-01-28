# BV-BRC Solr Python API Client

Async Python client for the BV-BRC Solr API with connection pooling and streaming support.

## Features

- **Fully async** using `httpx.AsyncClient` with connection pooling
- **Streaming support** with cursor-based pagination for large datasets
- **Type hints** for better IDE support
- **Custom exceptions** for better error handling
- **34 resource types** including genomes, features, sequences, and more

## Requirements

- Python >= 3.10
- pip (and optionally `venv`)

## Installation

### Install from source (editable)

1) Clone this repository:

```bash
git clone <YOUR_GIT_REMOTE_URL>
cd bvbrc-python-api
```

2) (Optional) Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3) Install the package in editable mode:

```bash
pip install -U pip
pip install -e .
```

## Usage

### Basic Query

```python
import asyncio
from bvbrc_solr_api import create_client

async def main():
    async with create_client() as client:
        # Get a genome by ID
        genome = await client.genome.get_by_id("1310613.3")
        print(f"Genome: {genome}")
        
        # Query by filters
        genomes = await client.genome.get_by_genus("Salmonella", {"limit": 10})
        print(f"Found {len(genomes)} Salmonella genomes")

asyncio.run(main())
```

### Streaming Large Datasets

```python
import asyncio
from bvbrc_solr_api import create_client

async def main():
    async with create_client() as client:
        # Stream all genomes with cursor pagination
        pager = client.genome.stream_all_solr(
            rows=1000,  # Fetch 1000 per page
            fields=["genome_id", "genome_name", "species"],
        )
        
        count = 0
        async for doc in pager:
            count += 1
            print(f"Genome {count}: {doc['genome_name']}")

asyncio.run(main())
```

### Concurrent Queries

```python
import asyncio
from bvbrc_solr_api import create_client

async def main():
    async with create_client() as client:
        # Run multiple queries in parallel
        results = await asyncio.gather(
            client.genome.get_by_genus("Salmonella", {"limit": 10}),
            client.genome.get_by_genus("Escherichia", {"limit": 10}),
            client.genome.get_by_genus("Bacillus", {"limit": 10}),
        )
        
        for i, result in enumerate(results):
            print(f"Query {i+1}: {len(result)} results")

asyncio.run(main())
```

### Error Handling

```python
import asyncio
from bvbrc_solr_api import create_client, BVBRCHTTPError, BVBRCConnectionError

async def main():
    async with create_client() as client:
        try:
            result = await client.genome.get_by_id("invalid_id")
        except BVBRCHTTPError as e:
            print(f"HTTP Error {e.status_code}: {e.message}")
        except BVBRCConnectionError as e:
            print(f"Connection Error: {e.message}")

asyncio.run(main())
```

### Custom Configuration

```python
import asyncio
from bvbrc_solr_api import create_client

async def main():
    config = {
        "base_url": "https://www.bv-brc.org/api-bulk",
        "timeout": 120.0,  # 2 minute timeout
    }
    
    async with create_client(config) as client:
        result = await client.genome.get_by_id("1310613.3")

asyncio.run(main())
```

## Available Resources

The client provides access to 34 BV-BRC resources:

- `antibiotics`
- `bioset`, `bioset_result`
- `enzyme_class_ref`
- `epitope`, `epitope_assay`
- `experiment`
- `feature_sequence`
- `gene_ontology_ref`
- `genome`, `genome_amr`, `genome_feature`, `genome_sequence`
- `id_ref`
- `pathway`, `pathway_ref`
- `ppi`
- `protein_feature`, `protein_family_ref`, `protein_structure`
- `sequence_feature`, `sequence_feature_vt`
- `serology`
- `misc_niaid_sgc`
- `spike_lineage`, `spike_variant`
- `sp_gene`, `sp_gene_ref`
- `strain`
- `structured_assertion`
- `subsystem`, `subsystem_ref`
- `surveillance`
- `taxonomy`

Each resource provides methods like:
- `get_by_id()` - Retrieve by primary key
- `query_by()` - Query with custom filters
- `get_by_*()` - Retrieve by specific fields
- `search_by_keyword()` - Keyword search
- `stream_all_solr()` - Stream large result sets

## Architecture

- **Connection Pooling**: Shared `AsyncClient` with configurable limits (20 keepalive, 100 max connections)
- **Async/Await**: All I/O operations are async for better concurrency
- **Context Manager**: Automatic cleanup of HTTP connections
- **Custom Exceptions**: Clear error types for different failure modes
- **Streaming**: Cursor-based pagination for memory-efficient large queries

## Notes

- The package depends on `httpx>=0.28` for async HTTP support
- All methods are async and must be awaited
- Use `async with` context manager for proper connection cleanup
- See `example_async.py` for more usage examples

## License

See the main repository for license information.
