import asyncio
from bvbrc_solr_api import create_client
from bvbrc_solr_api.core.solr_query_builder import qb as solrqb

async def main():
    async with create_client() as client:
        pager = client.genome_feature.stream_all_solr(fq=[solrqb.eq("genome_id", "208964.12")])
        count = 0
        async for feature in pager:
            count += 1
        print(f"Found {count} features")

asyncio.run(main())

