import asyncio
from maga.crawler import Maga

async def print_stats(crawler):
    """
    Prints the number of nodes in the routing table every 10 seconds.
    """
    while True:
        # Wait a bit for the crawler to start finding nodes
        await asyncio.sleep(10)
        print(f"[Stats] Nodes in routing table: {len(crawler.routing_table.get_all_nodes())}")

        # You can also print bucket information for more detail
        # for i, bucket in enumerate(crawler.routing_table.buckets):
        #     print(f"  Bucket {i}: {len(bucket)} nodes")

if __name__ == "__main__":
    print("Starting DHT crawler...")
    print("Press Ctrl+C to stop.")

    loop = asyncio.get_event_loop()
    crawler = Maga(loop=loop)

    # Schedule the stats printer to run in the background
    asyncio.ensure_future(print_stats(crawler), loop=loop)

    try:
        # This call will block and run the event loop, executing all scheduled tasks.
        crawler.run()
    except KeyboardInterrupt:
        pass
    finally:
        print("\nCrawler stopping...")
        # The stop() method is already called by the signal handler inside run(),
        # but we can call it here just in case. The loop is also stopped
        # and closed inside the crawler's run/stop methods.
        crawler.stop()
