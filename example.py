import asyncio
import signal

from maga.crawler import Maga
from maga.dht import DHTNode

async def print_stats(dht_node):
    """
    Prints the number of nodes in the DHTNode's routing table every 10 seconds.
    """
    while True:
        await asyncio.sleep(10)
        print(f"[Stats] DHT Node knows {len(dht_node.routing_table.get_all_nodes())} nodes.")
        # You can also print bucket information for more detail
        # for i, bucket in enumerate(dht_node.routing_table.buckets):
        #     print(f"  Bucket {i}: {len(bucket)} nodes")


async def main():
    loop = asyncio.get_running_loop()

    # Create instances of the crawler and the DHT node
    crawler = Maga(loop=loop)
    dht_node = DHTNode(loop=loop)

    # Start the services on different ports
    await crawler.run(port=6881)
    print("Lightweight crawler started on port 6881.")

    await dht_node.run(port=6882)
    print("DHT node for downloader started on port 6882.")

    # Start the stats printer
    stats_task = loop.create_task(print_stats(dht_node))

    print("\nRunning both services. Press Ctrl+C to stop.")

    # Wait for Ctrl+C
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    # Clean up
    print("\nStopping services...")
    stats_task.cancel()
    crawler.stop()
    dht_node.stop()
    print("Services stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
