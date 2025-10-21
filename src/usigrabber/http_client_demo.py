import asyncio

from async_http_client import AsyncHttpClient


async def main():
    async with AsyncHttpClient() as client:
        html = await client.get("https://example.com")
        print("Got:", len(html), "chars")

        response = await client.post("https://httpbin.org/post", json={"foo": "bar"})
        print("POST result:", response)


asyncio.run(main())
