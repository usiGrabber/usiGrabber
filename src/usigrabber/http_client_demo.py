import asyncio

from async_http_client import AsyncHttpClient


async def main():
    async with AsyncHttpClient() as client:
        html = await client.get("https://example.com")
        print("Got:", len(html), "chars")

        # If the API correctly sets the content type to application/json we parse it
        # automatically. Else set parse_json=True
        json_data = await client.get("https://jsonplaceholder.typicode.com/todos/1")
        print("JSON data:", json_data)


asyncio.run(main())
