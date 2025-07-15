import asyncio
from app.collectors.unified_kto_client import UnifiedKTOClient

async def main():
    client = UnifiedKTOClient()
    result = await client.collect_all_data(content_types=["28"])
    print("레저스포츠 수집 결과:", result)

if __name__ == "__main__":
    asyncio.run(main())
