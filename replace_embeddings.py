import asyncio
from pyairtable import Api
from src.embedder import ProfileEmbedder
from config.credentials import creds
from tqdm import tqdm

async def replace_all_embeddings(batch_size=50):
    """Replace all embeddings with new comprehensive format"""
    embedder = ProfileEmbedder()
    api = Api(creds.AIRTABLE_API_KEY)
    table = api.table(creds.AIRTABLE_BASE_ID, creds.AIRTABLE_TABLE_NAME)
    
    # Get all enriched records
    print("Fetching enriched records...")
    records = table.all(formula="NOT({Raw_Enriched_Data} = '')")
    print(f"Found {len(records)} enriched records")
    
    # Delete all existing embeddings with a WHERE clause
    print("Deleting existing embeddings...")
    await embedder.supabase.table('profile_embeddings').delete().neq('id', 0).execute()
    
    # Process in batches
    print("Creating new embeddings...")
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        print(f"\nProcessing batch {i//batch_size + 1} of {len(records)//batch_size + 1}")
        
        # Process batch concurrently
        tasks = []
        for record in batch:
            text = embedder.prepare_text_for_embedding(record)
            tasks.append(embedder.get_embedding(text))
        
        # Wait for all embeddings in batch
        embeddings = await asyncio.gather(*tasks)
        
        # Store successful embeddings
        for record, embedding in zip(batch, embeddings):
            if embedding:
                success = await embedder.store_in_supabase(
                    record['id'],
                    embedding,
                    embedder.prepare_text_for_embedding(record),
                    record
                )
                if not success:
                    print(f"Failed to store embedding for record {record['id']}")
        
        # Cool down between batches
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(replace_all_embeddings(batch_size=50)) 