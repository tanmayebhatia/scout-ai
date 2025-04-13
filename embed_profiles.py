import asyncio
from src.embedder import ProfileEmbedder
from pyairtable import Api
from config.credentials import creds
from tqdm import tqdm

async def main():
    embedder = ProfileEmbedder()
    api = Api(creds.AIRTABLE_API_KEY)
    table = api.table(creds.AIRTABLE_BASE_ID, creds.AIRTABLE_TABLE_NAME)
    
    # Debug credentials
    print("\nChecking credentials:")
    print(f"Airtable API Key exists: {bool(creds.AIRTABLE_API_KEY)}")
    print(f"Base ID: {creds.AIRTABLE_BASE_ID}")
    print(f"Table name: {creds.AIRTABLE_TABLE_NAME}")
    
    # Get enriched records
    print("\nFetching enriched records...")
    try:
        records = table.all(formula="NOT({Raw_Enriched_Data} = '')")
        print(f"Found {len(records)} records")
        
        if not records:
            print("No records found!")
            return
            
        # Debug first record
        first_record = records[0]
        print("\nFirst record structure:")
        print(f"Keys: {first_record.keys()}")
        if 'fields' in first_record:
            print(f"Fields: {first_record['fields'].keys()}")
            print(f"Has Raw_Enriched_Data: {'Raw_Enriched_Data' in first_record['fields']}")
        
        # Show example of first record
        print("\nExample of first record embedding:")
        print("="*80)
        text = embedder.prepare_text_for_embedding(first_record)
        print(text)
        print("="*80)
        
        proceed = input("\nContinue with all records? (y/n): ")
        if proceed.lower() != 'y':
            return
        
        # Process all records
        for i in range(0, len(records), 50):
            batch = records[i:i + 50]
            print(f"\nBatch {i//50 + 1} of {len(records)//50 + 1}")
            
            tasks = [embedder.get_embedding(embedder.prepare_text_for_embedding(r)) for r in batch]
            embeddings = await asyncio.gather(*tasks)
            
            for r, emb in zip(batch, embeddings):
                if emb:
                    await embedder.store_in_supabase(r['id'], emb, embedder.prepare_text_for_embedding(r), r)
            
            await asyncio.sleep(2)
            
    except Exception as e:
        print(f"\nError: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 