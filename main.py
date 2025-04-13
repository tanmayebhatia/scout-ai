import argparse
import logging
import json
from pyairtable import Api
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from src.enricher import LinkedInEnricher
from src.utils import setup_logging, parse_openai_response
import asyncio
import aiohttp
from openai import AsyncOpenAI
import time
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from typing import AsyncGenerator
from fastapi.middleware.cors import CORSMiddleware
from src.single_record_enricher import enrich_single_profile
import os
import uvicorn
from dotenv import load_dotenv

load_dotenv()

# Create FastAPI app
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update with your Vercel URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def enrich_profiles(table, batch_size=50, max_records=None):
    """Step 1: Enrich profiles with ProxyCurl"""
    enricher = LinkedInEnricher()
    
    print("Fetching records from Airtable...")
    all_records = table.all(max_records=max_records)
    
    # Filter records needing enrichment
    to_enrich = [
        record for record in all_records 
        if 'Raw_Enriched_Data' not in record['fields']
        and 'linkedin_url' in record['fields']
        and enricher.is_valid_profile_url(record['fields']['linkedin_url'])
    ]
    
    print(f"\nFound {len(to_enrich)} profiles to enrich")
    
    # Process in batches
    enriched_data = []
    for i in range(0, len(to_enrich), batch_size):
        batch = to_enrich[i:i + batch_size]
        print(f"\nEnriching batch {i//batch_size + 1} of {len(to_enrich)//batch_size + 1}")
        
        for record in tqdm(batch):
            linkedin_url = record['fields']['linkedin_url']
            data = enricher.enrich_profile(linkedin_url)
            if data:
                enriched_data.append({
                    'record_id': record['id'],
                    'data': data
                })
        
        # Update Airtable with raw data only
        updates = [
            {
                'id': item['record_id'],
                'fields': {
                    'Raw_Enriched_Data': json.dumps(item['data'])
                }
            } 
            for item in enriched_data
        ]
        
        if updates:
            table.batch_update(updates)
            print(f"Updated {len(updates)} records with raw data")
        
        enriched_data = []  # Clear for next batch

async def analyze_profile_async(client, enricher, record, semaphore):
    """Analyze a single profile with OpenAI"""
    max_retries = 5  # Increased from 3
    base_delay = 1.0  # Increased from 0.1 to 1 second
    
    async with semaphore:
        for attempt in range(max_retries):
            try:
                raw_data = json.loads(record['fields']['Raw_Enriched_Data'])
                analysis = await enricher.analyze_with_openai_async(client, raw_data)
                
                if analysis:
                    companies, summary, location = parse_openai_response(analysis)
                    return {
                        'id': record['id'],
                        'fields': {
                            'Previous_Companies': companies,
                            'AI_Summary': summary,
                            'Location': location
                        }
                    }
                break
                
            except Exception as e:
                if "429" in str(e) and attempt < max_retries - 1:
                    delay = base_delay * (4 ** attempt)  # More aggressive exponential backoff
                    print(f"Rate limit hit, retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    logging.error(f"Error analyzing profile: {str(e)}")
                    return None

async def analyze_profiles_async(table, batch_size=50, max_records=None):
    """Analyze profiles concurrently with OpenAI"""
    enricher = LinkedInEnricher()
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    semaphore = asyncio.Semaphore(25)  # Reduced from 50 to 25
    
    # Get records with raw data but no analysis
    all_records = table.all(max_records=max_records)
    print(f"\nTotal records: {len(all_records)}")
    
    # Debug counts
    has_raw_data = [r for r in all_records if 'Raw_Enriched_Data' in r['fields']]
    has_ai_summary = [r for r in all_records if 'AI_Summary' in r['fields']]
    has_companies = [r for r in all_records if 'Previous_Companies' in r['fields']]
    
    print(f"Records with Raw_Enriched_Data: {len(has_raw_data)}")
    print(f"Records with AI_Summary: {len(has_ai_summary)}")
    print(f"Records with Previous_Companies: {len(has_companies)}")
    
    to_analyze = [
        record for record in all_records
        if 'Raw_Enriched_Data' in record['fields']
        and ('AI_Summary' not in record['fields'] or 'Previous_Companies' not in record['fields'])
    ]
    
    print(f"\nFound {len(to_analyze)} profiles to analyze")
    
    # Track total processed
    total_processed = 0
    start_time = time.time()
    
    # Process in batches
    for i in range(0, len(to_analyze), batch_size):
        batch = to_analyze[i:i + batch_size]
        batch_start = time.time()
        print(f"\nAnalyzing batch {i//batch_size + 1} of {len(to_analyze)//batch_size + 1}")
        
        # Process batch concurrently
        tasks = [
            analyze_profile_async(client, enricher, record, semaphore)
            for record in batch
        ]
        
        results = await tqdm_asyncio.gather(*tasks)
        updates = [r for r in results if r is not None]
        
        # Update Airtable
        if updates:
            table.batch_update(updates)
            total_processed += len(updates)
            batch_time = time.time() - batch_start
            total_time = time.time() - start_time
            
            print(f"\nBatch Statistics:")
            print(f"- Processed {len(updates)} profiles in {batch_time:.1f} seconds")
            print(f"- Rate: {len(updates)/batch_time:.1f} profiles/second")
            print(f"\nOverall Progress:")
            print(f"- Total processed: {total_processed} of {len(to_analyze)}")
            print(f"- Average rate: {total_processed/total_time:.1f} profiles/second")
            print(f"- Time elapsed: {total_time/60:.1f} minutes")
            
            # Add cooling period between batches
            print("\nCooling down for 5 seconds before next batch...")
            await asyncio.sleep(5)

def main():
    parser = argparse.ArgumentParser(description='LinkedIn Profile Enricher')
    parser.add_argument('--batch-size', type=int, default=50)
    parser.add_argument('--max-records', type=int)
    parser.add_argument('--step', choices=['enrich', 'analyze', 'both'], default='both')
    args = parser.parse_args()
    
    setup_logging()
    api = Api(os.getenv("AIRTABLE_API_KEY"))
    table = api.table(
        os.getenv("AIRTABLE_BASE_ID"),
        os.getenv("AIRTABLE_TABLE_NAME")
    )
    
    if args.step in ['enrich', 'both']:
        enrich_profiles(table, args.batch_size, args.max_records)
    if args.step in ['analyze', 'both']:
        asyncio.run(analyze_profiles_async(table, args.batch_size, args.max_records))


async def process_single_profile(linkedin_url: str) -> AsyncGenerator[str, None]:
    """Process single LinkedIn URL with real-time status updates"""
    try:
        # Step 1: Initialize clients
        yield "Initializing clients...\n"
        enricher = LinkedInEnricher()
        airtable = Api(os.getenv("AIRTABLE_API_KEY"))
        table = airtable.table(os.getenv("AIRTABLE_BASE_ID"), os.getenv("AIRTABLE_TABLE_NAME"))
        openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        embedder = ProfileEmbedder()
        yield "✅ Clients initialized successfully\n"

        # Step 2: Check if URL exists
        yield "Checking if profile exists in database...\n"
        records = table.all(formula=f"{{linkedin_url}} = '{linkedin_url}'")
        if records:
            yield "⚠️ Profile already exists in database\n"
            return

        # Step 3: Enrich with Proxy Curl
        yield "Enriching profile with Proxy Curl...\n"
        enriched_data = await enricher.enrich_profile(linkedin_url)
        if not enriched_data or enriched_data in ["No LinkedIn URL provided", "LinkedIn profile not found (404)"]:
            yield "❌ Failed to enrich profile\n"
            raise HTTPException(status_code=400, detail="Failed to enrich profile")
        yield "✅ Profile enriched successfully\n"

        # Step 4: Create Airtable record
        yield "Creating record in Airtable...\n"
        airtable_record = table.create({
            'linkedin_url': linkedin_url,
            'Raw_Enriched_Data': json.dumps(enriched_data)
        })
        yield f"✅ Created Airtable record: {airtable_record['id']}\n"

        # Step 5: Analyze with OpenAI
        yield "Analyzing profile with OpenAI...\n"
        analysis = await analyze_with_openai(openai_client, enriched_data)
        if analysis:
            companies, summary, location = parse_openai_response(analysis)
            table.update(airtable_record['id'], {
                'Previous_Companies': companies,
                'AI_Summary': summary,
                'Location': location
            })
            yield "✅ OpenAI analysis complete and stored\n"
        else:
            yield "⚠️ OpenAI analysis failed\n"

        # Step 6: Create and store embedding
        yield "Creating embedding...\n"
        text = embedder.prepare_text_for_embedding(airtable_record)
        if text:
            embedding = await embedder.get_embedding(text)
            metadata = embedder.prepare_metadata(airtable_record)
            if embedding and metadata:
                embedder.pinecone_index.upsert(
                    vectors=[(airtable_record['id'], embedding, metadata)]
                )
                yield "✅ Embedding created and stored in Pinecone\n"
            else:
                yield "⚠️ Failed to create embedding\n"

        yield "🎉 Processing complete!\n"

    except Exception as e:
        yield f"❌ Error: {str(e)}\n"
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/process-profile")
async def process_profile(linkedin_url: str):
    return StreamingResponse(
        process_single_profile(linkedin_url),
        media_type='text/event-stream'
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080)