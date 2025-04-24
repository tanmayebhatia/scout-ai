import argparse
import logging
import json
from pyairtable import Api
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from src.enricher import LinkedInEnricher
from src.utils import setup_logging, parse_openai_response, extract_fields_from_enriched_data
from src.embedder import ProfileEmbedder
from src.analyze_enriched_records import generate_embedding_summary
import asyncio
import aiohttp
from openai import AsyncOpenAI
import time
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from typing import AsyncGenerator
from fastapi.middleware.cors import CORSMiddleware
from src.single_record_enricher import enrich_single_profile
import os
import uvicorn
from dotenv import load_dotenv
from src.scout_slackbot import ScoutSlackBot
import threading

load_dotenv()

# Create FastAPI app
app = FastAPI()

# Initialize Scout bot
scout_bot = ScoutSlackBot()

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
        if '⚓️ Raw_Enriched_Data' not in record['fields']
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
                    '⚓️ Raw_Enriched_Data': json.dumps(item['data'])
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
                raw_data = json.loads(record['fields']['⚓️ Raw_Enriched_Data'])
                analysis = await enricher.analyze_with_openai_async(client, raw_data)
                
                if analysis:
                    companies, summary, location = parse_openai_response(analysis)
                    return {
                        'id': record['id'],
                        'fields': {
                            '⚓️ Past Roles': companies,
                            '⚓️ embedding_summary': summary,
                            '⚓️ Location': location
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
    has_raw_data = [r for r in all_records if '⚓️ Raw_Enriched_Data' in r['fields']]
    has_ai_summary = [r for r in all_records if '⚓️ embedding_summary' in r['fields']]
    has_companies = [r for r in all_records if '⚓️ Past Roles' in r['fields']]
    
    print(f"Records with ⚓️ Raw_Enriched_Data: {len(has_raw_data)}")
    print(f"Records with ⚓️ embedding_summary: {len(has_ai_summary)}")
    print(f"Records with ⚓️ Past Roles: {len(has_companies)}")
    
    to_analyze = [
        record for record in all_records
        if '⚓️ Raw_Enriched_Data' in record['fields']
        and ('⚓️ embedding_summary' not in record['fields'] or '⚓️ Past Roles' not in record['fields'])
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
        # Pre-validation before starting to stream
        if not linkedin_url or not linkedin_url.strip():
            # Don't stream anything, just raise the exception
            raise HTTPException(status_code=400, detail="LinkedIn URL is empty or missing")
            
        # Initialize clients for validation (without yielding)
        enricher = LinkedInEnricher()
        airtable = Api(os.getenv("AIRTABLE_API_KEY"))
        table = airtable.table(os.getenv("AIRTABLE_BASE_ID"), os.getenv("AIRTABLE_TABLE_NAME"))
        
        # Validate URL format
        if not enricher.is_valid_profile_url(linkedin_url):
            raise HTTPException(status_code=400, detail="Invalid LinkedIn URL format")
            
        # Check if URL already exists in database
        records = table.all(formula=f"{{linkedin_url}} = '{linkedin_url}'")
        if records:
            # This is not an error, but we'll handle it early
            yield "⚠️ Profile already exists in database\n"
            return
            
        # Pre-validate API key
        proxycurl_key = os.getenv("PROXYCURL_API_KEY")
        if not proxycurl_key:
            raise HTTPException(status_code=500, detail="PROXYCURL_API_KEY is missing")
        
        # Now begin the regular streaming process
        # Step 1: Initialize clients (we already initialized some, but reinitialize for consistency)
        yield "Initializing clients...\n"
        openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        embedder = ProfileEmbedder()
        yield "✅ Clients initialized\n"

        # Step 2: Check if URL exists (we already did this, but keep the message for the client)
        yield "Checking if profile exists in database...\n"
        yield "✅ Profile not found in database\n"

        # Step 3: Enrich with Proxy Curl
        yield "Enriching profile with ProxyCurl...\n"
        enriched_data = await enricher.enrich_profile(linkedin_url)

        # Check if the response is an error message (string) - now inside the stream
        if isinstance(enriched_data, str):
            yield f"❌ Failed to enrich profile: {enriched_data}\n"
            return  # Just return instead of raising an exception
            
        # Check if the response is empty - now inside the stream
        if not enriched_data:
            yield "❌ Failed to enrich profile: No data returned\n"
            return  # Just return instead of raising an exception

        yield "✅ Profile enriched successfully\n"

        # Step 4: Extract data directly from the enriched data using the utility function
        yield "Extracting profile data...\n"
        extracted_fields = extract_fields_from_enriched_data(enriched_data, linkedin_url)
        
        # Check if extraction was successful
        if not extracted_fields:
            yield "❌ Failed to extract profile data\n"
            return  # Just return instead of raising an exception
            
        yield f"✅ Extracted {len(extracted_fields)} fields\n"
        
        # Step 5: Create record in Airtable with extracted data
        yield "Creating record in Airtable...\n"
        try:
            airtable_record = table.create(extracted_fields)
            if not airtable_record or 'id' not in airtable_record:
                yield "❌ Failed to create Airtable record\n"
                return  # Just return instead of raising an exception
                
            record_id = airtable_record['id']
            yield f"✅ Created Airtable record\n"
        except Exception as e:
            yield f"❌ Error creating Airtable record: {str(e)}\n"
            return  # Just return instead of raising an exception

        # Step 6: Generate embedding summary
        yield "Generating embedding summary...\n"
        try:
            embedding_summary = await generate_embedding_summary(openai_client, enriched_data)
            
            if embedding_summary:
                # Update the record with the embedding summary
                table.update(record_id, {
                    '⚓️ embedding_summary': embedding_summary
                })
                yield "✅ Added embedding summary\n"
            else:
                yield "⚠️ Could not generate embedding summary\n"
        except Exception as e:
            yield f"⚠️ Error generating embedding summary: {str(e)}\n"
            # Continue anyway - this is non-critical

        # Step 7: Create vector embedding
        yield "Creating vector embedding...\n"
        try:
            # Get the updated record to ensure we have all fields
            updated_record = table.get(record_id)
            if not updated_record:
                yield "⚠️ Could not retrieve updated record for embedding\n"
                return
                
            text = embedder.prepare_text_for_embedding(updated_record)
            if text:
                embedding = await embedder.get_embedding(text)
                metadata = embedder.prepare_metadata(updated_record)
                if embedding and metadata:
                    embedder.pinecone_index.upsert(
                        vectors=[(record_id, embedding, metadata)]
                    )
                    yield "✅ Embedding created and stored\n"
                else:
                    yield "⚠️ Could not create embedding metadata\n"
            else:
                yield "⚠️ Could not prepare text for embedding\n"
        except Exception as e:
            logging.error(f"Error creating embedding: {str(e)}")
            yield f"⚠️ Error creating embedding: {str(e)}\n"

        # All done!
        yield "🎉 Profile processed successfully!\n"

    except HTTPException as he:
        # Let FastAPI handle HTTP exceptions without streaming
        raise he
    except Exception as e:
        logging.error(f"Error in process_single_profile: {str(e)}")
        # For other exceptions during validation, raise HTTPException
        raise HTTPException(status_code=500, detail=f"Error processing profile: {str(e)}")

@app.post("/api/process-profile")
async def process_profile(linkedin_url: str):
    return StreamingResponse(
        process_single_profile(linkedin_url),
        media_type='text/event-stream'
    )

@app.post("/api/process-all-records")
async def process_all_records(batch_size: int = 10, max_concurrent: int = 5):
    """
    Process all records that need enrichment:
    1. Find records with valid LinkedIn URLs but missing/invalid enrichment data
    2. Process them using the single record enricher flow with concurrency control
    3. Stream progress updates back to the client
    """
    return StreamingResponse(
        batch_process_profiles(batch_size, max_concurrent),
        media_type='text/event-stream'
    )

async def batch_process_profiles(batch_size: int = 10, max_concurrent: int = 5) -> AsyncGenerator[str, None]:
    """Process all records that need enrichment with real-time updates using a phase-based approach"""
    try:
        # Pre-validation before streaming
        proxycurl_key = os.getenv("PROXYCURL_API_KEY")
        if not proxycurl_key:
            raise HTTPException(status_code=500, detail="PROXYCURL_API_KEY is missing")
            
        airtable_key = os.getenv("AIRTABLE_API_KEY")
        if not airtable_key:
            raise HTTPException(status_code=500, detail="AIRTABLE_API_KEY is missing")
            
        airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
        if not airtable_base_id:
            raise HTTPException(status_code=500, detail="AIRTABLE_BASE_ID is missing")
            
        airtable_table_name = os.getenv("AIRTABLE_TABLE_NAME")
        if not airtable_table_name:
            raise HTTPException(status_code=500, detail="AIRTABLE_TABLE_NAME is missing")

        # Now begin streaming
        # Initialize clients
        yield "Initializing clients...\n"
        api = Api(airtable_key)
        table = api.table(airtable_base_id, airtable_table_name)
        enricher = LinkedInEnricher()
        openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        embedder = ProfileEmbedder()
        yield "✅ Clients initialized\n"
        
        # Find records that need processing
        yield "Fetching records from Airtable...\n"
        all_records = table.all()
        yield f"Found {len(all_records)} total records\n"
        
        # Filter records that need enrichment
        to_process = []
        linkedin_url_field = "linkedin_url"
        raw_data_field = "⚓️ Raw_Enriched_Data"
        embedding_field = "⚓️ embedding_summary"
        
        for record in all_records:
            # Check if record has a LinkedIn URL
            linkedin_url = record.get('fields', {}).get(linkedin_url_field)
            
            # Check if Raw_Enriched_Data is missing or empty
            raw_data = record.get('fields', {}).get(raw_data_field, '')
            needs_enrichment = False
            
            if linkedin_url:
                # If URL exists, check if raw data is missing or empty
                if not raw_data:
                    needs_enrichment = True
            
            if needs_enrichment:
                to_process.append((record['id'], linkedin_url))
        
        yield f"Found {len(to_process)} records needing enrichment\n"
        
        if not to_process:
            yield "✅ No records need enrichment. All done!\n"
            return
            
        # ============= PHASE 1: URL VALIDATION =============
        yield "\n📋 PHASE 1: URL Validation\n"
        valid_records = []
        invalid_records = []
        
        for record_id, url in to_process:
            # Clean URL by trimming whitespace
            if url:
                url = url.strip()
                
            if not url:
                invalid_records.append((record_id, "No LinkedIn Provided"))
            elif not enricher.is_valid_profile_url(url):
                invalid_records.append((record_id, "Invalid LinkedIn URL"))
            else:
                valid_records.append((record_id, url))
        
        # Update invalid records in Airtable with error message
        if invalid_records:
            for record_id, error_msg in invalid_records:
                table.update(record_id, {
                    raw_data_field: error_msg
                })
            yield f"Updated {len(invalid_records)} records with URL validation errors\n"
            
        yield f"URL Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid\n"
        
        if not valid_records:
            yield "✅ No valid records to process. All done!\n"
            return
            
        # ============= PHASE 2: PROXYCURL ENRICHMENT =============
        yield "\n📋 PHASE 2: ProxyCurl Enrichment\n"
        enrichment_results = []
        semaphore = asyncio.Semaphore(max_concurrent)
        
        # Process enrichment in small batches to avoid overloading
        enrichment_batch_size = min(batch_size, 5)  # Smaller batch size for API calls
        total_enrichment_batches = (len(valid_records) + enrichment_batch_size - 1) // enrichment_batch_size
        
        for batch_idx in range(0, len(valid_records), enrichment_batch_size):
            batch = valid_records[batch_idx:batch_idx + enrichment_batch_size]
            yield f"\n🔄 Processing enrichment batch {batch_idx//enrichment_batch_size + 1} of {total_enrichment_batches} ({len(batch)} records)\n"
            
            batch_results = []
            batch_success = 0
            batch_failed = 0
            
            # Process each record in the batch
            for record_id, url in batch:
                async with semaphore:
                    try:
                        enriched_data = await enricher.enrich_profile(url)
                        
                        # Check if the response is an error message (string)
                        if isinstance(enriched_data, str):
                            error_msg = enriched_data
                            if "401" in error_msg:
                                error_msg = "LinkedIn profile not found (401 error)"
                            elif "404" in error_msg:
                                error_msg = "LinkedIn profile not found (404 error)"
                            batch_results.append((record_id, error_msg))
                            batch_failed += 1
                            continue
                        
                        # Check if the response is empty
                        if not enriched_data:
                            batch_results.append((record_id, "Error: No data returned"))
                            batch_failed += 1
                            continue
                        
                        # Check if the response has expected structure
                        if not isinstance(enriched_data, dict):
                            batch_results.append((record_id, "Error: Invalid response format"))
                            batch_failed += 1
                            continue
                            
                        # Check if the record exists (has a name field or similar)
                        if 'full_name' not in enriched_data and 'name' not in enriched_data:
                            batch_results.append((record_id, "Error: Profile not found or incomplete data"))
                            batch_failed += 1
                            continue
                        
                        batch_results.append((record_id, enriched_data))
                        batch_success += 1
                        
                    except Exception as e:
                        error_str = str(e)
                        error_msg = "Error during enrichment"
                        
                        # Categorize common errors with more user-friendly messages
                        if "401" in error_str:
                            error_msg = "LinkedIn profile not found (401 error)"
                        elif "404" in error_str:
                            error_msg = "LinkedIn profile not found (404 error)"
                        elif "429" in error_str:
                            error_msg = "Rate limit exceeded (429 error)"
                        elif "timeout" in error_str.lower():
                            error_msg = "Request timeout"
                        else:
                            error_msg = f"Error: {error_str[:100]}"  # Limit error message length
                            
                        batch_results.append((record_id, error_msg))
                        batch_failed += 1
            
            # Update Airtable with results immediately
            for record_id, result in batch_results:
                if isinstance(result, dict):
                    # Success case - store the JSON data
                    table.update(record_id, {
                        raw_data_field: json.dumps(result)
                    })
                else:
                    # Error case - store the error message directly
                    table.update(record_id, {
                        raw_data_field: result
                    })
            
            yield f"Batch results: {batch_success} successful, {batch_failed} failed\n"
            
            enrichment_results.extend(batch_results)
            
            # Add a delay between batches to respect rate limits
            if batch_idx + enrichment_batch_size < len(valid_records):
                yield "Cooling down for 3 seconds before next batch...\n"
                await asyncio.sleep(3)
        
        # Process enrichment results to separate successes and failures
        successful_enrichments = []
        failed_enrichments = []
        
        for record_id, result in enrichment_results:
            if isinstance(result, dict):
                successful_enrichments.append((record_id, result))
            else:
                failed_enrichments.append((record_id, result))
        
        yield f"Enrichment complete: {len(successful_enrichments)} successful, {len(failed_enrichments)} failed\n"
        
        if not successful_enrichments:
            yield "✅ No successful enrichments to process further. All done!\n"
            return
            
        # ============= PHASE 3: FIELD EXTRACTION AND AIRTABLE UPDATE =============
        yield "\n📋 PHASE 3: Field Extraction and Airtable Update\n"
        extraction_results = []
        extraction_success = 0
        extraction_failed = 0
        
        for record_id, enriched_data in successful_enrichments:
            try:
                extracted_fields = extract_fields_from_enriched_data(enriched_data, None)  # URL already included in enriched_data
                
                # Raw data already stored in the enrichment phase
                # Just add the extracted fields
                
                # Update record in Airtable
                table.update(record_id, extracted_fields)
                extraction_results.append((record_id, extracted_fields))
                extraction_success += 1
                
            except Exception as e:
                extraction_failed += 1
                # Don't update Raw_Enriched_Data as it already contains the valid JSON
                # Just log the error and continue
        
        yield f"Field extraction complete: {extraction_success} successful, {extraction_failed} failed\n"
        
        # ============= PHASE 4: EMBEDDING SUMMARY GENERATION =============
        yield "\n📋 PHASE 4: Embedding Summary Generation\n"
        summary_batch_size = min(batch_size, 10)  # Adjust batch size for OpenAI calls
        total_summary_batches = (len(extraction_results) + summary_batch_size - 1) // summary_batch_size
        
        summary_success = 0
        summary_failed = 0
        
        for batch_idx in range(0, len(extraction_results), summary_batch_size):
            batch = extraction_results[batch_idx:batch_idx + summary_batch_size]
            yield f"\n🔄 Processing summary batch {batch_idx//summary_batch_size + 1} of {total_summary_batches} ({len(batch)} records)\n"
            
            batch_success = 0
            batch_failed = 0
            
            for record_id, extracted_fields in batch:
                if embedding_field not in extracted_fields or not extracted_fields[embedding_field]:
                    # Get the record to access the raw data
                    record = table.get(record_id)
                    raw_data = record['fields'].get(raw_data_field, '')
                    
                    if raw_data and raw_data.startswith('{'):
                        try:
                            enriched_data = json.loads(raw_data)
                            
                            async with semaphore:
                                try:
                                    embedding_summary = await generate_embedding_summary(openai_client, enriched_data)
                                    
                                    if embedding_summary:
                                        table.update(record_id, {embedding_field: embedding_summary})
                                        batch_success += 1
                                    else:
                                        batch_failed += 1
                                            
                                except Exception as e:
                                    batch_failed += 1
                        except json.JSONDecodeError:
                            batch_failed += 1
            
            summary_success += batch_success
            summary_failed += batch_failed
            
            yield f"Batch results: {batch_success} successful, {batch_failed} failed\n"
            
            # Add a delay between batches
            if batch_idx + summary_batch_size < len(extraction_results):
                yield "Cooling down for 2 seconds before next batch...\n"
                await asyncio.sleep(2)
        
        yield f"Summary generation complete: {summary_success} successful, {summary_failed} failed\n"
        
        # ============= PHASE 5: VECTOR EMBEDDING CREATION =============
        yield "\n📋 PHASE 5: Vector Embedding Creation\n"
        embedding_batch_size = min(batch_size, 20)  # Adjust batch size for embedding creation
        
        # Get updated records with embedding summary
        record_ids = [record_id for record_id, _ in extraction_results]
        updated_records = [table.get(record_id) for record_id in record_ids]
        
        successful_embeddings = 0
        failed_embeddings = 0
        
        total_embedding_batches = (len(updated_records) + embedding_batch_size - 1) // embedding_batch_size
        
        for batch_idx in range(0, len(updated_records), embedding_batch_size):
            batch = updated_records[batch_idx:batch_idx + embedding_batch_size]
            yield f"\n🔄 Processing embedding batch {batch_idx//embedding_batch_size + 1} of {total_embedding_batches} ({len(batch)} records)\n"
            
            batch_success = 0
            batch_failed = 0
            
            for record in batch:
                record_id = record['id']
                try:
                    text = embedder.prepare_text_for_embedding(record)
                    
                    if text:
                        embedding = await embedder.get_embedding(text)
                        metadata = embedder.prepare_metadata(record)
                        
                        if embedding and metadata:
                            embedder.pinecone_index.upsert(
                                vectors=[(record_id, embedding, metadata)]
                            )
                            successful_embeddings += 1
                            batch_success += 1
                        else:
                            failed_embeddings += 1
                            batch_failed += 1
                    else:
                        failed_embeddings += 1
                        batch_failed += 1
                        
                except Exception as e:
                    failed_embeddings += 1
                    batch_failed += 1
            
            yield f"Batch results: {batch_success} successful, {batch_failed} failed\n"
            
            # Add a delay between batches
            if batch_idx + embedding_batch_size < len(updated_records):
                yield "Cooling down for 1 second before next batch...\n"
                await asyncio.sleep(1)
                
        yield f"Embedding creation complete: {successful_embeddings} successful, {failed_embeddings} failed\n"
        
        # Final summary
        yield f"\n===== ENRICHMENT PROCESS COMPLETE =====\n"
        yield f"Total records processed: {len(to_process)}\n"
        yield f"Valid URLs: {len(valid_records)}\n"
        yield f"Invalid URLs: {len(invalid_records)}\n"
        yield f"Successful enrichments: {len(successful_enrichments)}\n"
        yield f"Failed enrichments: {len(failed_enrichments)}\n"
        yield f"Successful embeddings: {successful_embeddings}\n"
        yield f"Failed embeddings: {failed_embeddings}\n"
        yield f"Overall success rate: {successful_embeddings/len(to_process)*100:.1f}%\n"
        yield "🎉 Done!\n"
        
    except Exception as e:
        yield f"❌ Error during batch processing: {str(e)}\n"
        logging.error(f"Error during batch processing: {str(e)}")
        logging.exception("Detailed traceback:")

@app.get("/")
async def root():
    return {"status": "ok"}

# Keep the HTTP endpoint for Railway
@app.post("/slack/events")
async def slack_events(request: Request):
    return await scout_bot.http_handler.handle(request)

@app.on_event("startup")
async def startup_event():
    await scout_bot.setup()  # Setup async components
    asyncio.create_task(scout_bot.start())
    print("🤖 Scout bot socket mode activated")

@app.on_event("shutdown")
async def shutdown_event():
    await scout_bot.cleanup()
    print("🤖 Scout bot shutdown complete")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=port,
        workers=1  # Important for WebSocket connections
    )