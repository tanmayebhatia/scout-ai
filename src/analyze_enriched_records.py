import json
import time
import asyncio
from tqdm import tqdm
from pyairtable import Api
import logging
from openai import AsyncOpenAI
import sys
import os
from asyncio import Semaphore
from dotenv import load_dotenv

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Initialize logging
logging.basicConfig(level=logging.INFO)

load_dotenv()

async def analyze_with_openai(client, profile_data):
    """Analyze profile with OpenAI"""
    try:
        profile_summary = {
            'full_name': profile_data.get('full_name', ''),
            'headline': profile_data.get('headline', ''),
            'experiences': [
                {
                    'company': exp.get('company', ''),
                    'title': exp.get('title', ''),
                    'duration': exp.get('duration', '')
                } for exp in profile_data.get('experiences', [])
            ],
            'city': profile_data.get('city', ''),
            'state': profile_data.get('state', ''),
            'country': profile_data.get('country', '')
        }
        
        response = await client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a professional career analyst specializing in the VC and startup ecosystem."},
                {"role": "user", "content": get_analysis_prompt(profile_summary)}
            ],
            temperature=0.7
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logging.error(f"Error in OpenAI analysis: {str(e)}")
        return None

def get_analysis_prompt(profile_summary):
    """Using existing prompt"""
    return f"""
    Analyze this LinkedIn profile data and provide:
    1. A comma-separated list of all previous companies (including the current company and current title)
    2. A summary of their background and experience as it relates to a VC and startup ecosystem. Keep the context rich and detailed, 3-4 sentences. Also CLASSIFY them as best you can between Founder, Expert, Investor, Operator, or other.
    3. A concatenated location in the format: City, State, Country (include only available parts)

    Profile data:
    {json.dumps(profile_summary, indent=2)}

    Format your response exactly like this:
    COMPANIES: title at company1 (CURRENT),company2, company3
    SUMMARY: AI summary, CLASSIFICATION: Founder
    LOCATION: City, State, Country
    """

async def process_batch(table, client, batch, sem):
    """Process a batch of records with semaphore control"""
    successful = 0
    failed = 0
    
    async def process_single_record(record):
        async with sem:  # Use semaphore to control concurrency
            try:
                enriched_data = json.loads(record['fields']['Raw_Enriched_Data'])
                analysis = await analyze_with_openai(client, enriched_data)
                
                if analysis:
                    print(f"\nOpenAI Response for {record['id']}: {analysis}")
                    
                    companies, summary, location = parse_openai_response(analysis)
                    airtable_data = {
                        'Previous_Companies': companies,
                        'AI_Summary': summary,
                        'Location': location
                    }
                    
                    try:
                        table.update(record['id'], airtable_data)
                        return True, None
                    except Exception as e:
                        logging.error(f"Airtable update failed for record {record['id']}")
                        logging.error(f"Data being sent: {airtable_data}")
                        logging.error(f"Error details: {str(e)}")
                        return False, e
                else:
                    logging.error(f"Failed to get analysis for record {record['id']}")
                    return False, "No analysis returned"
                    
            except Exception as e:
                logging.error(f"Error processing record {record['id']}: {str(e)}")
                return False, e
    
    # Process all records in parallel but controlled by semaphore
    results = await asyncio.gather(
        *[process_single_record(record) for record in batch],
        return_exceptions=True
    )
    
    # Count successes and failures
    for result in results:
        if isinstance(result, tuple) and result[0]:
            successful += 1
        else:
            failed += 1
    
    print(f"\nBatch Results: {successful} successful, {failed} failed")

async def main():
    # Initialize clients
    api = Api(os.getenv("AIRTABLE_API_KEY"))
    table = api.table(os.getenv("AIRTABLE_BASE_ID"), os.getenv("AIRTABLE_TABLE_NAME"))
    openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    # Create semaphore to limit concurrent API calls
    sem = Semaphore(20)  # Limit to 20 concurrent requests

    print("\n=== Starting Analysis Process ===")
    print("Fetching records from Airtable...")
    records = table.all()
    total_records = len(records)
    print(f"Total records fetched: {total_records}")
    
    # Count records at each filter step
    has_raw_data = [r for r in records if r['fields'].get('Raw_Enriched_Data')]
    no_previous_companies = [
        r for r in has_raw_data 
        if not r['fields'].get('Previous_Companies')
    ]
    not_error_messages = [
        r for r in no_previous_companies 
        if r['fields']['Raw_Enriched_Data'] != "No LinkedIn URL provided"
        and r['fields']['Raw_Enriched_Data'] != "LinkedIn profile not found (404)"
    ]
    
    # Log filtering results
    print("\n=== Filtering Statistics ===")
    print(f"Records with Raw Enrichment Data: {len(has_raw_data)} ({len(has_raw_data)/total_records*100:.1f}%)")
    print(f"Records without Previous Companies: {len(no_previous_companies)} ({len(no_previous_companies)/total_records*100:.1f}%)")
    print(f"Records with valid enrichment data: {len(not_error_messages)} ({len(not_error_messages)/total_records*100:.1f}%)")
    
    to_analyze = not_error_messages
    print(f"\n=== Processing {len(to_analyze)} Records ===")
    
    # Process in batches
    batch_size = 50
    total_batches = (len(to_analyze) + batch_size - 1) // batch_size
    
    for i in range(0, len(to_analyze), batch_size):
        batch = to_analyze[i:i + batch_size]
        print(f"\nProcessing batch {i//batch_size + 1} of {total_batches}")
        print(f"Batch size: {len(batch)} records")
        await process_batch(table, openai_client, batch, sem)
    
    print("\n=== Analysis Complete ===")

if __name__ == "__main__":
    asyncio.run(main()) 