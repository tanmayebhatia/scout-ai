import json
import time
import asyncio
from datetime import datetime
from tqdm import tqdm
from pyairtable import Api
import logging
from openai import AsyncOpenAI
import os
from asyncio import Semaphore
from dotenv import load_dotenv
import re

# Initialize logging
logging.basicConfig(level=logging.INFO)

load_dotenv()

def extract_current_role(profile_data):
    """Extract current role from profile data"""
    # First try occupation field
    if 'occupation' in profile_data and profile_data['occupation']:
        return profile_data['occupation']
        
    # Try to get most recent experience
    if 'experiences' in profile_data and profile_data['experiences']:
        # Sort experiences by start date, most recent first
        experiences = sorted(
            [e for e in profile_data['experiences'] if e.get('starts_at')],
            key=lambda x: (
                x['starts_at'].get('year', 0),
                x['starts_at'].get('month', 0),
                x['starts_at'].get('day', 0)
            ),
            reverse=True
        )
        
        if experiences:
            current_exp = experiences[0]
            title = current_exp.get('title', '')
            company = current_exp.get('company', '')
            if title and company:
                return f"{title} at {company}"
            elif title:
                return title
            elif company:
                return f"Works at {company}"
    
    return ""

def extract_past_roles(profile_data):
    """Extract past roles from profile data"""
    past_roles = []
    
    if 'experiences' in profile_data and profile_data['experiences']:
        # Sort experiences by start date, most recent first
        experiences = sorted(
            [e for e in profile_data['experiences'] if e.get('starts_at')],
            key=lambda x: (
                x['starts_at'].get('year', 0),
                x['starts_at'].get('month', 0),
                x['starts_at'].get('day', 0)
            ),
            reverse=True
        )
        
        # Skip first one if it's the current role
        start_idx = 1 if len(experiences) > 1 else 0
        
        for exp in experiences[start_idx:]:
            title = exp.get('title', '')
            company = exp.get('company', '')
            if title and company:
                past_roles.append(f"{title} at {company}")
            elif title:
                past_roles.append(title)
            elif company:
                past_roles.append(f"Worked at {company}")
    
    return past_roles

def extract_education(profile_data):
    """Extract education from profile data"""
    education_list = []
    
    if 'education' in profile_data and profile_data['education']:
        for edu in profile_data['education']:
            school = edu.get('school', '')
            degree = edu.get('degree_name', '')
            field = edu.get('field_of_study', '')
            
            edu_str = ""
            if degree and field and school:
                edu_str = f"{degree} in {field} from {school}"
            elif degree and school:
                edu_str = f"{degree} from {school}"
            elif school:
                edu_str = school
                
            if edu_str:
                education_list.append(edu_str)
    
    return education_list

def calculate_work_experience(profile_data):
    """Calculate years of work experience from profile data"""
    years = 0
    
    if 'experiences' in profile_data and profile_data['experiences']:
        # Get current year as the max end year
        current_year = datetime.now().year
        
        # Track start and end years, handling overlaps
        timeline = []
        
        for exp in profile_data['experiences']:
            # Skip experiences without start dates
            if not exp.get('starts_at') or not exp['starts_at'].get('year'):
                continue
                
            start_year = exp['starts_at'].get('year')
            
            # Handle end date
            if exp.get('ends_at') and exp['ends_at'].get('year'):
                end_year = exp['ends_at'].get('year')
            else:
                # If no end date, assume it's current (use current year)
                end_year = current_year
                
            timeline.append((start_year, end_year))
        
        # Sort by start year
        timeline.sort()
        
        # Merge overlapping periods
        if timeline:
            merged = [timeline[0]]
            
            for current in timeline[1:]:
                prev = merged[-1]
                
                # Check if current period overlaps with previous
                if current[0] <= prev[1]:
                    # Merge periods
                    merged[-1] = (prev[0], max(prev[1], current[1]))
                else:
                    # Add new period
                    merged.append(current)
            
            # Calculate total years
            for start, end in merged:
                years += end - start
    
    return years

def extract_location(profile_data):
    """Extract location from profile data"""
    location_parts = []
    
    if 'city' in profile_data and profile_data['city']:
        location_parts.append(profile_data['city'])
        
    if 'state' in profile_data and profile_data['state']:
        location_parts.append(profile_data['state'])
        
    if 'country_full_name' in profile_data and profile_data['country_full_name']:
        location_parts.append(profile_data['country_full_name'])
    elif 'country' in profile_data and profile_data['country']:
        location_parts.append(profile_data['country'])
        
    return ", ".join(location_parts)

def clean_url(text):
    """Remove URLs from text"""
    if not text:
        return ""
    # Remove URLs that start with http or https
    url_pattern = r'https?://\S+'
    return re.sub(url_pattern, '', text)

def extract_experience_details(profile_data):
    """Extract detailed information about experiences including descriptions"""
    detailed_experiences = []
    
    if 'experiences' in profile_data and profile_data['experiences']:
        # Sort experiences by start date, most recent first
        experiences = sorted(
            [e for e in profile_data['experiences'] if e.get('starts_at')],
            key=lambda x: (
                x['starts_at'].get('year', 0),
                x['starts_at'].get('month', 0),
                x['starts_at'].get('day', 0)
            ),
            reverse=True
        )
        
        for exp in experiences:
            title = exp.get('title', '')
            company = exp.get('company', '')
            description = clean_url(exp.get('description', ''))
            start_year = exp.get('starts_at', {}).get('year', '')
            
            # Handle end date
            if exp.get('ends_at') and exp['ends_at'].get('year'):
                end_year = exp['ends_at'].get('year')
                duration = f"{start_year}-{end_year}"
            else:
                duration = f"{start_year}-Present"
            
            if title and company:
                exp_details = {
                    "role": f"{title} at {company}",
                    "duration": duration,
                    "description": description
                }
                detailed_experiences.append(exp_details)
    
    return detailed_experiences

async def generate_embedding_summary(client, profile_data):
    """Generate embedding summary using GPT-4"""
    try:
        # Prepare the data
        name = profile_data.get('full_name', '')
        current_role = extract_current_role(profile_data)
        past_roles = extract_past_roles(profile_data)
        experience_years = calculate_work_experience(profile_data)
        education = extract_education(profile_data)
        location = extract_location(profile_data)
        summary = clean_url(profile_data.get('summary', ''))
        headline = clean_url(profile_data.get('headline', ''))
        
        # Get detailed experience information
        detailed_experiences = extract_experience_details(profile_data)
        
        # Create a profile summary for GPT-4
        profile_summary = {
            "name": name,
            "current_role": current_role,
            "headline": headline,
            "summary": summary,
            "location": location,
            "detailed_experiences": detailed_experiences,
        }
        
        # Call GPT-4
        response = await client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[
                {"role": "system", "content": "You are an expert at summarizing professional profiles for use in AI-powered search."},
                {"role": "user", "content": f"""
                You are an AI assistant generating concise summaries of professional profiles for use in vector search and semantic retrieval.
                Your output will be embedded, so it should be dense, fact-based, and avoid fluff or filler.
                
                Focus on:
                - Current role and company
                - Past roles and unique domain expertise
                - Any decision-making scope 
                - Specific industry terms 
                
                Here's the LinkedIn profile data:
                {json.dumps(profile_summary, indent=2)}
                
                Output a single paragraph of plain text optimized for semantic embedding. Do not output JSON or bullet points.
                Keep it under 150 tokens.
                """}
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        return response.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Error generating embedding summary: {str(e)}")
        return ""

async def process_batch(table, client, batch, sem):
    """Process a batch of records with semaphore control"""
    successful = 0
    failed = 0
    
    async def process_single_record(record):
        async with sem:  # Use semaphore to control concurrency
            try:
                # Check if Raw_Enriched_Data is a string (likely JSON)
                raw_data = record['fields'].get('Raw_Enriched_Data', '')
                
                # Verify that this is properly formatted JSON data starting with {"p
                if not raw_data or not isinstance(raw_data, str) or not raw_data.startswith('{"p'):
                    print(f"Skipping record {record['id']}: Invalid Raw_Enriched_Data format")
                    return False, "Invalid Raw_Enriched_Data format"
                
                # Parse JSON
                try:
                    enriched_data = json.loads(raw_data)
                except json.JSONDecodeError:
                    print(f"Skipping record {record['id']}: Invalid JSON in Raw_Enriched_Data")
                    return False, "Invalid JSON in Raw_Enriched_Data"
                
                # Extract data without ChatGPT (minimize data preparation time)
                current_role = extract_current_role(enriched_data)
                past_roles = extract_past_roles(enriched_data)
                work_experience = calculate_work_experience(enriched_data)
                education = extract_education(enriched_data)
                location = extract_location(enriched_data)
                
                # Generate embedding summary with GPT-3.5 (faster than GPT-4)
                embedding_summary = await generate_embedding_summary(client, enriched_data)
                
                if embedding_summary:
                    # Convert everything to strings to avoid data type issues
                    past_roles_str = ", ".join(past_roles[:5]) if past_roles else ""  # Limit to first 5
                    education_str = ", ".join(education) if education else ""
                    
                    # Prepare data for Airtable update, all as strings
                    airtable_data = {
                        '⚓️ Current Roles': str(current_role),
                        '⚓️ Past Roles': str(past_roles_str),
                        '⚓️ Work Experience (yrs)': str(work_experience),
                        '⚓️ Education': str(education_str),
                        '⚓️ embedding_summary': str(embedding_summary),
                        '⚓️ Location': str(location)
                    }
                    
                    # Try updating only essential fields first to reduce failure chance
                    try:
                        simplified_data = {
                            '⚓️ embedding_summary': str(embedding_summary),
                            '⚓️ Current Roles': str(current_role)
                        }
                        table.update(record['id'], simplified_data)
                        return True, None
                    except Exception as e:
                        logging.error(f"Airtable update failed for record {record['id']}")
                        logging.error(f"Error details: {str(e)}")
                        return False, e
                else:
                    logging.error(f"Failed to generate embedding summary for record {record['id']}")
                    return False, "No embedding summary generated"
                    
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
    
    return successful, failed

async def main():
    # Initialize clients
    api = Api(os.getenv("AIRTABLE_API_KEY"))
    table = api.table(os.getenv("AIRTABLE_BASE_ID"), os.getenv("AIRTABLE_TABLE_NAME"))
    openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    # Create semaphore to limit concurrent API calls (increased for GPT-3.5 which has higher rate limits)
    sem = Semaphore(15)  # Increased from 5 to 15 concurrent requests

    print("\n=== Starting Analysis Process ===")
    print("Fetching records from Airtable...")
    records = table.all()
    total_records = len(records)
    print(f"Total records fetched: {total_records}")
    
    # Filter records that need processing
    has_raw_data = [
        r for r in records 
        if r['fields'].get('⚓️ Raw_Enriched_Data') and 
        isinstance(r['fields'].get('⚓️ Raw_Enriched_Data'), str) and
        r['fields'].get('⚓️ Raw_Enriched_Data').startswith('{"p') and  # Only valid JSON format
        not r['fields'].get('⚓️ embedding_summary')  # Only process if no embedding summary yet
    ]
    
    # Log filtering results
    print("\n=== Filtering Statistics ===")
    print(f"Records to process: {len(has_raw_data)} of {total_records} ({len(has_raw_data)/total_records*100:.1f}%)")
    
    to_analyze = has_raw_data
    print(f"\n=== Processing {len(to_analyze)} Records ===")
    
    # Process in larger batches for GPT-3.5
    batch_size = 20  # Increased from 10 to 20
    total_batches = (len(to_analyze) + batch_size - 1) // batch_size
    
    total_successful = 0
    total_failed = 0
    
    # Create progress bar
    with tqdm(total=len(to_analyze), desc="Processing records") as pbar:
        for i in range(0, len(to_analyze), batch_size):
            batch = to_analyze[i:i + batch_size]
            print(f"\nProcessing batch {i//batch_size + 1} of {total_batches}")
            print(f"Batch size: {len(batch)} records")
            
            successful, failed = await process_batch(table, openai_client, batch, sem)
            total_successful += successful
            total_failed += failed
            
            # Update progress bar
            pbar.update(len(batch))
            pbar.set_postfix({"Success": successful, "Failed": failed})
            
            # Only add short delay to avoid rate limits
            if i + batch_size < len(to_analyze):
                await asyncio.sleep(2)  # Reduced from 10 to 2 seconds
    
    print(f"\n=== Analysis Complete ===")
    print(f"Total successful: {total_successful}")
    print(f"Total failed: {total_failed}")
    print(f"Success rate: {total_successful/len(to_analyze)*100:.1f}%")

if __name__ == "__main__":
    asyncio.run(main()) 