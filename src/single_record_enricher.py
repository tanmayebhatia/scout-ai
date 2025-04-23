import asyncio
import json
import sys
import os
from openai import AsyncOpenAI
from pyairtable import Api
from pinecone import Pinecone
import logging
from dotenv import load_dotenv
from src.enricher import LinkedInEnricher
from src.embedder import ProfileEmbedder
import re
from typing import Dict, Any, List, Tuple, Optional
import datetime

logging.basicConfig(level=logging.INFO)

load_dotenv()

async def extract_field_data(enriched_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract specific fields from enriched data to populate Airtable fields"""
    extracted = {}
    
    try:
        # Extract current role using the same logic as analyze_enriched_records.py
        current_role = ""
        
        # First try occupation field
        if enriched_data.get('occupation'):
            current_role = enriched_data.get('occupation')
        # Then try headline
        elif enriched_data.get('headline'):
            current_role = enriched_data.get('headline')
        # Then try job_title + company
        elif enriched_data.get('job_title'):
            current_role = enriched_data.get('job_title')
            if enriched_data.get('company'):
                current_role += f" at {enriched_data.get('company')}"
        # Finally try to get most recent experience
        elif enriched_data.get('experiences') and len(enriched_data.get('experiences')) > 0:
            # Sort experiences by start date, most recent first
            experiences = sorted(
                [e for e in enriched_data.get('experiences') if e.get('starts_at')],
                key=lambda x: (
                    x['starts_at'].get('year', 0),
                    x['starts_at'].get('month', 0) if x['starts_at'].get('month') else 0,
                    x['starts_at'].get('day', 0) if x['starts_at'].get('day') else 0
                ),
                reverse=True
            )
            
            if experiences:
                current_exp = experiences[0]
                title = current_exp.get('title', '')
                company = current_exp.get('company', '')
                if title and company:
                    current_role = f"{title} at {company}"
                elif title:
                    current_role = title
                elif company:
                    current_role = f"Works at {company}"
        
        if current_role:
            extracted['⚓️ Current Roles'] = current_role
            
        # Extract education list
        education_list = []
        if enriched_data.get('education'):
            for edu in enriched_data.get('education', []):
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
        
        if education_list:
            # Join as a string for better compatibility
            extracted['⚓️ Education'] = ", ".join(education_list)
        
        # Extract location using the same approach
        location_parts = []
        if enriched_data.get('location'):
            extracted['⚓️ Location'] = enriched_data.get('location')
        else:
            if enriched_data.get('city'):
                location_parts.append(enriched_data.get('city'))
                
            if enriched_data.get('state'):
                location_parts.append(enriched_data.get('state'))
                
            if enriched_data.get('country_full_name'):
                location_parts.append(enriched_data.get('country_full_name'))
            elif enriched_data.get('country'):
                location_parts.append(enriched_data.get('country'))
            
            if location_parts:
                extracted['⚓️ Location'] = ", ".join(location_parts)
            
        # Extract past roles
        past_roles = []
        if enriched_data.get('experiences'):
            # Sort experiences by start date, most recent first
            experiences = sorted(
                [e for e in enriched_data.get('experiences') if e.get('starts_at')],
                key=lambda x: (
                    x['starts_at'].get('year', 0),
                    x['starts_at'].get('month', 0) if x['starts_at'].get('month') else 0,
                    x['starts_at'].get('day', 0) if x['starts_at'].get('day') else 0
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
        
        if past_roles:
            extracted['⚓️ Past Roles'] = ", ".join(past_roles)
        
        # Calculate work experience years
        years = 0
        if enriched_data.get('experiences'):
            # Get current year
            current_year = datetime.datetime.now().year
            
            # Track start and end years, handling overlaps
            timeline = []
            
            for exp in enriched_data.get('experiences'):
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
                    
                # Sanity check years
                if start_year > 1900 and end_year <= current_year:
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
        
        if years > 0:
            extracted['⚓️ Work Experience (yrs)'] = years
        
        # Add raw enriched data
        extracted['⚓️ Raw_Enriched_Data'] = json.dumps(enriched_data)
        
        # Debug logs
        logging.info(f"Extracted {len(extracted)} fields from profile data")
        
    except Exception as e:
        logging.error(f"Error extracting fields from enriched data: {str(e)}")
        logging.exception("Detailed traceback:")
    
    return extracted

async def create_embedding_summary(client: AsyncOpenAI, enriched_data: Dict[str, Any]) -> str:
    """Create a concise summary of the profile for embedding"""
    try:
        prompt = f"""
        LinkedIn Profile data:
        {json.dumps(enriched_data)}
        
        You are an AI assistant generating concise summaries of professional profiles for use in vector search and semantic retrieval.
        Your output will be embedded, so it should be dense, fact-based, and avoid fluff or filler.
        
        Focus on:
        - Current role and company
        - Past roles and unique domain expertise
        - Any decision-making scope (e.g. GTM, platform strategy)
        - Industry terms (e.g. cloud security, POS, SaaS, AppSec)
        
        Output a single paragraph of plain text optimized for semantic embedding. Do not output JSON or bullet points.
        Keep it under 150 tokens.
        """
        
        response = await client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[
                {"role": "system", "content": "You are an expert at summarizing professional profiles for use in AI-powered search."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        return response.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Error creating embedding summary: {str(e)}")
        return ""

async def enrich_single_profile(linkedin_url: str):
    """Process a single LinkedIn URL through the entire pipeline"""
    try:
        logging.info("Initializing clients...")
        enricher = LinkedInEnricher()
        airtable = Api(os.getenv("AIRTABLE_API_KEY"))
        table = airtable.table(
            os.getenv("NETWORKS_HUB_BASE_ID") or os.getenv("AIRTABLE_BASE_ID"),
            os.getenv("NETWORKS_HUB_TABLE_NAME") or os.getenv("AIRTABLE_TABLE_NAME")
        )
        openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        embedder = ProfileEmbedder()
        logging.info("✅ Clients initialized")

        # Check if profile exists
        logging.info("Checking if profile exists...")
        try:
            records = table.all(formula=f"{{LinkedIn URL}} = '{linkedin_url}'")
            if records:
                logging.info("⚠️ Profile already exists in database")
                return records[0]['id']
        except Exception as e:
            logging.error(f"Error checking for existing profile: {str(e)}")
            # Continue anyway - we'll try to create a new record

        # Enrich with Proxy Curl
        logging.info("Enriching profile with Proxy Curl...")
        enriched_data = await enricher.enrich_profile(linkedin_url)
        if not enriched_data:
            logging.error("❌ Failed to enrich profile - no data returned")
            return None
        if isinstance(enriched_data, str) and "not found" in enriched_data.lower():
            logging.error(f"❌ Failed to enrich profile: {enriched_data}")
            return None
        logging.info("✅ Profile enriched successfully")

        # Extract fields from enriched data
        logging.info("Extracting fields from enriched data...")
        extracted_fields = await extract_field_data(enriched_data)
        
        # Add required fields that might not be in extracted_fields yet
        extracted_fields['LinkedIn URL'] = linkedin_url
        
        # Get name for record
        name = enriched_data.get('full_name', 'Unknown Profile')
        if name:
            extracted_fields['Name'] = name
        
        # Create embedding summary
        logging.info("Creating embedding summary...")
        embedding_summary = await create_embedding_summary(openai_client, enriched_data)
        if embedding_summary:
            extracted_fields['⚓️ embedding_summary'] = embedding_summary
            logging.info("✅ Embedding summary created")
            
        # Print exactly what we're sending to Airtable for debugging
        logging.info("Fields being sent to Airtable:")
        for key, value in extracted_fields.items():
            field_value = str(value)
            if len(field_value) > 100:
                field_value = field_value[:100] + "..."
            logging.info(f"  - {key}: {field_value}")
        
        # Create Airtable record
        logging.info("Creating Airtable record...")
        airtable_record = table.create(extracted_fields)
        record_id = airtable_record['id']
        logging.info(f"✅ Created Airtable record: {record_id}")
            
        # Create and store embedding
        logging.info("Creating embedding...")
        try:
            text = embedder.prepare_text_for_embedding({'fields': extracted_fields})
            if text:
                embedding = await embedder.get_embedding(text)
                metadata = embedder.prepare_metadata({'fields': extracted_fields})
                if embedding and metadata:
                    embedder.pinecone_index.upsert(
                        vectors=[(record_id, embedding, metadata)]
                    )
                    logging.info("✅ Embedding created and stored in Pinecone")
                else:
                    logging.warning("⚠️ Failed to create embedding - empty data")
            else:
                logging.warning("⚠️ Failed to prepare text for embedding")
        except Exception as e:
            logging.error(f"Error creating embedding: {str(e)}")
            # Continue anyway - we already created the record

        logging.info("🎉 Processing complete!")
        return record_id

    except Exception as e:
        logging.error(f"❌ Error processing profile: {str(e)}")
        return None

if __name__ == "__main__":
    print("\n=== LinkedIn Profile Enricher ===")
    linkedin_url = input("\nEnter LinkedIn URL: ").strip()
    
    if not linkedin_url:
        print("❌ No URL provided")
    elif "linkedin.com/in/" not in linkedin_url:
        print("❌ Invalid LinkedIn URL format. Should be like: https://www.linkedin.com/in/username")
    else:
        print("\nStarting enrichment process...\n")
        record_id = asyncio.run(enrich_single_profile(linkedin_url))
        
        if record_id:
            print(f"\n✅ Success! Record ID: {record_id}")
            print(f"\nProfile added to database with destination fields:")
            print("- ⚓️ Current Roles")
            print("- ⚓️ Education")
            print("- ⚓️ embedding_summary")
            print("- ⚓️ Location")
            print("- ⚓️ Past Roles")
            print("- ⚓️ Raw_Enriched_Data")
            print("- ⚓️ Work Experience (yrs)")
        else:
            print("\n❌ Failed to process profile") 