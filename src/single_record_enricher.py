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
from src.analyze_enriched_records import analyze_with_openai
from src.utils import parse_openai_response
from src.embedder import ProfileEmbedder
import re
from typing import Dict, Any, List, Tuple, Optional

logging.basicConfig(level=logging.INFO)

load_dotenv()

async def extract_field_data(enriched_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract specific fields from enriched data to populate Airtable fields"""
    extracted = {}
    
    try:
        # Current Role
        if enriched_data.get('job_title'):
            current_role = enriched_data.get('job_title')
            if enriched_data.get('company'):
                current_role += f" at {enriched_data.get('company')}"
            extracted['⚓️ Current Roles'] = current_role
        
        # Education
        if enriched_data.get('education'):
            education_list = []
            for edu in enriched_data.get('education', []):
                edu_str = ""
                if edu.get('degree_name'):
                    edu_str += edu.get('degree_name')
                if edu.get('school'):
                    if edu_str:
                        edu_str += f" at {edu.get('school')}"
                    else:
                        edu_str = edu.get('school')
                if edu_str:
                    education_list.append(edu_str)
            if education_list:
                extracted['⚓️ Education'] = education_list
        
        # Location
        if enriched_data.get('location'):
            extracted['⚓️ Location'] = enriched_data.get('location')
            
        # Past Roles
        if enriched_data.get('experiences'):
            past_roles = []
            # Skip the first one if it's the current role
            for exp in enriched_data.get('experiences', [])[1:]:
                role_str = ""
                if exp.get('title'):
                    role_str += exp.get('title')
                if exp.get('company'):
                    if role_str:
                        role_str += f" at {exp.get('company')}"
                    else:
                        role_str = exp.get('company')
                if role_str:
                    past_roles.append(role_str)
            if past_roles:
                extracted['⚓️ Past Roles'] = past_roles
        
        # Work Experience (yrs)
        total_experience = 0
        for exp in enriched_data.get('experiences', []):
            if exp.get('starts_at') and exp.get('ends_at'):
                start_year = exp.get('starts_at', {}).get('year', 0)
                # If still current, use current year
                if not exp.get('ends_at', {}).get('year'):
                    import datetime
                    end_year = datetime.datetime.now().year
                else:
                    end_year = exp.get('ends_at', {}).get('year', 0)
                
                if start_year and end_year:
                    experience_years = end_year - start_year
                    total_experience += experience_years
        
        if total_experience > 0:
            extracted['⚓️ Work Experience (yrs)'] = total_experience
            
    except Exception as e:
        logging.error(f"Error extracting fields from enriched data: {str(e)}")
    
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
        records = table.all(formula=f"{{LinkedIn URL}} = '{linkedin_url}'")
        if records:
            logging.info("⚠️ Profile already exists in database")
            return records[0]['id']

        # Enrich with Proxy Curl
        logging.info("Enriching profile with Proxy Curl...")
        enriched_data = await enricher.enrich_profile(linkedin_url)
        if not enriched_data or enriched_data in ["No LinkedIn URL provided", "LinkedIn profile not found (404)"]:
            logging.error("❌ Failed to enrich profile")
            return None
        logging.info("✅ Profile enriched successfully")

        # Extract fields from enriched data
        extracted_fields = await extract_field_data(enriched_data)
        logging.info(f"✅ Extracted fields: {', '.join(extracted_fields.keys())}")
        
        # Create embedding summary
        logging.info("Creating embedding summary...")
        embedding_summary = await create_embedding_summary(openai_client, enriched_data)
        if embedding_summary:
            extracted_fields['⚓️ embedding_summary'] = embedding_summary
            logging.info("✅ Embedding summary created")
        
        # Add raw enriched data
        extracted_fields['⚓️ Raw_Enriched_Data'] = json.dumps(enriched_data)
        extracted_fields['LinkedIn URL'] = linkedin_url
        
        # Get name for record
        name = enriched_data.get('full_name', 'Unknown Profile')
        extracted_fields['Name'] = name
        
        # Create Airtable record
        logging.info("Creating Airtable record...")
        airtable_record = table.create(extracted_fields)
        record_id = airtable_record['id']
        logging.info(f"✅ Created Airtable record: {record_id}")
            
        # Create and store embedding
        logging.info("Creating embedding...")
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
                logging.warning("⚠️ Failed to create embedding")

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