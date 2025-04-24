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
from src.utils import extract_fields_from_enriched_data
import re
from typing import Dict, Any, List, Tuple, Optional
import datetime

logging.basicConfig(level=logging.INFO)

load_dotenv()

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
            records = table.all(formula=f"{{⚓️ LinkedIn URL}} = '{linkedin_url}'")
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

        # Extract fields using the common utility function
        logging.info("Extracting fields from enriched data...")
        extracted_fields = extract_fields_from_enriched_data(enriched_data, linkedin_url)
        logging.info(f"✅ Extracted {len(extracted_fields)} fields from profile data")
        
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