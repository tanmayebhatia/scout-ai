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

logging.basicConfig(level=logging.INFO)

load_dotenv()

async def enrich_single_profile(linkedin_url: str):
    """Process a single LinkedIn URL through the entire pipeline"""
    try:
        logging.info("Initializing clients...")
        enricher = LinkedInEnricher()
        airtable = Api(os.getenv("AIRTABLE_API_KEY"))
        table = airtable.table(
            os.getenv("AIRTABLE_BASE_ID"),
            os.getenv("AIRTABLE_TABLE_NAME")
        )
        openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        embedder = ProfileEmbedder()
        logging.info("✅ Clients initialized")

        # Check if profile exists
        logging.info("Checking if profile exists...")
        records = table.all(formula=f"{{linkedin_url}} = '{linkedin_url}'")
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

        # Create Airtable record
        logging.info("Creating Airtable record...")
        airtable_record = table.create({
            'linkedin_url': linkedin_url,
            'Raw_Enriched_Data': json.dumps(enriched_data)
        })
        record_id = airtable_record['id']
        logging.info(f"✅ Created Airtable record: {record_id}")

        # Analyze with OpenAI
        logging.info("Analyzing with OpenAI...")
        analysis = await analyze_with_openai(openai_client, enriched_data)
        if analysis:
            companies, summary, location = parse_openai_response(analysis)
            table.update(record_id, {
                'Previous_Companies': companies,
                'AI_Summary': summary,
                'Location': location
            })
            logging.info("✅ OpenAI analysis complete and stored")
        else:
            logging.warning("⚠️ OpenAI analysis failed")

        # Create and store embedding
        logging.info("Creating embedding...")
        text = embedder.prepare_text_for_embedding(airtable_record)
        if text:
            embedding = await embedder.get_embedding(text)
            metadata = embedder.prepare_metadata(airtable_record)
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
        else:
            print("\n❌ Failed to process profile") 