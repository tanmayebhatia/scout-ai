from openai import AsyncOpenAI
import asyncio
from tqdm import tqdm
import json
import sys
import os
from pinecone import Pinecone
import logging
from dotenv import load_dotenv
from pyairtable import Api


load_dotenv()

logging.basicConfig(level=logging.INFO)

class ProfileEmbedder:
    def __init__(self):
        self.openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        self.pinecone_index = pc.Index(os.getenv("PINECONE_INDEX"))
    
    def prepare_text_for_embedding(self, profile):
        """Prepare comprehensive text for embedding"""
        try:
            raw_data_str = profile.get('fields', {}).get('⚓️ Raw_Enriched_Data', '{}')
            
            # Skip if it's one of our error messages
            if raw_data_str in ["No LinkedIn URL provided", "LinkedIn profile not found (404)"]:
                logging.info(f"Skipping record {profile.get('id')} - error message in ⚓️ Raw_Enriched_Data")
                return None
            
            try:
                raw_data = json.loads(raw_data_str)
            except json.JSONDecodeError:
                logging.error(f"Invalid JSON in ⚓️ Raw_Enriched_Data for record {profile.get('id')}")
                logging.error(f"Raw data: {raw_data_str[:200]}")
                return None
            
            experiences = raw_data.get('experiences', [])
            
            # Detailed experience text
            experience_details = []
            for exp in experiences:
                if not isinstance(exp, dict):
                    continue
                    
                company = exp.get('company', '')
                title = exp.get('title', '')
                
                # Safely handle date fields
                starts_at = exp.get('starts_at')
                ends_at = exp.get('ends_at')
                
                start_year = starts_at.get('year', '') if isinstance(starts_at, dict) else ''
                end_year = ends_at.get('year', 'Present') if isinstance(ends_at, dict) else 'Present'
                
                description = exp.get('description', '')
                location = exp.get('location', '')
                
                exp_text = f"Company: {company}\nTitle: {title}\nDuration: {start_year}-{end_year}\nDescription: {description}\nLocation: {location}"
                experience_details.append(exp_text)

            # Get basic profile info
            name = raw_data.get('full_name', '')
            headline = raw_data.get('headline', '')
            location = f"{raw_data.get('city', '')} {raw_data.get('state', '')} {raw_data.get('country', '')}".strip()
            summary = raw_data.get('summary', '')
            embedding_summary = raw_data.get('⚓️ embedding_summary', '')
            current_role = profile.get('fields', {}).get('⚓️ Current Roles', '')
            past_roles = profile.get('fields', {}).get('⚓️ Past Roles', '')
            tags = raw_data.get('Persona Tags (Filter Field)', '')
            events = raw_data.get('⚡️🗓 All Events Attended', '')

            # Build final text
            sections = [
                f"Current Position: {current_role}",
                f"Past Roles: {past_roles}",
                f"Headline: {headline}",
                f"Location: {location}",
                f"Experience History: {experience_details}",
                f"LinkedIn Summary: {summary}",
                f"AI Analysis: {embedding_summary}",
                f"Primary Tags: {tags}",
                f"Events: {events}"
            ]
            
            return "\n".join(sections)
            
        except Exception as e:
            logging.error(f"Error preparing text for record {profile.get('id')}: {str(e)}")
            return None
    
    async def get_embedding(self, text):
        """Get embedding from OpenAI"""
        try:
            response = await self.openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=text,
                dimensions=1536
            )
            return response.data[0].embedding
        except Exception as e:
            logging.error(f"Error getting embedding: {e}")
            return None
    
    def get_existing_ids(self):
        """Get just the IDs of records already in Pinecone"""
        try:
            stats = self.pinecone_index.describe_index_stats()
            total_vectors = stats.total_vector_count
            print(f"Found {total_vectors} vectors in Pinecone")
            
            if total_vectors == 0:
                return set()
            
            # Query with dummy vector to get IDs
            query_response = self.pinecone_index.query(
                vector=[0] * 1536,
                top_k=total_vectors,
                include_metadata=False
            )
            
            existing_ids = {match.id for match in query_response.matches}
            print(f"Retrieved {len(existing_ids)} existing IDs from Pinecone")
            return existing_ids
        
        except Exception as e:
            logging.error(f"Error getting existing IDs from Pinecone: {str(e)}")
            return set()

    def prepare_metadata(self, profile):
        """Prepare metadata for Pinecone with validation"""
        try:
            raw_data = json.loads(profile.get('fields', {}).get('⚓️ Raw_Enriched_Data', '{}'))
            experiences = raw_data.get('experiences', [{}])
            current_company = experiences[0].get('company', '') if experiences else ''
            fields = profile.get('fields', {})
            
            # Ensure no null values in metadata
            metadata = {
                'record_id': profile['id'],
                'full_name': raw_data.get('full_name', ''),
                'headline': raw_data.get('headline', ''),
                'current_company': current_company or 'No current company',
                'location': f"{raw_data.get('city', '')} {raw_data.get('state', '')} {raw_data.get('country', '')}".strip() or 'No location',
                'linkedin_url': raw_data.get('public_identifier', '') or 'No URL',
                'ai_summary': fields.get('⚓️ embedding_summary', 'No summary'),
                'companies': fields.get('⚓️ Past Roles', 'No companies'),
                'persona_tags': fields.get('Persona Tags (Filter Field)', 'No tags'),
                'events_attended': fields.get('⚡️🗓 All Events Attended', 'No events'),
                'email': profile.get('fields', {}).get('Email', 'email not available')
            }
            
            # Log metadata for debugging
            logging.info(f"Prepared metadata for record {profile['id']}: {json.dumps(metadata, indent=2)}")
            
            return metadata
        
        except Exception as e:
            logging.error(f"Error preparing metadata for record {profile.get('id')}: {e}")
            return None

async def run_embedder():
    embedder = ProfileEmbedder()
    api = Api(os.getenv("AIRTABLE_API_KEY"))
    table = api.table(os.getenv("AIRTABLE_BASE_ID"), os.getenv("AIRTABLE_TABLE_NAME"))
    
    # Get existing IDs from Pinecone
    print("Fetching existing IDs from Pinecone...")
    existing_ids = embedder.get_existing_ids()
    
    # Get all records from Airtable
    print("Fetching records from Airtable...")
    all_records = table.all()
    print(f"Found {len(all_records)} total records")
    
    # Debug: Check record structure
    if all_records:
        print("\nSample record fields:")
        sample_record = all_records[0]
        print(f"Record ID: {sample_record['id']}")
        print(f"Field names: {', '.join(sample_record.get('fields', {}).keys())}")
        
        # Count records with each field 
        has_raw_data = sum(1 for r in all_records if r.get('fields', {}).get('⚓️ Raw_Enriched_Data'))
        has_ai_summary = sum(1 for r in all_records if r.get('fields', {}).get('⚓️ embedding_summary'))
        has_error_msg = sum(1 for r in all_records if r.get('fields', {}).get('⚓️ Raw_Enriched_Data') in [
            "No LinkedIn URL provided", "LinkedIn profile not found (404)"
        ])
        
        print(f"\nValidation stats:")
        print(f"Records with ⚓️ Raw_Enriched_Data: {has_raw_data}/{len(all_records)}")
        print(f"Records with ⚓️ embedding_summary: {has_ai_summary}/{len(all_records)}")
        print(f"Records with error messages: {has_error_msg}/{len(all_records)}")
    
    # Print 10 sample embedding texts from any records, even if they might have issues
    print("\n=== SAMPLE EMBEDDING TEXTS (FROM ANY RECORDS) ===")
    
    sample_count = min(10, len(all_records))
    samples_shown = 0
    
    # First try to get a few valid records
    for record in all_records:
        if samples_shown >= sample_count:
            break
            
        text = None
        try:
            # Skip minimal validation - just make sure there's something in Raw_Enriched_Data
            if record.get('fields', {}).get('⚓️ Raw_Enriched_Data'):
                text = embedder.prepare_text_for_embedding(record)
        except Exception as e:
            print(f"Error preparing text for record {record.get('id')}: {e}")
            
        if text:
            is_in_pinecone = record['id'] in existing_ids
            print(f"\nSAMPLE #{samples_shown+1} (Record ID: {record['id']}, Already in Pinecone: {is_in_pinecone}):")
            print("="*50)
            print(text[:500] + "..." if len(text) > 500 else text)  # Print first 500 chars
            print("="*50)
            samples_shown += 1
    
    if samples_shown == 0:
        print("Could not generate text for ANY records! There might be an issue with the field names or data format.")
        
        # Show raw data from first record as a last resort
        if all_records:
            print("\nRaw data from first record:")
            print(json.dumps(all_records[0].get('fields', {}), indent=2)[:1000])
    
    # Ask for confirmation
    confirmation = input("\nDo you want to continue with the embedding process? (y/n): ")
    if confirmation.lower() != 'y':
        print("Embedding process cancelled.")
        return
    
    # Filter for new records with valid data for actual processing
    valid_records = [
        record for record in all_records 
        if record.get('fields', {}).get('⚓️ Raw_Enriched_Data')
        and record['fields']['⚓️ Raw_Enriched_Data'] not in [
            "No LinkedIn URL provided", 
            "LinkedIn profile not found (404)"
        ]
        and record.get('fields', {}).get('⚓️ embedding_summary')
    ]
    
    new_records = [
        record for record in valid_records 
        if record['id'] not in existing_ids
    ]
    
    print(f"Found {len(new_records)} new records to embed")
    
    # Process in batches
    batch_size = 50
    all_records = new_records
    
    for i in range(0, len(all_records), batch_size):
        batch = all_records[i:i + batch_size]
        print(f"\nProcessing batch {i//batch_size + 1} of {len(all_records)//batch_size + 1}")
        
        vectors_to_upsert = []
        for record in tqdm(batch):
            text = embedder.prepare_text_for_embedding(record)
            if not text:  # Skip if text preparation failed
                continue
            
            embedding = await embedder.get_embedding(text)
            
            if embedding:
                metadata = embedder.prepare_metadata(record)
                if metadata:
                    vectors_to_upsert.append((record['id'], embedding, metadata))
        
        # Batch upsert to Pinecone
        if vectors_to_upsert:
            try:
                embedder.pinecone_index.upsert(vectors=vectors_to_upsert)
                print(f"Successfully upserted {len(vectors_to_upsert)} vectors")
            except Exception as e:
                logging.error(f"Error upserting to Pinecone: {e}")
        
        await asyncio.sleep(.2)  # Rate limiting

async def test_embedding():
    embedder = ProfileEmbedder()
    api = Api(os.getenv("AIRTABLE_API_KEY"))
    table = api.table(os.getenv("AIRTABLE_BASE_ID"), os.getenv("AIRTABLE_TABLE_NAME"))
    
    print("Fetching from Airtable...")
    try:
        records = table.all(formula="NOT({⚓️ Raw_Enriched_Data} = '')", max_records=1)
        if records and len(records) > 0:
            record = records[0]
            
            # Show metadata
            print("\nMetadata that will be stored:")
            metadata = embedder.prepare_metadata(record)
            print(json.dumps(metadata, indent=2))
            
            # Show embedding text
            text = embedder.prepare_text_for_embedding(record)
            print("\nText that will be embedded:")
            print("="*80)
            print(text)
            print("="*80)
            
            return text
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    #asyncio.run(test_embedding())
    
    # Uncomment to run full embedding
    asyncio.run(run_embedder()) 
