from openai import AsyncOpenAI
import asyncio
from tqdm import tqdm
import json
from supabase import create_client
import sys
import os
from pinecone import Pinecone
import logging

# Add parent directory to path to find config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.credentials import creds
from pyairtable import Api

logging.basicConfig(level=logging.INFO)

class ProfileEmbedder:
    def __init__(self):
        self.openai_client = AsyncOpenAI(api_key=creds.OPENAI_API_KEY)
        pc = Pinecone(api_key=creds.PINECONE_API_KEY)
        self.pinecone_index = pc.Index(creds.PINECONE_INDEX_NAME)
        self.supabase = create_client(creds.SUPABASE_URL, creds.SUPABASE_KEY)
    
    def prepare_text_for_embedding(self, profile):
        """Prepare comprehensive text for embedding"""
        try:
            raw_data_str = profile.get('fields', {}).get('Raw_Enriched_Data', '{}')
            
            # Skip if it's one of our error messages
            if raw_data_str in ["No LinkedIn URL provided", "LinkedIn profile not found (404)"]:
                logging.info(f"Skipping record {profile.get('id')} - error message in Raw_Enriched_Data")
                return None
            
            try:
                raw_data = json.loads(raw_data_str)
            except json.JSONDecodeError:
                logging.error(f"Invalid JSON in Raw_Enriched_Data for record {profile.get('id')}")
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
            current_title = experiences[0].get('title', '') if experiences else ''
            current_company = experiences[0].get('company', '') if experiences else ''
            headline = raw_data.get('headline', '')
            location = f"{raw_data.get('city', '')} {raw_data.get('state', '')} {raw_data.get('country', '')}".strip()
            summary = raw_data.get('summary', '')
            ai_summary = profile.get('fields', {}).get('AI_Summary', '')
            tags = profile.get('fields', {}).get('Persona Tags (Filter Field)', '')
            events = profile.get('fields', {}).get('⚡️🗓 All Events Attended', '')

            # Build final text
            sections = [
                f"Name: {name}",
                f"Current Position: {current_title} at {current_company}",
                f"Headline: {headline}",
                f"Location: {location}",
                "",
                "Experience History:",
                "----------------------------------------",
                "\n----------------------------------------\n".join(experience_details),
                "----------------------------------------",
                "",
                f"LinkedIn Summary: {summary}",
                f"AI Analysis: {ai_summary}",
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
    
    async def store_in_supabase(self, record_id, embedding, text, profile):
        """Store embedding with metadata in Supabase"""
        try:
            raw_data = json.loads(profile.get('fields', {}).get('Raw_Enriched_Data', '{}'))
            metadata = {
                'record_id': record_id,
                'full_name': raw_data.get('full_name', ''),
                'headline': raw_data.get('headline', ''),
                'current_company': raw_data.get('experiences', [{}])[0].get('company', ''),
                'location': f"{raw_data.get('city', '')} {raw_data.get('state', '')} {raw_data.get('country', '')}".strip(),
                'linkedin_url': raw_data.get('public_identifier', ''),
                'ai_summary': profile.get('fields', {}).get('AI_Summary', ''),
                'companies': profile.get('fields', {}).get('Previous_Companies', ''),
                'persona_tags': profile.get('fields', {}).get('Persona Tags (Filter Field)', ''),
                'events_attended': profile.get('fields', {}).get('⚡️🗓 All Events Attended', ''),
                'email': profile.get('fields', {}).get('Email', 'email not available')
            }
            
            data = {
                'record_id': record_id,
                'embedding': embedding,
                'content': text,
                'metadata': metadata
            }
            
            self.supabase.table('profile_embeddings').insert(data).execute()
            return True
        except Exception as e:
            print(f"Error storing in Supabase: {e}")
            return False

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
            raw_data = json.loads(profile.get('fields', {}).get('Raw_Enriched_Data', '{}'))
            experiences = raw_data.get('experiences', [{}])
            current_company = experiences[0].get('company', '') if experiences else ''
            
            # Ensure no null values in metadata
            metadata = {
                'record_id': profile['id'],
                'full_name': raw_data.get('full_name', ''),
                'headline': raw_data.get('headline', ''),
                'current_company': current_company or 'No current company',  # Default if empty
                'location': f"{raw_data.get('city', '')} {raw_data.get('state', '')} {raw_data.get('country', '')}".strip() or 'No location',
                'linkedin_url': raw_data.get('public_identifier', '') or 'No URL',
                'ai_summary': profile.get('fields', {}).get('AI_Summary', '') or 'No summary',
                'companies': profile.get('fields', {}).get('Previous_Companies', '') or 'No companies',
                'persona_tags': profile.get('fields', {}).get('Persona Tags (Filter Field)', '') or 'No tags',
                'events_attended': profile.get('fields', {}).get('⚡️🗓 All Events Attended', '') or 'No events',
                'email': profile.get('fields', {}).get('Email', 'email not available')
            }
            
            # Validate no nulls exist
            for key, value in metadata.items():
                if value is None:
                    metadata[key] = f'No {key}'
                
            return metadata
        
        except Exception as e:
            logging.error(f"Error preparing metadata for record {profile.get('id')}: {e}")
            return None

async def run_embedder():
    embedder = ProfileEmbedder()
    api = Api(creds.AIRTABLE_API_KEY)
    table = api.table(creds.AIRTABLE_BASE_ID, creds.AIRTABLE_TABLE_NAME)
    
    # Get existing IDs from Pinecone
    print("Fetching existing IDs from Pinecone...")
    existing_ids = embedder.get_existing_ids()
    
    # Get all records from Airtable
    print("Fetching records from Airtable...")
    all_records = table.all()
    print(f"Found {len(all_records)} total records")
    
    # Filter for new records with valid data
    new_records = [
        record for record in all_records 
        if record['id'] not in existing_ids
        and record.get('fields', {}).get('Raw_Enriched_Data')
        and record['fields']['Raw_Enriched_Data'] not in [
            "No LinkedIn URL provided",
            "LinkedIn profile not found (404)"
        ]
        and record.get('fields', {}).get('AI_Summary')
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
    api = Api(creds.AIRTABLE_API_KEY)
    table = api.table(creds.AIRTABLE_BASE_ID, creds.AIRTABLE_TABLE_NAME)
    
    print("Fetching from Airtable...")
    try:
        records = table.all(formula="NOT({Raw_Enriched_Data} = '')", max_records=1)
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