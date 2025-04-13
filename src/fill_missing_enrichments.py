import time
import requests
from tqdm import tqdm
from pyairtable import Api
import logging
import json  # Add this import at the top

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Credentials (copy from your existing creds.py)
AIRTABLE_API_KEY = "patGlN75YO3Yrlc7m.a55ee6118e62c505701c42529b4599cbd0282c0ae63af6c5d4d4093910ce6855"  # Your Airtable API key
PROXY_CURL_API_KEY = "qZFZXjK-K9s92rNfTPIqGg"   # Your Proxy-Curl API key
AIRTABLE_BASE_ID = "appKoKRVzD3mSxA6j"
AIRTABLE_TABLE_NAME = "tblM9K7BWV1XrPh82"


def enrich_profile(linkedin_url):
    """Get raw enrichment data from proxy-curl"""
    try:
        headers = {'Authorization': f'Bearer {PROXY_CURL_API_KEY}'}
        response = requests.get(
            'https://nubela.co/proxycurl/api/v2/linkedin',
            params={'url': linkedin_url},
            headers=headers
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to enrich {linkedin_url}: Status {response.status_code}")
            return None
            
    except Exception as e:
        logging.error(f"Error enriching {linkedin_url}: {str(e)}")
        return None

def is_valid_linkedin_profile_url(url):
    """Check if URL is a valid LinkedIn profile URL"""
    if not url:
        return False
    
    # Must be a profile URL, not company
    if not isinstance(url, str):
        return False
    if 'linkedin.com/in/' not in url.lower():
        return False
        
    return True

def clean_linkedin_url(url):
    """Clean and standardize LinkedIn URL"""
    if not url:
        return None
        
    # Convert to string and lowercase
    url = str(url).lower().strip()
    
    # Remove duplicate https:// or http://
    url = url.replace('https://https://', 'https://')
    url = url.replace('http://http://', 'http://')
    url = url.replace('https://http://', 'https://')
    
    # Ensure starts with https://
    if not url.startswith('https://'):
        url = url.replace('http://', '')
        url = url.replace('https://', '')
        url = f"https://{url}"
    
    # Remove www. if present
    url = url.replace('www.', '')
    
    # Remove trailing slash
    url = url.rstrip('/')
    
    return url

def process_batch(table, batch):
    for record in tqdm(batch, desc="Processing batch"):
        linkedin_url = record['fields'].get('linkedin_url')
        
        # If no LinkedIn URL
        if not linkedin_url:
            table.update(record['id'], {
                'Raw_Enriched_Data': "No LinkedIn URL provided"
            })
            continue
        
        # Clean URL
        linkedin_url = clean_linkedin_url(linkedin_url)
        
        # Try to enrich
        enriched_data = enrich_profile(linkedin_url)
        
        if enriched_data:
            # Success - store enriched data
            table.update(record['id'], {
                'Raw_Enriched_Data': json.dumps(enriched_data)
            })
        else:
            # Failed - store error message
            table.update(record['id'], {
                'Raw_Enriched_Data': "LinkedIn profile not found (404)"
            })
        
        time.sleep(0.3)
    
    print("Waiting 0.3 seconds before next batch...")
    time.sleep(0.3)

def main():
    api = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    
    print("Fetching records from Airtable...")
    records = table.all()
    
    # Get all records missing enrichment data
    to_process = [
        record for record in records 
        if not record['fields'].get('Raw_Enriched_Data')
    ]
    
    print(f"\nFound {len(to_process)} records to process")
    
    # Process in batches of 50
    batch_size = 50
    for i in range(0, len(to_process), batch_size):
        batch = to_process[i:i + batch_size]
        process_batch(table, batch)

if __name__ == "__main__":
    main() 