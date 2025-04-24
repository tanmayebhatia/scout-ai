import requests
from openai import OpenAI
import json
import logging
import re
from tqdm import tqdm
from .rate_limiter import RateLimit
import os
from dotenv import load_dotenv
from pyairtable import Api
import aiohttp

# Remove any config.credentials import!
load_dotenv()

class LinkedInEnricher:
    def __init__(self):
        self.rate_limiter = RateLimit(limit=200, interval=60)
        self.openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.proxy_curl_key = os.getenv("PROXYCURL_API_KEY")
    
    def is_valid_profile_url(self, url):
        """
        Check if the URL is a valid LinkedIn profile URL
        Accepts URLs with or without protocol (http/https)
        Handles various LinkedIn URL formats
        """
        if not url:
            return False
        
        # Trim whitespace
        url = url.strip()
        
        # Normalize URL by adding https:// if needed
        normalized_url = url
        if not url.startswith('http://') and not url.startswith('https://'):
            normalized_url = 'https://' + url
        
        # Try to normalize URLs missing /in/ path
        if re.search(r'linkedin\.com/[^/]+$', normalized_url, re.IGNORECASE):
            # This matches patterns like linkedin.com/username (missing /in/)
            username = normalized_url.split('/')[-1]
            normalized_url = re.sub(r'(linkedin\.com)/[^/]+$', r'\1/in/' + username, normalized_url, flags=re.IGNORECASE)
        
        # Also handle linkedin/username format (missing .com)
        if re.search(r'linkedin/[^/]+$', normalized_url, re.IGNORECASE):
            username = normalized_url.split('/')[-1]
            normalized_url = re.sub(r'(linkedin)/[^/]+$', r'linkedin.com/in/' + username, normalized_url, flags=re.IGNORECASE)
        
        # Basic pattern validation with more flexibility
        # Allow both linkedin.com/in/username and linkedin.com/username
        pattern = r'https?:\/\/(www\.)?(linkedin\.com\/(in\/)?[\w\-_%]+)\/?.*$'
        return bool(re.match(pattern, normalized_url, re.IGNORECASE))
    
    async def enrich_profile(self, linkedin_url: str):
        """Enrich a LinkedIn profile URL using Proxy Curl"""
        try:
            # Trim whitespace
            if linkedin_url:
                linkedin_url = linkedin_url.strip()
            
            if not self.is_valid_profile_url(linkedin_url):
                return "Invalid LinkedIn URL"
            
            # Normalize URL for API call
            normalized_url = linkedin_url
            if not linkedin_url.startswith('http://') and not linkedin_url.startswith('https://'):
                normalized_url = 'https://' + linkedin_url
            
            # Try to normalize URLs missing /in/ path
            if re.search(r'linkedin\.com/[^/]+$', normalized_url, re.IGNORECASE):
                # This matches patterns like linkedin.com/username (missing /in/)
                username = normalized_url.split('/')[-1]
                normalized_url = re.sub(r'(linkedin\.com)/[^/]+$', r'\1/in/' + username, normalized_url, flags=re.IGNORECASE)
            
            # Also handle linkedin/username format (missing .com)
            if re.search(r'linkedin/[^/]+$', normalized_url, re.IGNORECASE):
                username = normalized_url.split('/')[-1]
                normalized_url = re.sub(r'(linkedin)/[^/]+$', r'linkedin.com/in/' + username, normalized_url, flags=re.IGNORECASE)
            
            headers = {
                'Authorization': f'Bearer {self.proxy_curl_key}'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'https://nubela.co/proxycurl/api/v2/linkedin',
                    headers=headers,
                    params={'url': normalized_url}
                ) as response:
                    if response.status == 404:
                        return "404 error, LinkedIn not found"
                    elif response.status == 401:
                        return "401 error, LinkedIn not found"
                    elif response.status != 200:
                        return f"API error: Status {response.status}"
                    return await response.json()
                    
        except Exception as e:
            error_str = str(e)
            logging.error(f"Error enriching profile {linkedin_url}: {error_str}")
            if "401" in error_str:
                return "401 error, LinkedIn not found"
            elif "404" in error_str:
                return "404 error, LinkedIn not found"
            return f"Error: {error_str}"

    async def analyze_with_openai_async(self, client, profile_data):
        """Analyze profile with OpenAI asynchronously"""
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
                    {"role": "user", "content": self.get_analysis_prompt(profile_summary)}
                ],
                temperature=0.7
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logging.error(f"Error in OpenAI analysis: {str(e)}")
            return None

    def get_analysis_prompt(self, profile_summary):
        """Generate the analysis prompt"""
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

    def enrich_missing_profiles(self):
        """Enrich profiles in Airtable that don't have raw enrichment data"""
        # Initialize Airtable
        airtable = Api(os.getenv("AIRTABLE_API_KEY"))
        table = airtable.table(os.getenv("AIRTABLE_BASE_ID"), 'Contacts')
        
        # Get all records
        records = table.all()
        
        # Filter for records with LinkedIn URL but no raw enrichment
        to_enrich = [
            record for record in records 
            if record['fields'].get('linkedin_url') and not record['fields'].get('Raw Enrichment')
        ]
        
        print(f"Found {len(to_enrich)} profiles to enrich")
        
        # Enrich each profile
        for record in tqdm(to_enrich):
            linkedin_url = record['fields']['linkedin_url']
            enriched_data = self.enrich_profile(linkedin_url)
            
            if enriched_data:
                # Update Airtable with raw enrichment data
                try:
                    table.update(record['id'], {
                        'Raw Enrichment': enriched_data
                    })
                except Exception as e:
                    logging.error(f"Failed to update Airtable for {linkedin_url}: {str(e)}")
                    continue
                
                # Small delay to respect rate limits
                time.sleep(0.5)

if __name__ == "__main__":
    # Create enricher and run
    enricher = LinkedInEnricher()
    enricher.enrich_missing_profiles()