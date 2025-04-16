import os
import json
from datetime import datetime
from typing import Dict, Optional
from affinity_client import AffinityClient

class AffinityScraper:
    def __init__(self):
        self.client = AffinityClient()
        self.output_dir = "Affinity/data"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Track progress
        self.processed_ids = self._load_existing_ids()
        self.processed_count = 0
        
    def _load_existing_ids(self) -> set:
        """Load IDs from master JSON file"""
        existing_ids = set()
        master_file = os.path.join(self.output_dir, 'affinity_master.json')
        
        if os.path.exists(master_file):
            try:
                with open(master_file, 'r') as f:
                    data = json.load(f)
                    for person in data.get('persons', []):
                        existing_ids.add(str(person['id']))
                print(f"Loaded {len(existing_ids)} existing person IDs from master file")
            except Exception as e:
                print(f"Error loading master file: {e}")
        
        return existing_ids

    def start_scraping(self, limit: Optional[int] = None):
        """Start scraping process with pagination"""
        master_file = os.path.join(self.output_dir, 'affinity_master.json')
        
        # Load existing master data
        try:
            with open(master_file, 'r') as f:
                master_data = json.load(f)
        except:
            master_data = {'persons': [], 'metadata': {}}
        
        print(f"\nStarting new scrape:")
        print(f"- Already have {len(self.processed_ids)} unique people")
        print(f"- Will fetch up to {limit if limit else 'all'} new people")
        
        next_page_token = self._load_last_token()
        current_page = 1
        new_people = []
        linkedin_count = 0  # Track LinkedIn data
        
        while True:
            print(f"\n--- Processing Page {current_page} ---")
            
            response = self.client.get_persons_page(next_page_token)
            if not response:
                break
                
            persons = response.get('persons', [])
            next_page_token = response.get('next_page_token')
            
            # Save token for next run
            if next_page_token:
                self._save_last_token(next_page_token)
            
            print(f"Found {len(persons)} people on this page")
            
            # Process new people with LinkedIn data
            new_on_this_page = []
            for person in persons:
                if str(person['id']) not in self.processed_ids:
                    person_data = {
                        **person,
                        'linkedin_url': person.get('linkedin_url'),
                        'linkedin_handle': person.get('linkedin_handle'),
                        'linkedin_profile': person.get('linkedin_profile')
                    }
                    new_on_this_page.append(person_data)
                    
                    # Track LinkedIn presence
                    if any(person_data.get(f) for f in ['linkedin_url', 'linkedin_handle', 'linkedin_profile']):
                        linkedin_count += 1
            
            print(f"Found {len(new_on_this_page)} new people")
            print(f"- {linkedin_count} have LinkedIn data")
            new_people.extend(new_on_this_page)
            
            # Update processed IDs
            for person in new_on_this_page:
                self.processed_ids.add(str(person['id']))
                self.processed_count += 1
            
            if limit and self.processed_count >= limit:
                print(f"\nReached limit of {limit} new people")
                break
                
            if not next_page_token:
                print("\nNo more pages to process")
                break
                
            current_page += 1
        
        # Update master file with new people
        if new_people:
            master_data['persons'].extend(new_people)
            master_data['metadata'] = {
                'last_updated': datetime.now().isoformat(),
                'total_people': len(master_data['persons']),
                'linkedin_coverage': f"{linkedin_count}/{len(new_people)} new people"
            }
            
            with open(master_file, 'w') as f:
                json.dump(master_data, f, indent=2)
            
            print(f"\nUpdated master file:")
            print(f"- Added {len(new_people)} new people")
            print(f"- {linkedin_count} of them have LinkedIn data")
            print(f"- Total unique people: {len(master_data['persons'])}")

    def _save_last_token(self, token):
        """Save the last page token for next run"""
        with open(f"{self.output_dir}/last_token.txt", 'w') as f:
            f.write(token or '')

    def _load_last_token(self):
        """Load the last page token if exists"""
        try:
            with open(f"{self.output_dir}/last_token.txt", 'r') as f:
                return f.read().strip() or None
        except:
            return None

    def combine_existing_batches(self):
        """Combine all existing batch files into a single master JSON"""
        all_people = []
        unique_ids = set()
        
        # Get all batch files
        json_files = [f for f in os.listdir(self.output_dir) if f.endswith('_persons.json')]
        
        print("\nCombining existing batch files:")
        for file in json_files:
            filepath = os.path.join(self.output_dir, file)
            print(f"- Processing {file}")
            
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    batch_people = data.get('persons', [])
                    
                    # Add only new people
                    for person in batch_people:
                        person_id = str(person['id'])
                        if person_id not in unique_ids:
                            all_people.append(person)
                            unique_ids.add(person_id)
                            
                    print(f"  Found {len(batch_people)} people, {len(unique_ids)} unique so far")
                    
            except Exception as e:
                print(f"Error processing {file}: {e}")
        
        # Save combined data to master file
        master_file = os.path.join(self.output_dir, 'affinity_persons_master.json')
        with open(master_file, 'w') as f:
            json.dump({
                'metadata': {
                    'last_updated': datetime.now().isoformat(),
                    'total_people': len(all_people),
                    'source_files': json_files
                },
                'persons': all_people
            }, f, indent=2)
        
        print(f"\nCreated master file:")
        print(f"- Path: {master_file}")
        print(f"- Total unique people: {len(all_people)}")
        return master_file

def main():
    scraper = AffinityScraper()
    
    print(f"Starting scraper for next 4000 people...")
    scraper.start_scraping(limit=4000)

if __name__ == "__main__":
    main() 