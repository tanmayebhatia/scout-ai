import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict

load_dotenv()

class AffinityClient:
    BASE_URL = "https://api.affinity.co"
    
    def __init__(self):
        self.api_key = os.getenv('AFFINITY_API_KEY')
        if not self.api_key:
            raise ValueError("Affinity API key not found")
            
        self.session = requests.Session()
        # Format auth exactly like the curl command: -u :API_KEY
        self.session.auth = ('', self.api_key)

    def get_person_with_interactions(self, person_id: int):
        """
        Get person details with their interaction history
        Args:
            person_id (int): Affinity person ID
        Returns:
            dict: Person data including interactions and related persons
        """
        try:
            # Format URL exactly like the working curl command
            url = f"{self.BASE_URL}/persons/{person_id}?with_interaction_dates=true&with_interaction_persons=true"
            
            print(f"\nMaking request to: {url}")
            response = self.session.get(url)
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error response: {response.text}")
                return None
            
        except Exception as e:
            print(f"Error: {str(e)}")
            if hasattr(e, 'response'):
                print(f"Response text: {e.response.text}")
            return None

    def get_persons_page(self, page_token: str = None) -> Dict:
        """
        Get a page of persons with their interactions
        Args:
            page_token: Token for pagination
        Returns:
            Dict containing persons and next_page_token
        """
        try:
            url = f"{self.BASE_URL}/persons"
            params = {
                'with_interaction_dates': 'true',
                'with_interaction_persons': 'true'
            }
            
            if page_token:
                params['page_token'] = page_token
            
            print(f"\nFetching persons page{' with token' if page_token else ''}")
            response = self.session.get(url, params=params)
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error response: {response.text}")
                return None
            
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

def main():
    client = AffinityClient()
    person_id = 226374944
    
    person_data = client.get_person_with_interactions(person_id)
    
    if person_data:
        # Person Details
        print("\nPerson Details:")
        print("=" * 50)
        print(f"Name: {person_data['first_name']} {person_data['last_name']}")
        print(f"Email: {person_data['primary_email']}")
        print(f"Organization IDs: {', '.join(map(str, person_data['organization_ids']))}")
        
        # Interactions
        print("\nInteractions with Primary Team:")
        print("=" * 50)
        
        interactions = person_data['interactions']
        
        # First Meeting/Email
        if interactions.get('first_email'):
            print("\nFirst Email:")
            print(f"Date: {interactions['first_email']['date']}")
            print(f"Primary Team IDs: {', '.join(map(str, interactions['first_email']['person_ids']))}")
            
        if interactions.get('first_event'):
            print("\nFirst Meeting:")
            print(f"Date: {interactions['first_event']['date']}")
            print(f"Primary Team IDs: {', '.join(map(str, interactions['first_event']['person_ids']))}")
        
        # Most Recent Interaction
        if interactions.get('last_event'):
            print("\nMost Recent Meeting:")
            print(f"Date: {interactions['last_event']['date']}")
            print(f"Primary Team IDs: {', '.join(map(str, interactions['last_event']['person_ids']))}")
            
        if interactions.get('last_email'):
            print("\nMost Recent Email:")
            print(f"Date: {interactions['last_email']['date']}")
            if interactions['last_email']['person_ids']:
                print(f"Primary Team IDs: {', '.join(map(str, interactions['last_email']['person_ids']))}")
        
        # Upcoming
        if interactions.get('next_event'):
            print("\nUpcoming Meeting:")
            print(f"Date: {interactions['next_event']['date']}")
            print(f"Primary Team IDs: {', '.join(map(str, interactions['next_event']['person_ids']))}")
        
        # Save raw data
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"Affinity/responses/person_data_{timestamp}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(person_data, f, indent=2)
    else:
        print("No data found or error occurred")

if __name__ == "__main__":
    main() 