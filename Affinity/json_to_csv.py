import json
import csv
import os
from datetime import datetime

def convert_json_to_csv():
    # Input/Output paths
    json_file = "Affinity/data/affinity_master.json"
    csv_file = f"Affinity/data/affinity_master_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Expanded headers to include all JSON data
    headers = [
        # Basic Info
        'person_id',
        'type',
        'first_name',
        'last_name',
        'primary_email',
        'all_emails',
        'organization_ids',
        
        # Interaction Dates
        'first_email_date',
        'first_email_person_ids',
        'last_email_date',
        'last_email_person_ids',
        'first_event_date',
        'first_event_person_ids',
        'last_event_date',
        'last_event_person_ids',
        'next_event_date',
        'next_event_person_ids',
        'last_chat_message_date',
        'last_interaction_date',
        
        # Aggregated Stats
        'total_meetings',
        'total_emails',
        'unique_team_members',
        
        # LinkedIn Data
        'linkedin_url',
        'linkedin_handle',
        'linkedin_profile',
        
        # Raw Data (for reference)
        'all_interactions_json',
        'interaction_dates_json',
        'raw_person_json'
    ]
    
    print(f"\nConverting JSON to CSV:")
    print(f"Reading from: {json_file}")
    print(f"Writing to: {csv_file}")
    
    # Read JSON
    with open(json_file, 'r') as f:
        data = json.load(f)
        people = data.get('persons', [])
    
    # Write CSV
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        for person in people:
            interaction_dates = person.get('interaction_dates', {})
            interactions = person.get('interactions', {})
            
            # Get all team members
            team_members = set()
            for interaction in interactions.values():
                if isinstance(interaction, dict) and interaction.get('person_ids'):
                    team_members.update(interaction['person_ids'])
            
            # Count interactions
            total_meetings = len([i for i in interactions.values() 
                                if isinstance(i, dict) and i.get('date') 
                                and 'event' in i])
            total_emails = len([i for i in interactions.values() 
                              if isinstance(i, dict) and i.get('date') 
                              and 'email' in i])
            
            # Extract specific interaction details - with None handling
            first_email = interactions.get('first_email') or {}
            last_email = interactions.get('last_email') or {}
            first_event = interactions.get('first_event') or {}
            last_event = interactions.get('last_event') or {}
            next_event = interactions.get('next_event') or {}
            
            # Write row
            writer.writerow({
                # Basic Info
                'person_id': person.get('id'),
                'type': person.get('type'),
                'first_name': person.get('first_name', ''),
                'last_name': person.get('last_name', ''),
                'primary_email': person.get('primary_email', ''),
                'all_emails': ';'.join(person.get('emails', [])),
                'organization_ids': ';'.join(map(str, person.get('organization_ids', []))),
                
                # Detailed Interaction Dates with Person IDs
                'first_email_date': first_email.get('date', ''),
                'first_email_person_ids': ';'.join(map(str, first_email.get('person_ids', []))),
                'last_email_date': last_email.get('date', ''),
                'last_email_person_ids': ';'.join(map(str, last_email.get('person_ids', []))),
                'first_event_date': first_event.get('date', ''),
                'first_event_person_ids': ';'.join(map(str, first_event.get('person_ids', []))),
                'last_event_date': last_event.get('date', ''),
                'last_event_person_ids': ';'.join(map(str, last_event.get('person_ids', []))),
                'next_event_date': next_event.get('date', ''),
                'next_event_person_ids': ';'.join(map(str, next_event.get('person_ids', []))),
                'last_chat_message_date': interaction_dates.get('last_chat_message_date', ''),
                'last_interaction_date': interaction_dates.get('last_interaction_date', ''),
                
                # Stats
                'total_meetings': total_meetings,
                'total_emails': total_emails,
                'unique_team_members': ';'.join(map(str, team_members)),
                
                # LinkedIn
                'linkedin_url': person.get('linkedin_url', ''),
                'linkedin_handle': person.get('linkedin_handle', ''),
                'linkedin_profile': person.get('linkedin_profile', ''),
                
                # Raw JSON data
                'all_interactions_json': json.dumps(interactions),
                'interaction_dates_json': json.dumps(interaction_dates),
                'raw_person_json': json.dumps(person)
            })
    
    print(f"\nConversion complete!")
    print(f"- Processed {len(people)} people")
    print(f"- CSV saved to: {csv_file}")

if __name__ == "__main__":
    convert_json_to_csv() 