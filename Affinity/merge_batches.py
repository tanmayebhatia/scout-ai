import json
from datetime import datetime

# Read first batch
with open('Affinity/data/persons_batch_20250415_231426.json', 'r') as f:
    batch1 = json.load(f)

# Read second batch
with open('Affinity/data/persons_batch_20250415_231835.json', 'r') as f:
    batch2 = json.load(f)

# Combine people arrays, ensuring no duplicates
all_people = batch1['persons']
seen_ids = {str(p['id']) for p in all_people}

# Add new people from batch 2
for person in batch2['persons']:
    if str(person['id']) not in seen_ids:
        all_people.append(person)
        seen_ids.add(str(person['id']))

# Create master file
master_data = {
    'metadata': {
        'last_updated': datetime.now().isoformat(),
        'total_people': len(all_people),
        'source_files': [
            'persons_batch_20250415_231426.json',
            'persons_batch_20250415_231835.json'
        ]
    },
    'persons': all_people
}

# Save master file
with open('Affinity/data/affinity_persons_master.json', 'w') as f:
    json.dump(master_data, f, indent=2)

print(f"Created master file with {len(all_people)} unique people") 