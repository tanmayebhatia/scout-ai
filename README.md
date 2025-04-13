# Networks AI Profile Enricher

A service that enriches LinkedIn profiles with AI analysis and vector embeddings.

## Features
- LinkedIn profile data enrichment via Proxy Curl
- AI analysis using OpenAI
- Vector embeddings stored in Pinecone
- Airtable integration for data storage

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
```env
AIRTABLE_API_KEY=your_key
AIRTABLE_BASE_ID=your_base
AIRTABLE_TABLE_NAME=your_table
OPENAI_API_KEY=your_key
PROXY_CURL_API_KEY=your_key
PINECONE_API_KEY=your_key
PINECONE_ENV=your_env
PINECONE_INDEX_NAME=your_index
```

3. Run the FastAPI server:
```bash
uvicorn main:app --reload
```

## API Endpoints

### POST /api/enrich-profile
Enriches a single LinkedIn profile
```json
{
  "linkedin_url": "https://www.linkedin.com/in/username"
}
```

## Project Structure
```
networks_ai/
├── main.py              # FastAPI application
├── requirements.txt     # Python dependencies
├── src/
│   ├── enricher.py     # LinkedIn data enrichment
│   ├── embedder.py     # Vector embeddings
│   └── single_record_enricher.py  # Single profile processing
└── config/
    └── credentials.py   # Credentials management
```

## Overview

This tool:
1. Fetches LinkedIn profiles from Airtable
2. Enriches them using ProxyCurl API
3. Analyzes them with OpenAI
4. Updates Airtable with enriched data

## Key Features

1. **Two-Step Processing**:
   - Step 1: Bulk enrichment with ProxyCurl (slowish, 100/minute)
   - Step 2: AI analysis with OpenAI (separate process - currently refining)

2. **Batch Processing**:
   - Process records in configurable batches
   - Batch updates to Airtable
   - Skip already processed records

3. **AI Analysis**:
   - Company history with current position
   - Background summary and classification
   - Location formatting
   - Role categorization (Founder/Investor/Operator/Expert)

## Usage

Run specific steps:
```bash
# Step 1: Just ProxyCurl enrichment with how many records you want to do
python main.py --step enrich --max-records 1000 

# Step 2: Just OpenAI analysis with how many records you want to do
python main.py --step analyze --max-records 1000

# Run both steps with how many records you want to do
python main.py --step both --max-records 1000
```

Parameters:
- `--step`: Choose 'enrich', 'analyze', or 'both'
- `--max-records`: Maximum number of records to process
- `--batch-size`: Records per batch (default: 50)

## Processing Details

1. **Rate Limits**:
   - ProxyCurl: 200 requests/minute
   - Batch updates to Airtable
   - No OpenAI rate limits at current scale

2. **Costs** (for 10,000 profiles):
   - ProxyCurl: $0.02/profile = $200
   - OpenAI: $0.002/profile = $20
   - Total: ~$220

3. **Processing Time**:
   - ProxyCurl: ~50 minutes per 10,000 profiles
   - OpenAI: Additional processing time
   - Airtable: Batch updates

## Output Format

The tool updates Airtable with:
1. Raw enriched data from ProxyCurl
2. Formatted company list with current position
3. AI summary and classification
4. Formatted location

## Troubleshooting

1. **Rate Limits**: If hitting ProxyCurl limits:
   - Use --step enrich for just enrichment
   - Decrease batch size
   - Monitor ProxyCurl dashboard

2. **API Errors**:
   - Check credentials
   - Verify LinkedIn URLs
   - Check log files

3. **Memory Issues**:
   - Process in smaller batches
   - Use --max-records to limit size
   - Run enrichment and analysis separately

## File Structure
```
networks_ai/
├── config/
│   ├── credentials.py.example
│   └── credentials.py
├── src/
│   ├── enricher.py
│   ├── rate_limiter.py
│   └── utils.py
├── logs/
├── requirements.txt
└── main.py
```

## Support

For issues or improvements:
1. Check logs in logs/ directory
2. Verify API credentials
3. Check rate limits on ProxyCurl dashboard
