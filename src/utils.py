import logging
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

def setup_logging():
    """Setup logging configuration"""
    log_filename = f"logs/enricher_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

def parse_openai_response(response):
    """Parse OpenAI response into components"""
    try:
        sections = response.split("SUMMARY:")
        companies = sections[0].replace("COMPANIES:", "").strip()
        
        remaining_sections = sections[1].split("LOCATION:")
        summary = remaining_sections[0].strip()
        location = remaining_sections[1].strip() if len(remaining_sections) > 1 else ""
        
        return companies, summary, location
    except Exception as e:
        logging.error(f"Error parsing OpenAI response: {str(e)}")
        return "", "", ""