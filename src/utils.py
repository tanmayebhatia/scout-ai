import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import json
import re
from typing import Dict, Any, List, Optional, Tuple

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

def parse_openai_response(response_str):
    """Parse OpenAI response to extract the fields we need"""
    try:
        # Try parsing as valid JSON first
        try:
            response_data = json.loads(response_str)
            companies = response_data.get("previous_companies", "")
            summary = response_data.get("summary", "")
            location = response_data.get("location", "")
            return companies, summary, location
        except json.JSONDecodeError:
            # If not valid JSON, use regex parsing
            pass
        
        # Extract companies, summary, and location using pattern matching
        sections = response_str.split("SUMMARY:")
        companies = ""
        summary = ""
        location = ""
        
        if len(sections) > 1:
            companies = sections[0].replace("PREVIOUS_COMPANIES:", "").strip()
            remaining_sections = sections[1].split("LOCATION:")
            summary = remaining_sections[0].strip() if len(remaining_sections) > 0 else ""
            location = remaining_sections[1].strip() if len(remaining_sections) > 1 else ""
        
        return companies, summary, location
        
    except Exception as e:
        logging.error(f"Error parsing OpenAI response: {e}")
        logging.error(f"Response string: {response_str}")
        return "", "", ""

def extract_fields_from_enriched_data(enriched_data: Dict[str, Any], linkedin_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Extract all relevant fields from enriched data for Airtable
    
    Args:
        enriched_data: The raw enriched data from ProxyCurl
        linkedin_url: Optional LinkedIn URL to include in the extracted fields
        
    Returns:
        Dictionary with all extracted fields with appropriate anchor prefixes
    """
    extracted = {}
    
    try:
        # Extract current role
        current_role = ""
        
        # First try occupation field
        if enriched_data.get('occupation'):
            current_role = enriched_data.get('occupation')
        # Then try headline
        elif enriched_data.get('headline'):
            current_role = enriched_data.get('headline')
        # Then try job_title + company
        elif enriched_data.get('job_title'):
            current_role = enriched_data.get('job_title')
            if enriched_data.get('company'):
                current_role += f" at {enriched_data.get('company')}"
        # Finally try to get most recent experience
        elif enriched_data.get('experiences') and len(enriched_data.get('experiences')) > 0:
            # Sort experiences by start date, most recent first
            experiences = sorted(
                [e for e in enriched_data.get('experiences') if e.get('starts_at')],
                key=lambda x: (
                    x['starts_at'].get('year', 0),
                    x['starts_at'].get('month', 0) if x['starts_at'].get('month') else 0,
                    x['starts_at'].get('day', 0) if x['starts_at'].get('day') else 0
                ),
                reverse=True
            )
            
            if experiences:
                current_exp = experiences[0]
                title = current_exp.get('title', '')
                company = current_exp.get('company', '')
                if title and company:
                    current_role = f"{title} at {company}"
                elif title:
                    current_role = title
                elif company:
                    current_role = f"Works at {company}"
        
        if current_role:
            extracted['⚓️ Current Roles'] = current_role
            
        # Extract education list
        education_list = []
        if enriched_data.get('education'):
            for edu in enriched_data.get('education', []):
                school = edu.get('school', '')
                degree = edu.get('degree_name', '')
                field = edu.get('field_of_study', '')
                
                edu_str = ""
                if degree and field and school:
                    edu_str = f"{degree} in {field} from {school}"
                elif degree and school:
                    edu_str = f"{degree} from {school}"
                elif school:
                    edu_str = school
                    
                if edu_str:
                    education_list.append(edu_str)
        
        if education_list:
            # Join as a string for better compatibility
            extracted['⚓️ Education'] = ", ".join(education_list)
        
        # Extract location
        location_parts = []
        if enriched_data.get('location'):
            extracted['⚓️ Location'] = enriched_data.get('location')
        else:
            if enriched_data.get('city'):
                location_parts.append(enriched_data.get('city'))
                
            if enriched_data.get('state'):
                location_parts.append(enriched_data.get('state'))
                
            if enriched_data.get('country_full_name'):
                location_parts.append(enriched_data.get('country_full_name'))
            elif enriched_data.get('country'):
                location_parts.append(enriched_data.get('country'))
            
            if location_parts:
                extracted['⚓️ Location'] = ", ".join(location_parts)
            
        # Extract past roles
        past_roles = []
        if enriched_data.get('experiences'):
            # Sort experiences by start date, most recent first
            experiences = sorted(
                [e for e in enriched_data.get('experiences') if e.get('starts_at')],
                key=lambda x: (
                    x['starts_at'].get('year', 0),
                    x['starts_at'].get('month', 0) if x['starts_at'].get('month') else 0,
                    x['starts_at'].get('day', 0) if x['starts_at'].get('day') else 0
                ),
                reverse=True
            )
            
            # Skip first one if it's the current role
            start_idx = 1 if len(experiences) > 1 else 0
            
            for exp in experiences[start_idx:]:
                title = exp.get('title', '')
                company = exp.get('company', '')
                if title and company:
                    past_roles.append(f"{title} at {company}")
                elif title:
                    past_roles.append(title)
                elif company:
                    past_roles.append(f"Worked at {company}")
        
        if past_roles:
            extracted['⚓️ Past Roles'] = ", ".join(past_roles)
        
        # Calculate work experience years
        years = 0
        if enriched_data.get('experiences'):
            # Get current year
            current_year = datetime.now().year
            
            # Track start and end years, handling overlaps
            timeline = []
            
            for exp in enriched_data.get('experiences'):
                # Skip experiences without start dates
                if not exp.get('starts_at') or not exp['starts_at'].get('year'):
                    continue
                    
                start_year = exp['starts_at'].get('year')
                
                # Handle end date
                if exp.get('ends_at') and exp['ends_at'].get('year'):
                    end_year = exp['ends_at'].get('year')
                else:
                    # If no end date, assume it's current (use current year)
                    end_year = current_year
                    
                # Sanity check years
                if start_year > 1900 and end_year <= current_year:
                    timeline.append((start_year, end_year))
            
            # Sort by start year
            timeline.sort()
            
            # Merge overlapping periods
            if timeline:
                merged = [timeline[0]]
                
                for current in timeline[1:]:
                    prev = merged[-1]
                    
                    # Check if current period overlaps with previous
                    if current[0] <= prev[1]:
                        # Merge periods
                        merged[-1] = (prev[0], max(prev[1], current[1]))
                    else:
                        # Add new period
                        merged.append(current)
                
                # Calculate total years
                for start, end in merged:
                    years += end - start
        
        if years > 0:
            extracted['⚓️ Work Experience (yrs)'] = years
        
        # Add raw enriched data
        extracted['⚓️ Raw_Enriched_Data'] = json.dumps(enriched_data)
        
        # Add LinkedIn URL if provided
        if linkedin_url:
            extracted['linkedin_url'] = linkedin_url
            
        # Get name for record
        name = enriched_data.get('full_name', '')
        if name:
            extracted['⚓️ Name'] = name
        
    except Exception as e:
        logging.error(f"Error extracting fields from enriched data: {str(e)}")
        logging.exception("Detailed traceback:")
    
    return extracted