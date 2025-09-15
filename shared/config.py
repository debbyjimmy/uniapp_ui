# Configuration for the unified automation tools app
import os

# Team login credentials - use environment variables or Streamlit secrets
def get_team_credentials():
    """Get team credentials from environment variables or Streamlit secrets"""
    try:
        import streamlit as st
        # Try Streamlit secrets first
        if hasattr(st.secrets, 'team_credentials'):
            return st.secrets.team_credentials
    except:
        pass
    
    # Fallback to environment variables
    return {
        "username": os.getenv("TEAM_USERNAME", "team"),
        "password": os.getenv("TEAM_PASSWORD", "12345")
    }

def get_api_keys():
    """Get API keys from Streamlit secrets"""
    try:
        import streamlit as st
        if hasattr(st.secrets, 'api_keys'):
            return st.secrets.api_keys
    except:
        pass
    
    # Fallback to environment variables
    return {
        "openai_api_key": os.getenv("OPENAI_API_KEY"),
        "serper_api_key": os.getenv("SERPER_API_KEY")
    }

def get_gcp_config():
    """Get GCP configuration from Streamlit secrets"""
    try:
        import streamlit as st
        if hasattr(st.secrets, 'gcp'):
            return st.secrets.gcp
    except:
        pass
    
    # Fallback to environment variables
    return {
        "project_id": os.getenv("GCP_PROJECT_ID", "contact-scraper-463913"),
        "region": os.getenv("GCP_REGION", "us-central1")
    }

TEAM_CREDENTIALS = get_team_credentials()

# GCP Configuration - use Streamlit secrets or environment variables
gcp_config = get_gcp_config()
GCP_PROJECT_ID = gcp_config.get("project_id", "contact-scraper-463913")
GCP_REGION = gcp_config.get("region", "us-central1")

# Tool configurations with separate buckets
TOOLS_CONFIG = {
    "contact_scraper": {
        "name": "Contact Scraper",
        "icon": "💼",
        "description": "Extract contact job profile",
        "bucket": "contact-scraper-bucket",
        "input_folder": "input",
        "results_folder": "results",
        "status_folder": "status"
    },
    "name_cleaner": {
        "name": "Name Cleaner",
        "icon": "🧹",
        "description": "Clean and standardize company names",
        "bucket": "name-cleaner-bucket",
        "input_folder": "input",
        "results_folder": "results",
        "status_folder": "status"
    },
    "lead_search": {
        "name": "Lead Search Agent",
        "icon": "🔍",
        "description": "Find and validate business leads",
        "bucket": "leadsearchagent",
        "input_folder": "input",
        "results_folder": "results",
        "status_folder": "status"
    },
    "company_relationship": {
        "name": "Company Relationship Verifier",
        "icon": "🔗",
        "description": "Verify company relationships using AI analysis",
        "bucket": "companyrelationship",
        "input_folder": "input",
        "results_folder": "results",
        "status_folder": "status"
    },
    "website_resolver": {
        "name": "Website Resolver",
        "icon": "🔎",
        "description": "Verify company website relationships",
        "bucket": "website-resolver-bucket",
        "input_folder": "input",
        "results_folder": "results",
        "status_folder": "status"
    }
}

# Job status tracking
JOB_STATUSES = {
    "pending": "⏳ Pending",
    "processing": "🔄 Processing",
    "completed": "✅ Completed",
    "failed": "❌ Failed"
}

# File monitoring settings
POLL_INTERVAL = 5  # seconds between status checks
MAX_WAIT_TIME = 300  # maximum seconds to wait for completion
