import streamlit as st
import pandas as pd
import time
import sys
import os
from datetime import datetime

# Add the project root to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from shared.auth import require_login
from shared.gcp_utils import get_bucket_manager

# Require login
require_login()

# Page config
st.set_page_config(
    page_title="Company Relationship Verifier - Automation Tools Hub",
    page_icon="🔗",
    layout="wide"
)

def main():

    st.title("🔗 Company Relationship Verifier")
    st.markdown("Verify relationships between companies using AI-powered analysis")
    
    # Back to dashboard button
    if st.button("← Back to Dashboard"):
        st.session_state.selected_tool = None
        st.rerun()
    
    

    # Initialize GCP bucket manager
    try:
        bucket_manager = get_bucket_manager('company_relationship')
        gcp_available = True
    except Exception as e:
        st.error(f"GCP not available: {e}")
        gcp_available = False

    # Session ID download box (top right)
    with st.sidebar:
        st.divider()
        st.subheader("📥 Download by Session ID")
        st.markdown("Enter a job ID to download results from a previous upload.")
        
        session_id = st.text_input(
            "Job ID",
            placeholder="e.g., job_20250828_134757",
            help="Enter the job ID from a previous upload"
        )
        
        if st.button("🔍 Check Status", key="check_session"):
            if session_id:
                # Store session ID in session state for later use
                st.session_state.checked_session_id = session_id
                check_session_status(session_id, bucket_manager)
            else:
                st.warning("Please enter a job ID")
        

    # Check GCP availability
    if not gcp_available:
        st.error("❌ GCP connection failed. Please check your configuration.")
        st.stop()

    st.divider()

    # Tool description
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **How it works:**
        1. Upload CSV with company relationship details (ec_id, contact_full_name, provided_company, linkedin_url, experience_companies)
        2. File is processed in GCP using AI analysis and web search
        3. Download results with relationship verification and evidence
        
        **Expected CSV format:**
        - ec_id: Unique identifier
        - provided_company: Company to verify
        - contact_full_name: Person's full name
        - Title: Job title/position
        - linkedin_url: LinkedIn profile URL
        - experience_companies: Semicolon-separated list of companies from experience
        """)

    with col2:
        st.info("""
        **Output columns:**
        - connected?: Yes/No (relationship found)
        - connection_type: Direct/Subsidiary/Brand/Partner/etc.
        - confidence: Score (0-100)
        - evidence_urls: Source URLs
        - explanation: AI reasoning
        """)

    st.divider()

    # File upload section
    st.subheader("📤 Upload Company List")
    st.markdown("""
    **Required columns:**
    - `ec_id`: Unique identifier for the contact
    - `provided_company`: Company name to verify
    - `contact_full_name`: Full name of the contact
    - `Title`: Job title/position
    - `linkedin_url`: LinkedIn profile URL
    - `experience_companies`: Semicolon-separated list of companies from experience
    """)

    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type=['csv'],
        help="Upload a CSV file with the required columns"
    )

    if uploaded_file is not None:
        # Preview the data
        try:
            df = pd.read_csv(uploaded_file)
            st.success(f"✅ File loaded successfully! {len(df)} rows found")
            
            # Show data preview
            st.subheader("📊 Data Preview")
            st.dataframe(df.head(), use_container_width=True)
            
            # Validate required columns
            required_columns = ['ec_id', 'provided_company', 'contact_full_name', 'Title', 'linkedin_url', 'experience_companies']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                st.error(f"❌ Missing required columns: {missing_columns}")
                st.stop()
            
            st.success("✅ All required columns are present")
            
            # Start processing button
            if st.button("🚀 Start Company Relationship Validation", type="primary"):
                process_file(uploaded_file, bucket_manager)
                
        except Exception as e:
            st.error(f"❌ Error reading file: {e}")
    
    # Job monitoring section
    if 'current_job_id' in st.session_state:
        st.divider()
        st.subheader("📊 Job Status")
        monitor_job(st.session_state.current_job_id, bucket_manager)
    
    # Session download section
    if 'checked_session_id' in st.session_state:
        st.divider()
        st.subheader("📥 Session Download")
        st.write(f"Session ID: {st.session_state.checked_session_id}")
        
        # Check if this session is completed
        try:
            status = bucket_manager.check_job_status(st.session_state.checked_session_id)
            if status and status.get("status") == "completed":
                if st.button("📥 Download Results", key="download_session_main"):
                    st.session_state.current_session_id = st.session_state.checked_session_id
                    st.rerun()
            else:
                st.warning(f"Session status: {status.get('status', 'unknown')}")
        except Exception as e:
            st.error(f"Error checking session: {e}")
    
    # Session results section
    if 'current_session_id' in st.session_state:
        st.divider()
        st.subheader("📊 Session Results")
        display_session_results(st.session_state.current_session_id, bucket_manager)
    

    # Footer
    st.divider()
    st.caption("Company Relationship Verifier - Powered by OpenAI GPT-4 and Serper API")

# Functions - Define all functions outside main()
def process_file(uploaded_file, bucket_manager):
    """Process uploaded file and upload to GCP"""
    try:
        # Upload file
        with st.spinner("📤 Uploading company list to GCP..."):
            # Reset file pointer to beginning
            uploaded_file.seek(0)
            file_data = uploaded_file.read()
            job_id = bucket_manager.upload_input_file(file_data, uploaded_file.name)
        
        if job_id:
            st.session_state.current_job_id = job_id
            st.success(f"✅ Company list uploaded successfully! Job ID: {job_id}")
            st.info("🔄 AI validation will start automatically. Monitor progress below.")
            st.rerun()
        else:
            st.error("❌ Upload failed. Please try again.")
            
    except Exception as e:
        st.error(f"❌ Error processing file: {e}")

def monitor_job(job_id: str, bucket_manager):
    """Monitor job progress and show results"""
    try:
        # Check current status
        status = bucket_manager.check_job_status(job_id)
        
        # Display status
        status_emoji = {
            'pending': '⏳',
            'processing': '🔄',
            'completed': '✅',
            'failed': '❌',
            'timeout': '⏰'
        }
        
        current_status = status.get('status', 'unknown')
        emoji = status_emoji.get(current_status, '❓')
        
        st.markdown(f"**Job ID:** {job_id}")
        st.markdown(f"**Status:** {emoji} {current_status.title()}")
        
        if 'timestamp' in status:
            st.markdown(f"**Started:** {status['timestamp']}")
        
        # Handle different statuses
        if current_status == 'pending':
            st.info("⏳ File uploaded and waiting to be processed...")
            
        elif current_status == 'processing':
            st.info("🔄 AI company relationship validation in progress...")
            
        elif current_status == 'completed':
            st.success("✅ Company relationship validation completed! Results are ready.")
            
            # Download results
            if st.button("⬇️ Download Results", key=f"download_btn_{job_id}"):
                download_results(job_id, bucket_manager)
                
        elif current_status == 'failed':
            st.error(f"❌ Processing failed: {status.get('error', 'Unknown error')}")
            
        elif current_status == 'timeout':
            st.warning("⏰ Job took too long to complete. Please check GCP logs.")
        
        # Auto-refresh
        if current_status in ['pending', 'processing']:
            time.sleep(2)
            st.rerun()
            
    except Exception as e:
        st.error(f"❌ Error monitoring job: {e}")

def download_results(job_id, bucket_manager):
    """Download and display the processing results"""
    try:
        with st.spinner("📥 Downloading results..."):
            # Get results as DataFrame
            results = bucket_manager.download_results_as_dataframe(job_id)
        
        if results is not None and not results.empty:
            # Display results
            st.subheader("📊 Results")
            st.dataframe(results, use_container_width=True)
            
            # Download button
            csv_data = results.to_csv(index=False)
            st.download_button(
                label="📥 Download Results CSV",
                data=csv_data,
                file_name=f"{job_id}_results.csv",
                mime="text/csv",
                key=f"download_{job_id}",
                help="Click to download the results as CSV"
            )
            
            # Show statistics
            if 'connected?' in results.columns:
                connection_stats = results['connected?'].value_counts()
                st.subheader("📈 Connection Statistics")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Records", len(results))
                with col2:
                    st.metric("Connected", connection_stats.get('yes', 0))
                with col3:
                    st.metric("Not Connected", connection_stats.get('no', 0))
                
                # Connection type breakdown
                if 'connection_type' in results.columns:
                    st.subheader("🔗 Connection Type Breakdown")
                    type_stats = results['connection_type'].value_counts()
                    st.bar_chart(type_stats)
                    
        else:
            st.warning("⚠️ No results found or results file is empty")
            st.info("💡 The job may still be processing or the results file is corrupted")
            
    except Exception as e:
        st.error(f"❌ Error downloading results: {e}")
        st.exception(e)

def check_session_status(session_id, bucket_manager):
    """Check the status of a specific session"""
    if not session_id:
        st.warning("Please enter a session ID")
        return
    
    try:
        # Use the correct method name
        status = bucket_manager.check_job_status(session_id)
        
        if status and status.get("status") != "not_found":
            st.success(f"✅ Session found: {session_id}")
            st.json(status)
            
            if status.get("status") == "completed":
                st.success("✅ Session completed! Use the download button below to get results.")
            elif status.get("status") == "failed":
                st.error(f"❌ Job failed: {status.get('error', 'Unknown error')}")
            elif status.get("status") == "pending":
                st.info("⏳ Job is still pending...")
            else:
                st.info(f"📊 Job status: {status.get('status')}")
        else:
            st.warning(f"❌ No session found with ID: {session_id}")
            st.info("💡 Make sure the job ID is correct and the job has been processed")
            
    except Exception as e:
        st.error(f"❌ Error checking session: {e}")
        st.exception(e)

def display_session_results(session_id, bucket_manager):
    """Display session results in the main UI (same as download_results but for sessions)"""
    try:
        with st.spinner("📥 Loading session results..."):
            # Get results as DataFrame
            results = bucket_manager.download_results_as_dataframe(session_id)
        
        if results is not None and not results.empty:
            # Display results
            st.subheader("📊 Results")
            st.dataframe(results, use_container_width=True)
            
            # Download button
            csv_data = results.to_csv(index=False)
            st.download_button(
                label="📥 Download Results CSV",
                data=csv_data,
                file_name=f"{session_id}_results.csv",
                mime="text/csv",
                key=f"download_{session_id}",
                help="Click to download the results as CSV"
            )
            
            # Show statistics
            if 'connected?' in results.columns:
                connection_stats = results['connected?'].value_counts()
                st.subheader("📈 Connection Statistics")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Records", len(results))
                with col2:
                    st.metric("Connected", connection_stats.get('yes', 0))
                with col3:
                    st.metric("Not Connected", connection_stats.get('no', 0))
                
                # Connection type breakdown
                if 'connection_type' in results.columns:
                    st.subheader("🔗 Connection Type Breakdown")
                    type_stats = results['connection_type'].value_counts()
                    st.bar_chart(type_stats)
                    
        else:
            st.warning("⚠️ No results found or results file is empty")
            st.info("💡 The job may still be processing or the results file is corrupted")
            
    except Exception as e:
        st.error(f"❌ Error loading session results: {e}")
        st.exception(e)
