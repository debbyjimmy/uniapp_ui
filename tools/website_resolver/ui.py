import streamlit as st
import pandas as pd
import time
import sys
import os

# Add the project root to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from shared.auth import require_login
from shared.gcp_utils import get_bucket_manager

# Require login
require_login()

# Page config
st.set_page_config(
    page_title="Website Resolver - Automation Tools Hub",
    page_icon="🔎",
    layout="wide"
)

def main():
    st.title("🔎 Website Resolver")
    st.markdown("Verify company website relationships using AI-powered analysis.")
    
    # Back to dashboard button
    if st.button("← Back to Dashboard"):
        st.session_state.selected_tool = None
        st.rerun()
    
    st.markdown("---")
    
    # Tool description
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **How it works:**
        1. Upload CSV with Company_Name and Company_Website columns
        2. File is processed in GCP using AI analysis
        3. Download results when processing is complete
        
        **Expected CSV format:**
        - Company_Name: Company name to verify
        - Company_Website: Website to check (optional)
        """)
    
    with col2:
        st.info("""
        **Status:**
        - ⏳ Pending: File uploaded, waiting to process
        - 🔄 Processing: AI analysis in progress
        - ✅ Completed: Results ready for download
        - ❌ Failed: Processing error occurred
        """)
    
    st.markdown("---")
    
    # File upload section
    st.subheader("📤 Upload Input File")
    
    uploaded_file = st.file_uploader(
        "Choose CSV file",
        type=['csv'],
        help="Upload CSV with Company_Name and Company_Website columns"
    )
    
    if uploaded_file:
        # Show preview
        try:
            df = pd.read_csv(uploaded_file)
            st.success(f"✅ File loaded: {len(df)} rows")
            
            # Validate columns
            required_cols = ['Company_Name', 'Company_Website']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                st.error(f"❌ Missing required columns: {', '.join(missing_cols)}")
                st.stop()
            
            # Show preview
            with st.expander("📋 Data Preview"):
                st.dataframe(df.head(), use_container_width=True)
            
            # Upload button
            if st.button("🚀 Start Processing", type="primary"):
                process_file(uploaded_file, df)
                
        except Exception as e:
            st.error(f"❌ Error reading file: {e}")
    
    # Job monitoring section
    if 'current_job_id' in st.session_state:
        st.markdown("---")
        st.subheader("📊 Job Status")
        monitor_job(st.session_state.current_job_id)

def process_file(uploaded_file, df):
    """Process uploaded file and upload to GCP"""
    try:
        # Get bucket manager
        bucket_manager = get_bucket_manager('website_resolver')
        
        # Upload file
        with st.spinner("📤 Uploading file to GCP..."):
            file_data = uploaded_file.read()
            job_id = bucket_manager.upload_input_file(file_data, uploaded_file.name)
        
        if job_id:
            st.session_state.current_job_id = job_id
            st.success(f"✅ File uploaded successfully! Job ID: {job_id}")
            st.info("🔄 Processing will start automatically. Monitor progress below.")
            st.rerun()
        else:
            st.error("❌ Upload failed. Please try again.")
            
    except Exception as e:
        st.error(f"❌ Error processing file: {e}")

def monitor_job(job_id: str):
    """Monitor job progress and show results"""
    try:
        bucket_manager = get_bucket_manager('website_resolver')
        
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
            st.info("🔄 AI analysis in progress...")
            
        elif current_status == 'completed':
            st.success("✅ Processing completed! Results are ready.")
            
            # Download results
            if st.button("⬇️ Download Results"):
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

def download_results(job_id: str, bucket_manager):
    """Download and display results"""
    try:
        with st.spinner("⬇️ Downloading results..."):
            results_data = bucket_manager.download_results(job_id)
        
        if results_data:
            # Convert to DataFrame
            df_results = pd.read_csv(pd.io.common.BytesIO(results_data))
            
            st.success(f"✅ Results downloaded: {len(df_results)} rows")
            
            # Display results
            st.dataframe(df_results, use_container_width=True)
            
            # Download button
            csv_data = df_results.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "💾 Save Results CSV",
                data=csv_data,
                file_name=f"website_resolver_results_{job_id}.csv",
                mime="text/csv"
            )
            
            # Clear job from session
            del st.session_state.current_job_id
            
        else:
            st.error("❌ Failed to download results")
            
    except Exception as e:
        st.error(f"❌ Error downloading results: {e}")

if __name__ == "__main__":
    main()
