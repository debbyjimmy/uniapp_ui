import streamlit as st
import pandas as pd
import time
import sys
import os
import uuid

# Add the project root to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from shared.auth import require_login
from shared.gcp_utils import get_bucket_manager

# Require login
require_login()

def main():
    st.title("🌐 Domain Relationship Analyzer")
    
    # Back to dashboard button
    if st.button("← Back to Dashboard"):
        st.session_state.selected_tool = None
        st.rerun()
    
    # --- GCS Client Setup ---
    try:
        bucket_manager = get_bucket_manager('domain_relationship')
        client = bucket_manager.client
        bucket = bucket_manager.bucket
        bucket_name = bucket_manager.bucket_name
    except Exception as e:
        st.error(f"GCP not available: {e}")
        st.stop()
    
    # Session ID download box (sidebar)
    with st.sidebar:
        st.markdown("---")
        st.subheader("📥 Download by Session ID")
        st.markdown("Enter a session ID to download results from a previous session.")
        
        lookup_id = st.text_input(
            "Session ID",
            placeholder="e.g., a7c29fdd",
            help="Enter the session ID from a previous upload"
        )
        
        if st.button("🔍 Check & Download", key="check_session"):
            if lookup_id:
                check_session_results(lookup_id, bucket_manager)
            else:
                st.warning("Please enter a session ID")
    
    st.markdown("---")
    
    # Tool description
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **How it works:**
        1. Upload CSV with domain pairs
        2. Tool extracts company names from domains
        3. Analyzes relationships between companies
        4. Results show relationship type and confidence
        
        **Expected CSV format:**
        - Two columns: domain1, domain2
        - Example: apple.com, microsoft.com
        - Will be automatically split into chunks for large datasets
        """)
    
    with col2:
        st.info("""
        **Relationship Types:**
        - Direct: Same company
        - Subsidiary/Parent: One owns the other
        - Sister: Both have same parent
        - Brand: Brand relationship
        - Acquired/Rebrand: One acquired the other
        - Partner/Vendor: Business partnership
        - Competitor: Direct competitors
        - None: No relationship
        """)
    
    st.markdown("---")
    
    # --- File Upload ---
    uploaded_file = st.file_uploader("Upload CSV with domain pairs", type=["csv"])
    
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            st.write(f"✅ Dataframe loaded: {len(df)} domain pairs")
            
            # Check required columns
            required_columns = ['domain1', 'domain2']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                st.error(f"❌ Missing required columns: {missing_columns}")
                st.info("Required columns: domain1, domain2")
                return
            
            # Show sample data
            st.subheader("📋 Sample Data")
            st.dataframe(df.head(), use_container_width=True)
            
            # Processing options
            col1, col2 = st.columns(2)
            with col1:
                chunk_size = st.number_input(
                    "Chunk Size (rows per chunk)",
                    min_value=10,
                    max_value=1000,
                    value=50,
                    help="Smaller chunks = faster processing, more chunks"
                )
            
            with col2:
                if len(df) > chunk_size:
                    st.info(f"📊 Large dataset detected: {len(df)} rows will be split into {(len(df) + chunk_size - 1) // chunk_size} chunks")
                else:
                    st.info(f"📊 Small dataset: {len(df)} rows will be processed as single chunk")
            
            # Process button
            if st.button("🚀 Analyze Domain Relationships", type="primary"):
                if len(df) > chunk_size:
                    process_large_dataset_chunked(df, uploaded_file, chunk_size)
                else:
                    process_small_dataset(df, uploaded_file)
        
        except Exception as e:
            st.error(f"❌ Error reading CSV: {e}")

def process_small_dataset(df, uploaded_file):
    """Process small datasets (single chunk)"""
    try:
        # Generate session ID
        session_id = str(uuid.uuid4())[:8]
        
        # Display session ID
        st.markdown("---")
        st.subheader("📋 Session ID")
        st.code(session_id, language="text")
        st.info("💡 Copy the Session ID above to track progress or download results later")
        
        # Upload file
        bucket_manager = get_bucket_manager('domain_relationship')
        filename = f"{session_id}_{uploaded_file.name}"
        job_id = bucket_manager.upload_input_file(uploaded_file.getvalue(), filename)
        
        if job_id:
            st.success("✅ File uploaded successfully!")
            st.session_state.current_job_id = job_id
            st.rerun()
        else:
            st.error("❌ Failed to upload file")
    
    except Exception as e:
        st.error(f"❌ Error processing file: {e}")

def process_large_dataset_chunked(df, uploaded_file, chunk_size):
    """Process large datasets in chunks with progress tracking"""
    total_rows = len(df)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    
    st.info(f"🚀 Starting chunked processing: {total_chunks} chunks of {chunk_size} domain pairs each")
    
    # Generate base session ID for all chunks (UUID-based like Contact Scraper)
    base_session_id = str(uuid.uuid4())[:8]
    
    # Display base session ID prominently for copying
    st.markdown("---")
    st.subheader("📋 Session ID")
    st.code(base_session_id, language="text")
    st.info("💡 Copy the Session ID above to track progress or download results later")
    
    # Create progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    all_results = []
    successful_chunks = 0
    failed_chunks = 0
    
    try:
        bucket_manager = get_bucket_manager('domain_relationship')
        
        for chunk_idx in range(total_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, total_rows)
            
            # Update progress
            progress = (chunk_idx + 1) / total_chunks
            progress_bar.progress(progress)
            status_text.text(f"Processing chunk {chunk_idx + 1}/{total_chunks} (domain pairs {start_idx + 1}-{end_idx})")
            
            # Extract chunk
            chunk_df = df.iloc[start_idx:end_idx].copy()
            
            # Create temporary file for chunk
            import io
            chunk_csv = chunk_df.to_csv(index=False)
            chunk_file = io.BytesIO(chunk_csv.encode('utf-8'))
            
            # Upload chunk with simplified naming (like Contact Scraper)
            chunk_filename = f"{base_session_id}_chunk{chunk_idx + 1}.csv"
            
            try:
                job_id = bucket_manager.upload_input_file(chunk_file.getvalue(), chunk_filename)
                
                if job_id:
                    # Wait for chunk to complete
                    chunk_completed = wait_for_chunk_completion(job_id, bucket_manager, chunk_idx + 1, total_chunks)
                    
                    if chunk_completed:
                        # Download chunk results using the simplified chunk filename
                        chunk_results_filename = f"{base_session_id}_chunk{chunk_idx + 1}.csv"
                        chunk_results = bucket_manager.download_results_by_filename(chunk_results_filename)
                        if chunk_results:
                            chunk_df_results = pd.read_csv(pd.io.common.BytesIO(chunk_results))
                            all_results.append(chunk_df_results)
                            successful_chunks += 1
                        else:
                            st.warning(f"⚠️ Chunk {chunk_idx + 1} completed but no results found")
                    else:
                        st.warning(f"⚠️ Chunk {chunk_idx + 1} timed out")
                        failed_chunks += 1
                else:
                    st.warning(f"⚠️ Chunk {chunk_idx + 1} upload failed")
                    failed_chunks += 1
                    
            except Exception as e:
                st.warning(f"⚠️ Chunk {chunk_idx + 1} error: {e}")
                failed_chunks += 1
        
        # Final progress update
        progress_bar.progress(1.0)
        status_text.text("Processing complete!")
        
        # Combine results
        if all_results:
            combined_results = pd.concat(all_results, ignore_index=True)
            
            # Save combined results with base session ID
            combined_filename = f"{base_session_id}_combined_results.csv"
            
            # Upload combined results directly to results folder (like Contact Scraper)
            try:
                combined_csv = combined_results.to_csv(index=False)
                blob = bucket_manager.bucket.blob(f"results/{combined_filename}")
                blob.upload_from_string(combined_csv, content_type="text/csv")
                
                st.success(f"✅ Chunked processing completed! {successful_chunks}/{total_chunks} chunks successful")
                
                # Display base session ID prominently for copying
                st.markdown("---")
                st.subheader("📋 Session ID")
                st.code(base_session_id, language="text")
                st.info("💡 Copy the Session ID above to download results later or share with others")
                
                # Provide download button for combined results
                st.download_button(
                    "⬇️ Download Combined Results",
                    combined_csv,
                    file_name=combined_filename,
                    mime="text/csv"
                )
                
                st.session_state.current_job_id = base_session_id
                st.rerun()
            except Exception as e:
                st.error(f"❌ Failed to save combined results: {e}")
        else:
            st.error("❌ No chunks processed successfully")
            
    except Exception as e:
        st.error(f"❌ Chunked processing failed: {e}")
    finally:
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()

def wait_for_chunk_completion(job_id, bucket_manager, chunk_num, total_chunks, max_wait_time=300):
    """Wait for a chunk to complete with timeout"""
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        try:
            status = bucket_manager.check_job_status(job_id)
            if status and status.get('status') == 'completed':
                return True
            elif status and status.get('status') == 'failed':
                st.error(f"❌ Chunk {chunk_num} failed: {status.get('message', 'Unknown error')}")
                return False
        except Exception as e:
            st.warning(f"⚠️ Error checking chunk {chunk_num} status: {e}")
        
        time.sleep(5)  # Check every 5 seconds
    
    st.warning(f"⏰ Chunk {chunk_num} timed out after {max_wait_time} seconds")
    return False

def check_session_results(lookup_id, bucket_manager):
    """Check and display results for a specific session ID"""
    try:
        # Try to find results with this session ID
        results = bucket_manager.download_results(lookup_id)
        
        if results:
            st.sidebar.success("✅ Results found!")
            st.sidebar.download_button(
                "⬇️ Download Results",
                results,
                file_name=f"domain_relationship_results_{lookup_id}.csv",
                mime="text/csv"
            )
        else:
            st.sidebar.info("⏳ No results found for this session ID yet")
    except Exception as e:
        st.sidebar.error(f"Error checking session: {e}")

if __name__ == "__main__":
    main()
