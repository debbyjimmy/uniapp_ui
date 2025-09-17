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
            
            # Show dataset size and chunking info
            total_rows = len(df)
            st.info(f"📊 Dataset size: **{total_rows} companies**")
            
            # Chunking options for large datasets
            chunk_size = 50  # Default chunk size
            if total_rows > 100:
                st.warning("⚠️ Large dataset detected! Processing will be done in chunks to avoid timeouts.")
                chunk_size = st.selectbox(
                    "Chunk size (companies per batch):",
                    [25, 50, 75, 100],
                    index=1,
                    help="Smaller chunks are more reliable but take longer overall"
                )
                st.info(f"📦 Will process {total_rows} companies in {(total_rows + chunk_size - 1) // chunk_size} chunks of {chunk_size} companies each")
            
            # Upload button
            if st.button("🚀 Start Processing", type="primary"):
                if total_rows > 100:
                    # Process in chunks
                    process_large_dataset_chunked(df, uploaded_file, chunk_size)
                else:
                    # Process normally for small datasets
                    process_small_dataset(df, uploaded_file)
                
        except Exception as e:
            st.error(f"❌ Error reading file: {e}")
    
    # Job monitoring section
    if 'current_job_id' in st.session_state:
        st.markdown("---")
        st.subheader("📊 Job Status")
        monitor_job(st.session_state.current_job_id)

def process_small_dataset(df, uploaded_file):
    """Process small datasets normally (original behavior)"""
    try:
        # Get bucket manager
        bucket_manager = get_bucket_manager('website_resolver')
        
        # Upload file
        with st.spinner("📤 Uploading file to GCP..."):
            file_data = uploaded_file.read()
            job_id = bucket_manager.upload_input_file(file_data, uploaded_file.name)
        
        if job_id:
            st.success(f"✅ File uploaded successfully!")
            
            # Display job ID prominently for copying
            st.markdown("---")
            st.subheader("📋 Job ID")
            st.code(job_id, language="text")
            st.info("💡 Copy the Job ID above to download results later or share with others")
            
            st.session_state.current_job_id = job_id
            st.info("🔄 Processing will start automatically. Monitor progress below.")
            st.rerun()
        else:
            st.error("❌ Upload failed. Please try again.")
            
    except Exception as e:
        st.error(f"❌ Error processing file: {e}")

def process_large_dataset_chunked(df, uploaded_file, chunk_size):
    """Process large datasets in chunks with progress tracking"""
    total_rows = len(df)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    
    st.info(f"🚀 Starting chunked processing: {total_chunks} chunks of {chunk_size} companies each")
    
    # Create progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    all_results = []
    successful_chunks = 0
    failed_chunks = 0
    
    try:
        bucket_manager = get_bucket_manager('website_resolver')
        
        for chunk_idx in range(total_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, total_rows)
            
            # Update progress
            progress = (chunk_idx + 1) / total_chunks
            progress_bar.progress(progress)
            status_text.text(f"Processing chunk {chunk_idx + 1}/{total_chunks} (companies {start_idx + 1}-{end_idx})")
            
            # Extract chunk
            chunk_df = df.iloc[start_idx:end_idx].copy()
            
            # Create temporary file for chunk
            import io
            chunk_csv = chunk_df.to_csv(index=False)
            chunk_file = io.BytesIO(chunk_csv.encode('utf-8'))
            
            # Upload chunk
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            chunk_filename = f"chunk_{chunk_idx + 1}_{timestamp}_{uploaded_file.name}"
            
            try:
                job_id = bucket_manager.upload_input_file(chunk_file.getvalue(), chunk_filename)
                
                if job_id:
                    # Wait for chunk to complete
                    chunk_completed = wait_for_chunk_completion(job_id, bucket_manager, chunk_idx + 1, total_chunks)
                    
                    if chunk_completed:
                        # Download chunk results
                        chunk_results = bucket_manager.download_results(job_id)
                        if chunk_results:
                            chunk_df_results = pd.read_csv(pd.io.common.BytesIO(chunk_results))
                            all_results.append(chunk_df_results)
                            successful_chunks += 1
                        else:
                            st.warning(f"⚠️ Chunk {chunk_idx + 1} completed but no results found")
                            failed_chunks += 1
                    else:
                        st.warning(f"⚠️ Chunk {chunk_idx + 1} failed or timed out")
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
            
            # Save combined results
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            combined_filename = f"combined_results_{timestamp}_{uploaded_file.name}"
            
            # Upload combined results
            combined_csv = combined_results.to_csv(index=False)
            combined_job_id = bucket_manager.upload_input_file(
                combined_csv.encode('utf-8'), 
                combined_filename
            )
            
            if combined_job_id:
                st.success(f"✅ Chunked processing completed! {successful_chunks}/{total_chunks} chunks successful")
                
                # Display job ID prominently for copying
                st.markdown("---")
                st.subheader("📋 Job ID")
                st.code(combined_job_id, language="text")
                st.info("💡 Copy the Job ID above to download results later or share with others")
                
                st.session_state.current_job_id = combined_job_id
                st.rerun()
            else:
                st.error("❌ Failed to save combined results")
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
            current_status = status.get('status', 'unknown')
            
            if current_status == 'completed':
                return True
            elif current_status in ['failed', 'timeout']:
                return False
            elif current_status in ['pending', 'processing']:
                # Still processing, wait a bit more
                time.sleep(5)
            else:
                # Unknown status, wait a bit more
                time.sleep(5)
                
        except Exception as e:
            st.warning(f"⚠️ Error checking chunk {chunk_num} status: {e}")
            time.sleep(5)
    
    # Timeout reached
    st.warning(f"⚠️ Chunk {chunk_num} timed out after {max_wait_time} seconds")
    return False

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
