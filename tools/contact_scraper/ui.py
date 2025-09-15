import streamlit as st
import pandas as pd
import os
import json
import time
import uuid
import shutil
import zipfile
import sys

# Add the project root to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from shared.auth import require_login
from shared.gcp_utils import get_bucket_manager

# Page config
st.set_page_config(
    page_title="Contact Scraper - Automation Tools Hub",
    page_icon="💼",
    layout="wide"
)

def main():
    # Require login
    require_login()
    st.title("📇 Contact Scraper")

    # Back to dashboard button
    if st.button("← Back to Dashboard"):
        st.session_state.selected_tool = None
        st.rerun()

    # --- GCS Client Setup ---
    try:
        # Use unified app's bucket configuration
        bucket_manager = get_bucket_manager('contact_scraper')
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
                check_session_results(lookup_id, bucket)
            else:
                st.warning("Please enter a session ID")

    st.markdown("---")
    
    # Tool description
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **How it works:**
        1. Upload LinkedIn CSV file
        2. File is split into chunks and processed in parallel
        3. Results are automatically merged and ready for download
        
        **Expected CSV format:**
        - LinkedIn export CSV with contact information
        - Will be automatically split into manageable chunks
        - Processing happens in parallel for faster results
        """)
    
    with col2:
        st.info("""
        **Output files:**
        - ALL_SUCCESS.csv: Successfully scraped contacts
        - ALL_FAILURES.csv: Failed scraping attempts
        - Processed in parallel chunks for speed
        """)

    st.markdown("---")
    
    # --- Upload + Split CSV ---
    uploaded_file = st.file_uploader("Upload full LinkedIn CSV to split and scrape", type=["csv"])
    num_chunks = 3

    run_id = st.session_state.get("run_id")

    if uploaded_file:
        input_df = pd.read_csv(uploaded_file)
        st.write(f"✅ Dataframe loaded: {len(input_df)} rows")

        rows_per_chunk = -(-len(input_df) // num_chunks)
        est_time_per_chunk_min = rows_per_chunk / 200
        st.info(f"⏱️ Estimated processing time: ~{int(est_time_per_chunk_min)} minutes per chunk")

        if st.button("Split & Upload"):
            # Generate session UUID at this point only
            run_id = str(uuid.uuid4())[:8]
            st.session_state["run_id"] = run_id
            st.markdown(f"**Session ID:** `{run_id}`")

            st.info("🧹 Clearing previous session files...")
            for prefix in [f"users/{run_id}/chunks/", f"users/{run_id}/results/"]:
                blobs = list(bucket.list_blobs(prefix=prefix))
                for blob in blobs:
                    blob.delete()

            st.info("📤 Splitting CSV and uploading new chunks...")
            os.makedirs("chunks", exist_ok=True)
            for i in range(num_chunks):
                start = i * rows_per_chunk
                end = min((i + 1) * rows_per_chunk, len(input_df))
                chunk_df = input_df.iloc[start:end]
                if not chunk_df.empty:
                    filename = f"chunk_{i + 1}.csv"
                    path = os.path.join("chunks", filename)
                    chunk_df.to_csv(path, index=False)
                    blob = bucket.blob(f"users/{run_id}/chunks/{filename}")
                    blob.upload_from_filename(path)
                    st.success(f"✅ Uploaded chunk: {filename} ({len(chunk_df)} rows)")
                    os.remove(path)
            shutil.rmtree("chunks", ignore_errors=True)
            st.balloons()
            st.success("🚀 All chunks uploaded. Scraping will start automatically.")

    # --- Progress Monitoring ---
    if run_id:
        st.header("📊 Scraping Progress")
        progress_placeholder = st.empty()
        status_text = st.empty()

        def fetch_central_progress():
            blob = bucket.blob("progress.jsonl")
            if not blob.exists():
                return []
            try:
                # Read directly from blob without downloading to local file
                raw_lines = blob.download_as_text()
                json_str = f"[{raw_lines.replace('}\n{', '},{')}]"
                return json.loads(json_str)
            except Exception as e:
                st.warning(f"Error reading progress log: {e}")
                return []

        def filter_records_by_run_id(records, run_id):
            return [r for r in records if r.get("run_id") == run_id and r.get("status") == "completed"]

        completed_chunks = 0
        attempt = 0
        while True:
            all_records = fetch_central_progress()
            session_records = filter_records_by_run_id(all_records, run_id)

            seen_chunks = set()
            for record in session_records:
                if isinstance(record.get("chunk_index"), int):
                    seen_chunks.add(record["chunk_index"])

            completed_chunks = len(seen_chunks)
            progress = int((completed_chunks / num_chunks) * 100)
            progress_placeholder.progress(progress, text=f"{completed_chunks}/{num_chunks} chunks completed")

            if completed_chunks >= num_chunks:
                status_text.success("✅ All chunks completed.")
                break

            attempt += 1
            status_text.info(f"⏳ Waiting... (Attempt {attempt})")
            time.sleep(5)

        # --- Merge Results ---
        def extract_zip_to_tmp(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall("/tmp")

        def download_and_extract_zip_files(prefix):
            blobs = list(bucket.list_blobs(prefix=prefix))
            for blob in blobs:
                if blob.name.endswith(".zip") and "scrape_results_" in blob.name:
                    local_path = f"/tmp/{os.path.basename(blob.name)}"
                    blob.download_to_filename(local_path)
                    extract_zip_to_tmp(local_path)

        def merge_csvs(pattern):
            files = [os.path.join("/tmp", f) for f in os.listdir("/tmp") if f.endswith(".csv") and pattern in f]
            return pd.concat([pd.read_csv(f) for f in files], ignore_index=True) if files else pd.DataFrame()

        def upload_to_bucket(local_path, dest_name):
            blob = bucket.blob(dest_name)
            blob.upload_from_filename(local_path)

        merge_success = False
        st.info("🔀 Merging results...")
        download_and_extract_zip_files(f"users/{run_id}/results/")
        success_df = merge_csvs("result_")
        failure_df = merge_csvs("failures_")

        if not success_df.empty:
            success_path = "/tmp/ALL_SUCCESS.csv"
            success_df.to_csv(success_path, index=False)
            upload_to_bucket(success_path, f"users/{run_id}/results/ALL_SUCCESS.csv")

        if not failure_df.empty:
            failure_path = "/tmp/ALL_FAILURES.csv"
            failure_df.to_csv(failure_path, index=False)
            upload_to_bucket(failure_path, f"users/{run_id}/results/ALL_FAILURES.csv")

        merge_success = True

        # --- Download Buttons ---
        if merge_success:
            st.success("🎉 Merge completed. You can now download your results:")
            for fname in ["ALL_SUCCESS.csv", "ALL_FAILURES.csv"]:
                blob = bucket.blob(f"users/{run_id}/results/{fname}")
                local_path = f"/tmp/{fname}"
                if blob.exists():
                    blob.download_to_filename(local_path)
                    with open(local_path, "rb") as f:
                        st.download_button(f"⬇️ Download {fname}", f, file_name=fname)

def check_session_results(lookup_id, bucket):
    """Check and display results for a specific session ID"""
    try:
        result_prefix = f"users/{lookup_id}/results/"
        zip_blobs = list(bucket.list_blobs(prefix=result_prefix))
        matching_zips = [blob for blob in zip_blobs if blob.name.endswith(".zip")]

        if matching_zips:
            st.sidebar.success(f"✅ {len(matching_zips)} result file(s) found.")
            for blob in matching_zips:
                filename = os.path.basename(blob.name)
                # Read directly from blob without downloading to local file
                file_data = blob.download_as_bytes()
                st.sidebar.download_button(f"⬇️ Download {filename}", file_data, file_name=filename)
        else:
            st.sidebar.info("⏳ No zipped result files found yet for this session.")
    except Exception as e:
        st.sidebar.error(f"Error checking session: {e}")

    st.markdown("---")
    st.caption("Powered by eCore Services.")


if __name__ == "__main__":
    main()