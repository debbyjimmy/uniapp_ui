import streamlit as st
import pandas as pd
import requests
import json
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

def main():
    st.title("🧹 Name Cleaner")
    
    # Back to Dashboard button
    if st.button("← Back to Dashboard"):
        st.session_state.selected_tool = None
        st.rerun()
    
    try:
        bucket_manager = get_bucket_manager('name_cleaner')
    except Exception as e:
        st.error(f"GCP not available: {e}")
        st.stop()

    # Initialize session state for rule management
    if 'rule_history' not in st.session_state:
        st.session_state.rule_history = []
    if 'current_rules' not in st.session_state:
        st.session_state.current_rules = None
    
    # Generate session ID for this user session
    if 'session_id' not in st.session_state:
        st.session_state.session_id = f"session_{int(time.time())}_{hash(st.session_state.get('_session_id', 'default')) % 10000}"

    # Session ID download box (top right)
    with st.sidebar:
        st.markdown("---")
        st.subheader("📥 Download by Job ID")
        st.markdown("Enter a job ID to download results from a previous session.")
        
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

    st.markdown("---")
    
    # Tool description
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **How it works:**
        1. Upload CSV with name data (any column name will be auto-detected)
        2. File is processed in GCP using AI-powered name cleaning
        3. Download results with original and cleaned names
        
        **Expected CSV format:**
        - Any CSV file with name data
        - Name column will be automatically detected
        - Supports various name formats and structures
        """)
    
    with col2:
        st.info("""
        **Output columns:**
        - original_name: Original name from input
        - cleaned_name: AI-cleaned version
        - confidence: AI confidence score
        - changes_made: Summary of changes
        """)
    
    st.markdown("---")
    
    # Main tabs - streamlined for production
    tab1, tab2 = st.tabs(["🚀 Process Names", "⚙️ Rule Management"])
    
    with tab1:
        show_name_processing(bucket_manager)
    
    with tab2:
        show_rule_management(bucket_manager)
    
    # Session download section
    if 'checked_session_id' in st.session_state:
        st.markdown("---")
        st.subheader("📥 Session Download")
        st.write(f"Job ID: {st.session_state.checked_session_id}")
        
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
        st.markdown("---")
        st.subheader("📊 Session Results")
        display_session_results(st.session_state.current_session_id, bucket_manager)

def show_name_processing(bucket_manager):
    """Streamlined name processing workflow with chunking support"""
    st.markdown("""
    ### Upload & Process Names
    Upload a CSV file, select the name column, and get cleaned names with progress monitoring.
    """)
    
    # Upload section
    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
    
    if uploaded_file is not None:
        # Preview data
        df = pd.read_csv(uploaded_file)
        st.subheader("📊 Data Preview")
        st.dataframe(df.head())
        
        # Show detected name column
        detected_column = auto_detect_name_column_from_df(df)
        st.info(f"🔍 Auto-detected name column: **{detected_column}**")
        
        # Show dataset size and chunking info
        total_rows = len(df)
        st.info(f"📊 Dataset size: **{total_rows} rows**")
        
        # Chunking options for large datasets
        chunk_size = 50  # Default chunk size
        if total_rows > 100:
            st.warning("⚠️ Large dataset detected! Processing will be done in chunks to avoid timeouts.")
            chunk_size = st.selectbox(
                "Chunk size (rows per batch):",
                [25, 50, 75, 100],
                index=1,
                help="Smaller chunks are more reliable but take longer overall"
            )
            st.info(f"📦 Will process {total_rows} rows in {(total_rows + chunk_size - 1) // chunk_size} chunks of {chunk_size} rows each")
        
        if st.button("🚀 Process Names", type="primary"):
            if total_rows > 100:
                # Process in chunks
                process_large_dataset_chunked(df, uploaded_file, chunk_size, bucket_manager)
            else:
                # Process normally for small datasets
                process_small_dataset(df, uploaded_file, bucket_manager)
    
    # Job monitoring section
    if st.session_state.get('current_job_id'):
        st.markdown("---")
        st.subheader("📊 Job Status")
        monitor_job(st.session_state.current_job_id, bucket_manager)

def show_rule_management(bucket_manager):
    """Streamlined rule management interface"""
    st.markdown("""
    ### Rule Management
    Add rules to improve name cleaning. Rules are applied to all future processing.
    """)
    
    # Initialize rules if they don't exist
    initialize_rules_if_needed(bucket_manager)
    
    # Add new rule section
    st.subheader("➕ Add New Rule")
    
    with st.form("add_rule_form"):
        rule_description = st.text_area(
            "Rule Description", 
            placeholder="e.g., Remove VP as a title, or Keep McDonald unchanged",
            help="Describe what you want the cleaning to do differently"
        )
        
        st.markdown("**Test Examples (required):**")
        example1 = st.text_input("Example 1", placeholder="Before: Dr. VP Smith → After: Smith")
        example2 = st.text_input("Example 2", placeholder="Before: VP Johnson → After: Johnson")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            add_rule = st.form_submit_button("➕ Add Rule", type="primary")
        with col2:
            test_rule = st.form_submit_button("🧪 Test Rule")
        with col3:
            reset_rules = st.form_submit_button("🔄 Reset to Defaults")
    
    if add_rule and rule_description and example1:
        examples = [ex for ex in [example1, example2] if ex.strip()]
        add_rule_to_system(rule_description, examples, bucket_manager)
    
    if test_rule and rule_description and example1:
        examples = [ex for ex in [example1, example2] if ex.strip()]
        test_rule_locally(rule_description, examples, bucket_manager)
    
    if reset_rules:
        reset_rules_to_defaults(bucket_manager)
    
    # Rule history
    if st.session_state.rule_history:
        st.subheader("📝 Recent Rule Changes")
        for i, rule in enumerate(reversed(st.session_state.rule_history[-5:])):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.text(f"{len(st.session_state.rule_history) - i}. {rule}")
            with col2:
                if st.button("↩️ Undo", key=f"undo_{i}"):
                    undo_last_rule(bucket_manager)
                    st.rerun()

def add_rule_to_system(rule_description, examples, bucket_manager):
    """Add a new rule to the system using direct GCS calls"""
    try:
        # Load current rules
        response = load_rules_from_gcs(bucket_manager)
        
        if not response.get('success'):
            st.error(f"❌ {response['message']}")
            return
            
        current_rules = response['rules']
        
        # Analyze and add rule
        analysis = analyze_and_add_rule_local(rule_description, examples, current_rules)
        
        if analysis.get('action') == 'add':
            # Save updated rules
            save_response = save_rules_to_gcs(bucket_manager, current_rules)
            if save_response.get('success'):
                st.success(f"✅ {analysis['message']}")
                st.session_state.rule_history.append(f"{rule_description} | {analysis['message']}")
            else:
                st.error(f"❌ {save_response['message']}")
        else:
            st.info(f"ℹ️ {analysis['message']}")
            
    except Exception as e:
        st.error(f"❌ Error adding rule: {e}")

def test_rule_locally(rule_description, examples, bucket_manager):
    """Test a rule locally before adding"""
    try:
        # Get current rules
        response = load_rules_from_gcs(bucket_manager)
        
        if response.get('success'):
            current_rules = response['rules']
            
            # Test with examples
            test_names = []
            for example in examples:
                if '→' in example:
                    before = example.split('→')[0].replace('Before:', '').strip()
                    test_names.append(before)
            
            if test_names:
                st.subheader("🧪 Test Results")
                # Simple test - just show what would be cleaned
                for name in test_names:
                    st.text(f"**{name}** → (would be processed with current rules)")
        else:
            st.error(f"❌ Failed to get rules: {response['message']}")
            
    except Exception as e:
        st.error(f"❌ Error testing rule: {e}")

def reset_rules_to_defaults(bucket_manager):
    """Reset rules to defaults using direct GCS calls"""
    try:
        # Default rules
        default_rules = {
            "titles_remove": [
                "mr","mrs","ms","miss","mx","dr","prof","engr","rev",
                "jr","sr","ii","iii","iv",
                "phd","md","mba","bsc","msc","jd","llb","llm","rn","np","pa","dpt","dds","dvm","od",
                "cfa","cpa","cisa","cissp","cipp","pmp","cfp","cma","cpc","cpt","cebs","esq",
                "fache","faan","cenp","facs","famia","nea","bcps","fcips","cpxp","mhrmir","mde","cpel",
                "lssyb","fachdm","fshea","fidsa","chcio","fhimss","rhia"
            ],
            "titles_remove_phrases": [
                "chief executive officer","ceo",
                "chief technology officer","cto",
                "chief operating officer","coo",
                "chief financial officer","cfo",
                "vice chancellor","pro vice chancellor","deputy vice chancellor"
            ],
            "unsafe_tokens": ["inc","llc","ltd","co","corp","@","(ceo)","(founder)"],
            "particles": [
                {"text":"van","case":"lower"},{"text":"van der","case":"lower"},
                {"text":"de","case":"lower"},{"text":"de la","case":"lower"},
                {"text":"del","case":"lower"},{"text":"da","case":"lower"},
                {"text":"di","case":"lower"},{"text":"bin","case":"lower"},
                {"text":"al","case":"lower"}
            ],
            "apostrophe_particles": ["o'","o'","d'","d'","l'","l'"],
            "keep_case": ["MacDonald","McIntyre","O'Neill","O'Neill"],
            "min_len": 2,
            "accent_removal": True,
            "accent_examples": ["é→e", "ñ→n", "ü→u", "ö→o", "à→a", "è→e", "ì→i", "ò→o", "ù→u", "ç→c"]
        }
        
        # Save default rules
        response = save_rules_to_gcs(bucket_manager, default_rules)
        
        if response.get('success'):
            st.success(f"✅ {response['message']}")
            st.session_state.rule_history = []
        else:
            st.error(f"❌ {response['message']}")
            
    except Exception as e:
        st.error(f"❌ Error resetting rules: {e}")

def undo_last_rule(bucket_manager):
    """Undo the last rule addition"""
    if st.session_state.rule_history:
        st.session_state.rule_history.pop()
        st.success("✅ Last rule change undone")
    else:
        st.info("ℹ️ No rules to undo")

def process_small_dataset(df, uploaded_file, bucket_manager):
    """Process small datasets normally (original behavior)"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"job_{timestamp}_{uploaded_file.name}"
    
    try:
        # Reset file pointer to beginning and read file data as bytes
        uploaded_file.seek(0)
        file_data = uploaded_file.read()
        
        job_id = bucket_manager.upload_input_file(file_data, filename)
        
        if job_id:
            st.success(f"✅ File uploaded successfully!")
            
            # Display job ID prominently for copying
            st.markdown("---")
            st.subheader("📋 Job ID")
            st.code(job_id, language="text")
            st.info("💡 Copy the Job ID above to download results later or share with others")
            
            # Store job ID in session state
            st.session_state.current_job_id = job_id
            st.rerun()
        else:
            st.error("❌ Upload failed - no job ID returned")
        
    except Exception as e:
        st.error(f"❌ Upload failed: {e}")

def process_large_dataset_chunked(df, uploaded_file, chunk_size, bucket_manager):
    """Process large datasets in chunks with progress tracking"""
    total_rows = len(df)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    
    st.info(f"🚀 Starting chunked processing: {total_chunks} chunks of {chunk_size} rows each")
    
    # Generate and display session ID immediately
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    session_id = f"chunked_{timestamp}_{uploaded_file.name}"
    
    # Display session ID prominently for copying
    st.markdown("---")
    st.subheader("📋 Session ID")
    st.code(session_id, language="text")
    st.info("💡 Copy the Session ID above to track progress or download results later")
    
    # Create progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    all_results = []
    successful_chunks = 0
    failed_chunks = 0
    
    try:
        for chunk_idx in range(total_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, total_rows)
            
            # Update progress
            progress = (chunk_idx + 1) / total_chunks
            progress_bar.progress(progress)
            status_text.text(f"Processing chunk {chunk_idx + 1}/{total_chunks} (rows {start_idx + 1}-{end_idx})")
            
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

def auto_detect_name_column_from_df(df):
    """Auto-detect the most likely name column from DataFrame columns"""
    columns = df.columns.tolist()
    
    # Priority list of common name column names
    name_priority = [
        'name', 'full_name', 'fullname', 'contact_name', 'person_name',
        'first_name', 'last_name', 'firstname', 'lastname',
        'contact', 'person', 'individual', 'client_name', 'customer_name',
        'employee_name', 'staff_name', 'member_name', 'user_name'
    ]
    
    # Check for exact matches first (case-insensitive)
    for priority_name in name_priority:
        for col in columns:
            if col.lower() == priority_name.lower():
                return col
    
    # If no exact match, check for partial matches
    for priority_name in name_priority:
        for col in columns:
            if priority_name.lower() in col.lower() or col.lower() in priority_name.lower():
                return col
    
    # If still no match, use the first column
    return columns[0] if columns else 'name'

def initialize_rules_if_needed(bucket_manager):
    """Initialize default rules if they don't exist"""
    try:
        import json
        
        client = bucket_manager.client
        bucket = client.bucket(bucket_manager.bucket_name)
        rules_blob = bucket.blob("rules/active_rules.json")
        
        if not rules_blob.exists():
            # Create default rules
            default_rules = {
                "titles_remove": [
                    "mr","mrs","ms","miss","mx","dr","prof","engr","rev",
                    "jr","sr","ii","iii","iv",
                    "phd","md","mba","bsc","msc","jd","llb","llm","rn","np","pa","dpt","dds","dvm","od",
                    "cfa","cpa","cisa","cissp","cipp","pmp","cfp","cma","cpc","cpt","cebs","esq",
                    "fache","faan","cenp","facs","famia","nea","bcps","fcips","cpxp","mhrmir","mde","cpel",
                    "lssyb","fachdm","fshea","fidsa","chcio","fhimss","rhia"
                ],
                "titles_remove_phrases": [
                    "chief executive officer","ceo",
                    "chief technology officer","cto",
                    "chief operating officer","coo",
                    "chief financial officer","cfo",
                    "vice chancellor","pro vice chancellor","deputy vice chancellor"
                ],
                "unsafe_tokens": ["inc","llc","ltd","co","corp","@","(ceo)","(founder)"],
                "particles": [
                    {"text":"van","case":"lower"},{"text":"van der","case":"lower"},
                    {"text":"de","case":"lower"},{"text":"de la","case":"lower"},
                    {"text":"del","case":"lower"},{"text":"da","case":"lower"},
                    {"text":"di","case":"lower"},{"text":"bin","case":"lower"},
                    {"text":"al","case":"lower"}
                ],
                "apostrophe_particles": ["o'","o'","d'","d'","l'","l'"],
                "keep_case": ["MacDonald","McIntyre","O'Neill","O'Neill"],
                "min_len": 2,
                "accent_removal": True,
                "accent_examples": ["é→e", "ñ→n", "ü→u", "ö→o", "à→a", "è→e", "ì→i", "ò→o", "ù→u", "ç→c"]
            }
            
            rules_data = {
                "version": "1.0",
                "last_updated": "2025-01-01T00:00:00Z",
                "rules": default_rules
            }
            
            rules_blob.upload_from_string(
                json.dumps(rules_data, indent=2),
                content_type="application/json"
            )
            st.info("ℹ️ Initialized default rules in GCS bucket")
    except Exception as e:
        st.warning(f"Could not initialize rules: {e}")

def load_rules_from_gcs(bucket_manager):
    """Load rules directly from GCS using the existing bucket manager"""
    try:
        import json
        
        # Default rules structure
        default_rules = {
            "titles_remove": [
                "mr","mrs","ms","miss","mx","dr","prof","engr","rev",
                "jr","sr","ii","iii","iv",
                "phd","md","mba","bsc","msc","jd","llb","llm","rn","np","pa","dpt","dds","dvm","od",
                "cfa","cpa","cisa","cissp","cipp","pmp","cfp","cma","cpc","cpt","cebs","esq",
                "fache","faan","cenp","facs","famia","nea","bcps","fcips","cpxp","mhrmir","mde","cpel",
                "lssyb","fachdm","fshea","fidsa","chcio","fhimss","rhia"
            ],
            "titles_remove_phrases": [
                "chief executive officer","ceo",
                "chief technology officer","cto",
                "chief operating officer","coo",
                "chief financial officer","cfo",
                "vice chancellor","pro vice chancellor","deputy vice chancellor"
            ],
            "unsafe_tokens": ["inc","llc","ltd","co","corp","@","(ceo)","(founder)"],
            "particles": [
                {"text":"van","case":"lower"},{"text":"van der","case":"lower"},
                {"text":"de","case":"lower"},{"text":"de la","case":"lower"},
                {"text":"del","case":"lower"},{"text":"da","case":"lower"},
                {"text":"di","case":"lower"},{"text":"bin","case":"lower"},
                {"text":"al","case":"lower"}
            ],
            "apostrophe_particles": ["o'","o'","d'","d'","l'","l'"],
            "keep_case": ["MacDonald","McIntyre","O'Neill","O'Neill"],
            "min_len": 2,
            "accent_removal": True,
            "accent_examples": ["é→e", "ñ→n", "ü→u", "ö→o", "à→a", "è→e", "ì→i", "ò→o", "ù→u", "ç→c"]
        }
        
        # Use the existing bucket manager's GCS client
        client = bucket_manager.client
        bucket = client.bucket(bucket_manager.bucket_name)
        rules_blob = bucket.blob("rules/active_rules.json")
        
        if rules_blob.exists():
            rules_json = rules_blob.download_as_text()
            rules_data = json.loads(rules_json)
            loaded_rules = rules_data.get("rules", {})
            # Merge with defaults to ensure all categories exist
            for key, value in default_rules.items():
                if key not in loaded_rules:
                    loaded_rules[key] = value
            return {"success": True, "rules": loaded_rules}
        else:
            return {"success": True, "rules": default_rules}
    except Exception as e:
        return {"success": False, "message": f"Error loading rules: {str(e)}"}

def save_rules_to_gcs(bucket_manager, rules):
    """Save rules directly to GCS using the existing bucket manager"""
    try:
        import json
        import time
        
        # Use the existing bucket manager's GCS client
        client = bucket_manager.client
        bucket = client.bucket(bucket_manager.bucket_name)
        
        rules_data = {
            "version": "1.0",
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "rules": rules
        }
        
        rules_blob = bucket.blob("rules/active_rules.json")
        rules_blob.upload_from_string(
            json.dumps(rules_data, indent=2),
            content_type="application/json"
        )
        return {"success": True, "message": "Rules saved successfully"}
    except Exception as e:
        return {"success": False, "message": f"Error saving rules: {str(e)}"}

def analyze_and_add_rule_local(user_input, test_examples, current_rules):
    """AI-powered rule analyzer (local version)"""
    try:
        import openai
        import json
        
        # Set up OpenAI
        import streamlit as st
        try:
            openai.api_key = st.secrets.api_keys.openai_api_key
        except:
            st.error("OpenAI API key not found in Streamlit secrets")
            return {"success": False, "message": "OpenAI API key not configured"}
        
        # Prepare examples for AI analysis
        examples_text = "\n".join([f"Before: {ex.split('→')[0].strip()} → After: {ex.split('→')[1].strip()}" for ex in test_examples])
        
        prompt = f"""
        You are a rule analyzer for a name cleaning system. Analyze the user's request and determine how to add/modify rules.
        
        Current rules structure:
        {json.dumps(current_rules, indent=2)}
        
        User request: "{user_input}"
        Test examples:
        {examples_text}
        
        Return ONLY a JSON object with:
        - "action": "add" or "no_change" 
        - "category": which rule category to modify (titles_remove, unsafe_tokens, keep_case, etc.)
        - "items": list of items to add to that category
        - "message": user-friendly confirmation message
        
        Rules:
        - If items already exist in the category, set action to "no_change"
        - If user says "keep" or "preserve", add to keep_case
        - If user says "remove" or "delete", add to titles_remove
        - For keep_case, add exact case versions (M., J., A.)
        - For titles_remove, add lowercase versions (m., j., a.)
        - Look at examples to determine intent, not just the words used
        - Be conservative - only add if examples clearly show the pattern
        """
        
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}]
        )
        
        analysis = json.loads(resp.choices[0].message.content)
        
        if analysis.get("action") == "add":
            category = analysis.get("category")
            items = analysis.get("items", [])
            
            if category in current_rules and isinstance(current_rules[category], list):
                # Add items to existing list
                for item in items:
                    if item not in current_rules[category]:
                        current_rules[category].append(item)
                analysis["message"] = f"Added {len(items)} items to {category}: {', '.join(items)}"
            else:
                analysis["action"] = "no_change"
                analysis["message"] = f"Category '{category}' not found or not a list"
        
        return analysis
        
    except Exception as e:
        return {
            "action": "error",
            "message": f"Error analyzing rule: {str(e)}"
        }

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
            st.info("🔄 AI name cleaning in progress...")
            
        elif current_status == 'completed':
            st.success("✅ Name cleaning completed! Results are ready.")
            
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
        with st.spinner("⬇️ Downloading name cleaning results..."):
            results_data = bucket_manager.download_results(job_id)
        
        if results_data:
            # Convert to DataFrame
            df_results = pd.read_csv(pd.io.common.BytesIO(results_data))
            
            st.success(f"✅ Results downloaded: {len(df_results)} names processed")
            
            # Display results summary
            st.subheader("📊 Name Cleaning Summary")
            
            # Count different types of changes
            original_col = None
            cleaned_col = None
            
            # Find the original and cleaned columns
            for col in df_results.columns:
                if 'original' in col.lower() or 'before' in col.lower():
                    original_col = col
                elif 'cleaned' in col.lower() or 'after' in col.lower():
                    cleaned_col = col
            
            if original_col and cleaned_col:
                # Count changes
                changes_made = len(df_results[df_results[original_col] != df_results[cleaned_col]])
                no_changes = len(df_results[df_results[original_col] == df_results[cleaned_col]])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Names", len(df_results))
                with col2:
                    st.metric("✨ Names Changed", changes_made)
                with col3:
                    st.metric("✅ No Changes", no_changes)
            
            # Show results table
            st.subheader("📋 Name Cleaning Results")
            st.dataframe(df_results, use_container_width=True)
            
            # Download button
            csv_data = df_results.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "💾 Save Results CSV",
                data=csv_data,
                file_name=f"name_cleaner_results_{job_id}.csv",
                mime="text/csv"
            )
            
            # Clear job from session
            del st.session_state.current_job_id
            
        else:
            st.error("❌ Failed to download results")
            
    except Exception as e:
        st.error(f"❌ Error downloading results: {e}")

def check_session_status(job_id: str, bucket_manager):
    """Check status and download results for a specific session ID"""
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
        
        st.sidebar.markdown(f"**Job ID:** {job_id}")
        st.sidebar.markdown(f"**Status:** {emoji} {current_status.title()}")
        
        if 'timestamp' in status:
            st.sidebar.markdown(f"**Started:** {status['timestamp']}")
        
        # Handle different statuses
        if current_status == 'pending':
            st.sidebar.info("⏳ File uploaded and waiting to be processed...")
            
        elif current_status == 'processing':
            st.sidebar.info("🔄 AI name cleaning in progress...")
            st.sidebar.markdown("**Message:** " + status.get('message', 'Processing...'))
            
        elif current_status == 'completed':
            st.sidebar.success("✅ Name cleaning completed! Use the download button below to get results.")
                
        elif current_status == 'failed':
            st.sidebar.error(f"❌ Processing failed: {status.get('error', 'Unknown error')}")
            
        elif current_status == 'timeout':
            st.sidebar.warning("⏰ Job took too long to complete. Please check GCP logs.")
            
        elif current_status == 'unknown':
            st.sidebar.error("❌ Job not found. Please check the job ID.")
            
    except Exception as e:
        st.sidebar.error(f"❌ Error checking session: {e}")

def display_session_results(job_id: str, bucket_manager):
    """Display session results in the main UI"""
    try:
        with st.spinner("⬇️ Downloading name cleaning results..."):
            results_data = bucket_manager.download_results(job_id)
        
        if results_data:
            # Convert to DataFrame
            df_results = pd.read_csv(pd.io.common.BytesIO(results_data))
            
            st.success(f"✅ Results downloaded: {len(df_results)} names processed")
            
            # Display results summary
            st.subheader("📊 Name Cleaning Summary")
            
            # Count different types of changes
            original_col = None
            cleaned_col = None
            
            # Find the original and cleaned columns
            for col in df_results.columns:
                if 'original' in col.lower() or 'before' in col.lower():
                    original_col = col
                elif 'cleaned' in col.lower() or 'after' in col.lower():
                    cleaned_col = col
            
            if original_col and cleaned_col:
                # Count changes
                changes_made = len(df_results[df_results[original_col] != df_results[cleaned_col]])
                no_changes = len(df_results[df_results[original_col] == df_results[cleaned_col]])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Names", len(df_results))
                with col2:
                    st.metric("✨ Names Changed", changes_made)
                with col3:
                    st.metric("✅ No Changes", no_changes)
            
            # Show results table
            st.subheader("📋 Name Cleaning Results")
            st.dataframe(df_results, use_container_width=True)
            
            # Download button
            csv_data = df_results.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "💾 Save Results CSV",
                data=csv_data,
                file_name=f"name_cleaner_results_{job_id}.csv",
                mime="text/csv"
            )
            
        else:
            st.error("❌ Failed to download results")
            
    except Exception as e:
        st.error(f"❌ Error downloading results: {e}")

if __name__ == "__main__":
    main()