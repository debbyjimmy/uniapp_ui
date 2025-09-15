import os
import time
import json
from datetime import datetime
from typing import Dict, List, Optional
import streamlit as st

# Try to import GCP libraries
try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False
    st.warning("⚠️ Google Cloud libraries not available. Install with: pip install google-cloud-storage")

import os
import sys

# Add the shared directory to the path
shared_dir = os.path.dirname(__file__)
if shared_dir not in sys.path:
    sys.path.insert(0, shared_dir)

from config import TOOLS_CONFIG, POLL_INTERVAL, MAX_WAIT_TIME

class GCPBucketManager:
    """Manages GCP bucket operations for tools"""
    
    def __init__(self, tool_id: str):
        self.tool_id = tool_id
        self.tool_config = TOOLS_CONFIG.get(tool_id)
        
        if not self.tool_config:
            raise ValueError(f"Unknown tool: {tool_id}")
        
        self.bucket_name = self.tool_config['bucket']
        self.input_folder = self.tool_config['input_folder']
        self.results_folder = self.tool_config['results_folder']
        self.status_folder = self.tool_config['status_folder']
        
        # Initialize GCP client if available
        if GCP_AVAILABLE:
            try:
                # Optimized for Streamlit Cloud - try secrets first, then fallback
                if hasattr(st.secrets, 'GCP_CREDENTIALS') and st.secrets.GCP_CREDENTIALS:
                    import json
                    credentials_info = json.loads(st.secrets.GCP_CREDENTIALS)
                    credentials = service_account.Credentials.from_service_account_info(credentials_info)
                    self.client = storage.Client(credentials=credentials, project=credentials_info.get("project_id"))
                    if st.session_state.get('show_gcp_status', True):
                        st.success("✅ Connected to GCP using Streamlit secrets!")
                else:
                    # Fallback to default service account (works in Streamlit Cloud)
                    self.client = storage.Client()
                    if st.session_state.get('show_gcp_status', True):
                        st.success("✅ Connected to GCP using default service account!")
            except Exception as e:
                st.error(f"⚠️ GCP connection failed: {str(e)}")
                st.info("Please add GCP_CREDENTIALS to Streamlit secrets for better reliability")
                self.client = None
                self.bucket = None
                return
            
            # Initialize bucket if client is available
            if self.client:
                self.bucket = self.client.bucket(self.bucket_name)
        else:
            self.client = None
            self.bucket = None
    
    def generate_job_id(self) -> str:
        """Generate unique job ID with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"job_{timestamp}"
    
    def upload_input_file(self, file_data: bytes, filename: str) -> str:
        """Upload input file to GCP bucket"""
        if not self.client:
            st.error("GCP not available")
            return ""
        
        job_id = self.generate_job_id()
        blob_name = f"{self.input_folder}/{job_id}_{filename}"
        
        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(file_data)
            
            # Create status file
            self._create_status_file(job_id, "pending")
            
            return job_id
        except Exception as e:
            st.error(f"Upload failed: {e}")
            return ""
    
    def _create_status_file(self, job_id: str, status: str):
        """Create or update status file"""
        if not self.client:
            return
        
        status_data = {
            "job_id": job_id,
            "tool": self.tool_id,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        blob_name = f"{self.status_folder}/{job_id}_status.json"
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(status_data, indent=2))
    
    def check_job_status(self, job_id: str) -> Dict:
        """Check job status from GCP bucket"""
        if not self.client:
            return {"status": "unknown", "error": "GCP not available"}
        
        try:
            # Check status file
            status_blob_name = f"{self.status_folder}/{job_id}_status.json"
            status_blob = self.bucket.blob(status_blob_name)
            
            if not status_blob.exists():
                return {"status": "not_found"}
            
            status_data = json.loads(status_blob.download_as_text())
            
            # Check if results are ready
            if status_data.get("status") == "completed":
                results_blob_name = f"{self.results_folder}/{job_id}_results.csv"
                results_blob = self.bucket.blob(results_blob_name)
                
                if results_blob.exists():
                    status_data["results_ready"] = True
                    status_data["results_url"] = results_blob.self_link
                else:
                    status_data["results_ready"] = False
            
            return status_data
            
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def get_status(self, job_id: str) -> Dict:
        """Alias for check_job_status for backward compatibility"""
        return self.check_job_status(job_id)
    
    def download_results(self, job_id: str) -> Optional[bytes]:
        """Download results file from GCP bucket"""
        if not self.client:
            st.error("GCP not available")
            return None
        
        try:
            results_blob_name = f"{self.results_folder}/{job_id}_results.csv"
            blob = self.bucket.blob(results_blob_name)
            
            if not blob.exists():
                st.warning(f"Results file not found: {results_blob_name}")
                return None
            
            return blob.download_as_bytes()
            
        except Exception as e:
            st.error(f"Download failed: {e}")
            return None
    
    def download_results_as_dataframe(self, job_id: str):
        """Download results file and return as pandas DataFrame"""
        try:
            import pandas as pd
            
            results_bytes = self.download_results(job_id)
            if results_bytes is None:
                return None
            
            # Convert bytes to DataFrame
            import io
            csv_data = io.StringIO(results_bytes.decode('utf-8'))
            df = pd.read_csv(csv_data)
            return df
            
        except Exception as e:
            st.error(f"Failed to convert results to DataFrame: {e}")
            return None
    
    def monitor_job_progress(self, job_id: str) -> Dict:
        """Monitor job progress with polling"""
        start_time = time.time()
        
        while time.time() - start_time < MAX_WAIT_TIME:
            status = self.check_job_status(job_id)
            
            if status.get("status") == "completed":
                return status
            elif status.get("status") == "failed":
                return status
            
            # Wait before next check
            time.sleep(POLL_INTERVAL)
        
        return {"status": "timeout", "error": "Job took too long to complete"}

def list_recent_jobs() -> List[Dict]:
    """List recent jobs across all tools (simple implementation)"""
    # This is a simplified version - in practice, you might want to scan all buckets
    # or maintain a central job registry
    return []

def get_bucket_manager(tool_id: str) -> GCPBucketManager:
    """Get bucket manager for specific tool"""
    return GCPBucketManager(tool_id)
