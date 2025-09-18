import streamlit as st
import os
from shared.auth import check_login
from shared.config import TOOLS_CONFIG

# Clear cache to ensure fresh config
st.cache_data.clear()

# Page config
st.set_page_config(
    page_title="Automation Tools Hub",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for beautiful styling
st.markdown("""
<style>
    /* Main theme colors */
    :root {
        --primary-color: #1f77b4;
        --secondary-color: #ff7f0e;
        --success-color: #2ca02c;
        --warning-color: #d62728;
        --info-color: #17a2b8;
        --light-bg: #f8f9fa;
        --dark-bg: #343a40;
        --border-color: #dee2e6;
    }
    
    /* Hide default Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Ensure text visibility */
    .main .block-container {
        color: #333;
    }
    
    .stMarkdown {
        color: #333;
    }
    
    .stText {
        color: #333;
    }
    
    /* Custom header */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem 1rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 700;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        font-size: 1.2rem;
        opacity: 0.9;
    }
    
    /* Tool cards */
    .tool-card {
        background: white;
        border-radius: 15px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        border: 1px solid var(--border-color);
        transition: all 0.3s ease;
        height: 100%;
    }
    
    .tool-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 30px rgba(0,0,0,0.15);
        border-color: var(--primary-color);
    }
    
    .tool-card h3 {
        color: var(--primary-color);
        margin-top: 0;
        font-size: 1.4rem;
        font-weight: 600;
    }
    
    .tool-card p {
        color: #333;
        margin-bottom: 1rem;
        line-height: 1.5;
        font-weight: 500;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, var(--primary-color) 0%, #5a67d8 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 2px 10px rgba(31, 119, 180, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(31, 119, 180, 0.4);
    }
    
    /* Login page styling */
    .login-container {
        background: white;
        border-radius: 15px;
        padding: 2rem;
        box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        border: 1px solid var(--border-color);
    }
    
    .login-container h3 {
        color: var(--primary-color);
        text-align: center;
        margin-bottom: 1.5rem;
        font-size: 1.8rem;
    }
    
    /* Activity section */
    .activity-item {
        background: var(--light-bg);
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 0.5rem;
        border-left: 4px solid var(--primary-color);
    }
    
    /* Status indicators */
    .status-success { color: var(--success-color); }
    .status-warning { color: var(--warning-color); }
    .status-info { color: var(--info-color); }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .main-header h1 { font-size: 2rem; }
        .main-header p { font-size: 1rem; }
        .tool-card { padding: 1rem; }
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Check if user is logged in
    if not check_login():
        show_login_page()
        return
    
    # Check if a tool is selected
    if st.session_state.get('selected_tool'):
        show_tool_page()
    else:
        # Main dashboard
        show_dashboard()

def show_login_page():
    # Beautiful header
    st.markdown("""
    <div class="main-header">
        <h1> Automation Tools Hub</h1>
        <p>Your Gateway to Team AI Powered Tools</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Login form with beautiful styling
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div class="login-container">
            <h3>🔐 Team Login</h3>
        </div>
        """, unsafe_allow_html=True)
        
        username = st.text_input("👤 Username", placeholder="Enter your username")
        password = st.text_input("🔒 Password", type="password", placeholder="Enter your password")
        
        if st.button("Login to Dashboard", type="primary", use_container_width=True):
            if check_login(username, password):
                st.session_state.logged_in = True
                st.session_state.username = username
                st.rerun()
            else:
                st.error("❌ Invalid credentials. Please try again.")
        
        st.markdown("""
        <div style="text-align: center; margin-top: 1rem; color: #666;">
            💡 Use your team's shared credentials to access the automation tools
        </div>
        """, unsafe_allow_html=True)

def show_dashboard():
    # Beautiful header with welcome message
    st.markdown(f"""
    <div class="main-header">
        <h1>Automation Tools Hub</h1>
        <p>Welcome back, <strong>{st.session_state.username}</strong>!</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Tool selection grid with beautiful cards
    st.markdown("### 🛠️ Available Tools")
    st.markdown("Choose from our powerful automation tools!.")
    
    # Create a grid layout for tools
    cols = st.columns(3)
    
    for idx, (tool_id, tool_info) in enumerate(TOOLS_CONFIG.items()):
        col_idx = idx % 3
        with cols[col_idx]:
            st.markdown(f"""
            <div class="tool-card">
                <h3>{tool_info['icon']} {tool_info['name']}</h3>
                <p>{tool_info['description']}</p>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button(f"Open {tool_info['name']}", key=f"btn_{tool_id}", use_container_width=True):
                st.session_state.selected_tool = tool_id
                st.rerun()
    
    # Recent activity section
    st.markdown("---")
    st.markdown("### 📊 Recent Activity")
    st.markdown("Track your latest automation jobs and their status.")
    show_recent_activity()

def show_recent_activity():
    # Simple file-based activity tracking
    try:
        from shared.gcp_utils import list_recent_jobs
        recent_jobs = list_recent_jobs()
        
        if recent_jobs:
            for job in recent_jobs[:5]:  # Show last 5
                status_class = "status-success" if job['status'] == "completed" else "status-warning" if job['status'] == "processing" else "status-info"
                st.markdown(f"""
                <div class="activity-item">
                    <strong>{job['tool']}</strong> 
                    <span class="{status_class}">● {job['status'].title()}</span> 
                    <small style="color: #666;">{job['timestamp']}</small>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="text-align: center; padding: 2rem; color: #666; background: var(--light-bg); border-radius: 10px;">
                📊 No recent activity yet. Start using the tools above to see your automation history!
            </div>
            """, unsafe_allow_html=True)
    except Exception as e:
        st.markdown("""
        <div style="text-align: center; padding: 2rem; color: #666; background: var(--light-bg); border-radius: 10px;">
            📊 Activity tracking will be available once you start using the tools
        </div>
        """, unsafe_allow_html=True)

def show_tool_page():
    """Show the selected tool page"""
    selected_tool = st.session_state.get('selected_tool')
    
    if selected_tool == 'lead_search':
        from tools.lead_search.ui import main as lead_search_main
        lead_search_main()
    elif selected_tool == 'website_resolver':
        from tools.website_resolver.ui import main as website_resolver_main
        website_resolver_main()
    elif selected_tool == 'company_relationship':
        from tools.company_relationship.ui import main as company_relationship_main
        company_relationship_main()
    elif selected_tool == 'contact_scraper':
        from tools.contact_scraper.ui import main as contact_scraper_main
        contact_scraper_main()
    elif selected_tool == 'name_cleaner':
        from tools.name_cleaner.ui import main as name_cleaner_main
        name_cleaner_main()
    elif selected_tool == 'domain_relationship':
        from tools.domain_relationship.ui import main as domain_relationship_main
        domain_relationship_main()
    else:
        st.markdown("""
        <div style="text-align: center; padding: 3rem; background: var(--light-bg); border-radius: 15px; margin: 2rem 0;">
            <h2 style="color: var(--warning-color);">⚠️ Tool Not Available</h2>
            <p style="color: #666; font-size: 1.1rem;">The tool '{selected_tool}' is not implemented yet.</p>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
