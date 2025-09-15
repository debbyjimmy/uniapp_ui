import streamlit as st
from .config import get_team_credentials

def check_login(username: str = None, password: str = None) -> bool:
    """
    Check if user is logged in or validate credentials.
    
    Args:
        username: Username to validate (if provided)
        password: Password to validate (if provided)
    
    Returns:
        True if logged in or credentials are valid
    """
    # If no credentials provided, check session state
    if username is None and password is None:
        return st.session_state.get('logged_in', False)
    
    try:
        # Get current credentials (from env vars or secrets)
        team_credentials = get_team_credentials()
        
        # Validate provided credentials
        if username == team_credentials['username'] and password == team_credentials['password']:
            return True
        
        return False
    except ValueError as e:
        # Credentials not configured
        st.error(f"🔐 {str(e)}")
        return False

def require_login():
    """
    Decorator-style function to require login for pages.
    Redirects to login if not authenticated.
    """
    if not check_login():
        st.error("🔐 Please log in to access this tool.")
        st.stop()

def logout():
    """
    Clear session state and log out user.
    """
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()
