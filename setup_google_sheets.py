#!/usr/bin/env python3
"""
Setup script for Google Sheets integration
This script helps you set up Google Sheets authentication
"""

import os
import json
from pathlib import Path

def create_credentials_template():
    """Create a template for Google Sheets credentials"""
    
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)
    
    credentials_file = config_dir / "google-sheets-credentials.json"
    
    if credentials_file.exists():
        print(f"Credentials file already exists: {credentials_file}")
        return
    
    # Template for Google Service Account credentials
    template = {
        "type": "service_account",
        "project_id": "your-project-id",
        "private_key_id": "your-private-key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----\n",
        "client_email": "your-service-account@your-project.iam.gserviceaccount.com",
        "client_id": "your-client-id",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
    }
    
    with open(credentials_file, 'w') as f:
        json.dump(template, f, indent=2)
    
    print(f"Created credentials template: {credentials_file}")
    print("\nTo set up Google Sheets integration:")
    print("1. Go to Google Cloud Console: https://console.cloud.google.com/")
    print("2. Create a new project or select existing one")
    print("3. Enable Google Sheets API")
    print("4. Create a Service Account")
    print("5. Download the JSON key file")
    print("6. Replace the template content with your actual credentials")
    print("7. Share your Google Sheet with the service account email")

def install_dependencies():
    """Install required Python packages"""
    print("Installing Google Sheets dependencies...")
    os.system("pip install gspread google-auth google-auth-oauthlib google-auth-httplib2")

if __name__ == "__main__":
    print("Setting up Google Sheets integration...")
    install_dependencies()
    create_credentials_template()
    print("\nSetup complete! Follow the instructions above to configure your credentials.") 