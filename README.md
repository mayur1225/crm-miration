# Project Title
Full-Stack Data Migration Application
SureShift

## Description
SureShift is system-to-system migration engine that connects directly to your CRMs, maps and migrate the data intelligently.

##  Tech Stack
- **Frontend:** Angular
- **Backend (Main):** Python

##  Project Structure
├── frontend/          # Angular client application
└── python_service/    # Python data processing main backend server

##  Prerequisites
Make sure you have the following installed on your local machine:
- [Angular CLI](https://angular.io/cli) (`npm install -g @angular/cli`)
- [Python 3.8+](https://www.python.org/)

## Installation

Before running the application, install the dependencies for each part of the stack.

1. Frontend
cd frontend 
npm install

2. PYTHON
cd Python_service
pip install -U -r requirements.txt (Linux)
pip install -U -r requirements.txt (Windows)

pip install -U -r requirements.txt
pip freeze > requirements.lock.txt

## Running the Application
1. FRONTEND: ng serve
2. PYTHON(Linux): uvicorn app.main:app --reload --port 8000
3. PYTHON(Windows): py -m uvicorn app.main:app --reload --port 8000
4. ollama pull mxbai-embed-large

## Running the Ollama
ollama pull mxbai-embed-large

## Container Application
docker build -t test-backend .
docker run -p 3000:3000 test-backend


1. To build and start everything at once:-  docker-compose up --build
2. To run them in the background:- docker-compose up -d --build
3. To stop and remove all of them at once:- docker-compose down

# URL Configurations
ANGULAR_FRONTEND_URL=http://localhost:4200
FASTAPI_BACKEND_URL=http://localhost:8000

LOCAL_AI_URL="http://127.0.0.1:11434/api"
LOCAL_AI_MODEL="mxbai-embed-large"

ENVIRONMENT=development
DISABLE_SSL_VERIFY=true
SUPABASE_SSL_VERIFY=false

SUPABASE_URL=https://tikxlmehnlxpjmcgvplv.supabase.co
SUPABASE_KEY=

# Salesforce OAuth Credentials
SF_CLIENT_ID=
SF_CLIENT_SECRET=

# Zendesk OAuth Credentials
ZD_CLIENT_ID=zdg-sureshift_migrator
ZD_CLIENT_SECRET=

ZOHO_CLIENT_ID=
ZOHO_CLIENT_SECRET=

MS_CLIENT_ID="your-microsoft-app-client-id"
MS_CLIENT_SECRET="your-microsoft-app-client-secret"

#HubSpot OAuth Credentials
HS_CLIENT_ID=
HS_CLIENT_SECRET=
HS_REDIRECT_URI=http://localhost:8000/api/crm/auth/hubspot/callback
