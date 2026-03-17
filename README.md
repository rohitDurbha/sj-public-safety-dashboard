# San José Public Safety & Services Intelligence Dashboard

A comprehensive multi-dataset analysis combining **Fire Incidents**, **Police Calls for Service**, and **311 Service Requests** from the City of San José Open Data Portal.

**Live data** is pulled directly from [data.sanjoseca.gov](https://data.sanjoseca.gov) on every load — no hardcoded values.

## Datasets Used

| Dataset | Source | Years |
|---------|--------|-------|
| Fire Incidents | [data.sanjoseca.gov/dataset/san-jose-fire-incidents](https://data.sanjoseca.gov/dataset/san-jose-fire-incidents) | 2015–2024 |
| Police Calls for Service | [data.sanjoseca.gov/dataset/police-calls-for-service-quarterly](https://data.sanjoseca.gov/dataset/police-calls-for-service-quarterly) | 2021–2023 |
| 311 Service Requests | [data.sanjoseca.gov/dataset/311-service-request-data](https://data.sanjoseca.gov/dataset/311-service-request-data) | 2017–2025 |

## Local Development

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/sj-public-safety-dashboard.git
cd sj-public-safety-dashboard

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run app.py
```

The dashboard will open at `http://localhost:8501`.

## Deploy to Streamlit Community Cloud (Free)

See the deployment steps in the companion guide or follow these quick steps:

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Sign in with GitHub
4. Click "New app" → select your repo → set `app.py` as the main file
5. Click "Deploy"

Your dashboard will be live at `https://YOUR_APP.streamlit.app` within minutes.

## License

Analysis code: MIT  
Data: Creative Commons CC0 / CC-BY (San José Open Data Portal)
