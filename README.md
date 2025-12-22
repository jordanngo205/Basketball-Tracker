# Paint Touch Tracker (Streamlit)

Single-page Streamlit app for logging paint touches by possession with analytics.

## Run locally

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Open http://localhost:8501 in your browser.

## Notes
- Data is stored in memory per session on the server.
- Export CSV from the active game using **Export CSV**.

## Database sync
- Set `DATABASE_URL` (PostgreSQL) in your environment or Posit Connect Cloud.
- Use **Sync to DB** to push the active game to the database.

## Legacy
The previous React frontend remains in `frontend/`. The old FastAPI code is in `backend/main.py`.
