# Paint Touch Tracker

Lightweight offline-first web app for logging paint touches by possession and syncing to a local FastAPI server.

## Frontend

```bash
cd frontend
npm install
npm run dev
```

## Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

## Offline flow

- Use the web app offline; data is stored in localStorage.
- When Wi-Fi is available, click **Sync to SQL** to push to the backend.
- Export CSV anytime from the active game.
