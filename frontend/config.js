// ─── API Configuration ───────────────────────────────────────────────────────
// Change this to your Render backend URL after deployment
// e.g. "https://ca-daily-api.onrender.com"
const API_BASE = window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
  ? "http://localhost:8000"
  : "https://ca-portal-backend-uaha.onrender.com";  // ← UPDATE THIS after Render deploy

window.API_BASE = API_BASE;
