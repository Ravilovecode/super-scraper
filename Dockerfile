# Playwright's official image ships Chromium + every system library it needs,
# plus a matching `playwright` Python package — so browser mode works out of the box.
FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

WORKDIR /app

# Python deps for the dashboard + scraper. Playwright + Chromium are already in
# the base image, so they are intentionally NOT reinstalled here (avoids a
# pip-vs-browser version mismatch).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render mounts the persistent disk here; the dashboard writes all output
# (jobs, report, resume state, uploaded CSV) into it so runs survive restarts.
ENV OUT_DIR=/var/data

# Render injects $PORT; dashboard.py binds to it automatically.
CMD ["sh", "-c", "python dashboard.py --out ${OUT_DIR:-out}"]
