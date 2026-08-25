import json
import secrets
from functools import cache
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request


PORT = 3111
RANDOM_NUMBER_LIMIT = 1_000_000_000
SUPPORTED_METHODS = ["DELETE", "GET", "PATCH", "POST", "PUT"]

app = FastAPI()


@cache
def request_directory() -> Path:
    """Return the directory where captured request bodies are stored."""
    return Path(__file__).resolve().parent


def save_request(payload: object) -> Path:
    """Save a JSON payload to a uniquely named local file."""
    while True:
        request_path = (
            request_directory()
            / f"request_{secrets.randbelow(RANDOM_NUMBER_LIMIT)}.json"
        )
        try:
            with request_path.open("x", encoding="utf-8") as request_file:
                json.dump(payload, request_file, ensure_ascii=False, indent=2)
                request_file.write("\n")
            return request_path
        except FileExistsError:
            continue


@app.api_route("/{path:path}", methods=SUPPORTED_METHODS)
async def capture_json(request: Request, path: str) -> dict[str, str]:
    """Persist the JSON body sent to any route."""
    del path

    try:
        payload = await request.json()
    except ValueError as error:
        raise HTTPException(
            status_code=400,
            detail="Request body must contain valid JSON",
        ) from error

    request_path = save_request(payload)
    return {"saved_as": request_path.name}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
