import json
import secrets
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request


app = FastAPI()
REQUEST_DIR = Path(__file__).resolve().parent


def save_request(payload: object) -> Path:
	REQUEST_DIR.mkdir(parents=True, exist_ok=True)

	while True:
		request_path = REQUEST_DIR / f"request_{secrets.randbelow(1_000_000_000)}.json"
		try:
			with request_path.open("x", encoding="utf-8") as request_file:
				json.dump(payload, request_file, indent=2)
				request_file.write("\n")
			return request_path
		except FileExistsError:
			continue


@app.api_route(
	"/{path:path}",
	methods=["DELETE", "GET", "PATCH", "POST", "PUT"],
)
async def capture_json(request: Request) -> dict[str, str]:
	try:
		payload = await request.json()
	except ValueError as error:
		raise HTTPException(status_code=400, detail="Request body must be valid JSON") from error

	request_path = save_request(payload)
	return {"saved_as": request_path.name}


if __name__ == "__main__":
	import uvicorn

	uvicorn.run(app, host="0.0.0.0", port=3111)
