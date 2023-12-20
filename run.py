from dotenv import load_dotenv
from uvicorn import run
import os

load_dotenv(".env")
app: str = "app.main:app"

# aqui va la parte que crea las carpetas pertinentes

if __name__ == "__main__":
    run(
        app, 
        host=  os.environ.get("FASTDUP_HOST"),
        port= int(os.environ.get("FASTDUP_PORT")),
        reload= bool(int(os.getenv("DEBUG")))
        )