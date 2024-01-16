from dotenv import load_dotenv
from uvicorn import run
import os

load_dotenv(".env")
app: str = "app.main:app"


if __name__ == "__main__":
    
    # creamos la carpeta trash donde vamos a estar creando y borrando carpetas y archivos
    if os.path.exists("trash"):
        if not os.path.exists("trash/fastdup/"):
            os.makedirs("trash/fastdup/")
            
        if not os.path.exists("trash/reports/"):
            os.makedirs("trash/reports/")
        
        if not os.path.exists("trash/s3/"):
            os.makedirs("trash/s3/")
            
    else:
        os.makedirs("trash")
        os.makedirs("trash/fastdup/")
        os.makedirs("trash/reports/")
        os.makedirs("trash/s3/")
    
    run(
            app, 
            host=  os.environ.get("FASTDUP_HOST"),
            port= int(os.environ.get("FASTDUP_PORT")),
            reload= bool(int(os.getenv("DEBUG"))),
            timeout_keep_alive=None
        )