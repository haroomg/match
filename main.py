from fastapi import FastAPI, HTTPException
from typing import Union
from dotenv  import load_dotenv
from pydantic import BaseModel, validator
import pandas as pd
import boto3
import os

# obtenemos los env
load_dotenv(".env")
# Crea una instancia del cliente de S3
s3 = boto3.client(
    's3',
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY"),
)

# clase que valida el request para el metodo post
class Matching_images(BaseModel):
    
    bucket: str
    path_origin_file: str
    path_alternative_file: str
    path_origin_img: str
    path_alternative_img: str
    img_per_object: Union[int, None]
    
    @validator("bucket")
    def validate_bucket(cls, bucket):
        if bucket:
            buckets: list = [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]
            if isinstance(bucket, str):
                if bucket in buckets:
                    return bucket
                else:
                    raise ValueError(f"El nombre del Bucket '{bucket}' esta mal escrito o no existe.")
            else:
                raise ValueError(f"El debe de ser de tipo string no {type(bucket)}")
        else:
            raise ValueError(f"se debe ingresar el nombre del bucket.")
    
    
    for path_file in ["path_origin_file", "path_alternative_file"]:
        
        @validator(path_file)
        def validate_path_file(cls, path_file):
            if path_file:
                if isinstance(path_file, str):
                    
                    extencion = path_file.split(".")[-1]
                    
                    if extencion == "json":
                        try:
                            s3.head_object(Bucket= cls.bucket, Key=path_file)
                            return path_file
                        except:
                            raise ValueError(f"{type}: El archivo '{path_file}' no existe o esta mal escrito")
                    else:
                        raise ValueError(f"{type}: El archivo '{path_file}' debe de ser se tipo json no de '{extencion}'")
                else:
                    raise ValueError(f"{type}: El parametro debe de ser de tipo str no de {type(path_file)}")
            else:
                raise ValueError(f"{type}: No se puede dejar vacio este atributo.")


app = FastAPI()

@app.post("/matching_images")
def matchin_images( **request: Matching_images ) -> dict:
    
    return

