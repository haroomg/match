from fastapi import FastAPI, HTTPException
from .schemas import Matching_images
from .s3 import s3
import pandas as pd
import fastdup
import os

app = FastAPI()


@app.post("/matching/image/")
def matching_images(matching_data: Matching_images):
    bucket = matching_data.bucket
    path_origin_file = matching_data.path_origin_file
    path_alternative_file = matching_data.path_alternative_file
    path_origin_img = matching_data.path_origin_img
    path_alternative_img = matching_data.path_alternative_img
    img_per_object = matching_data.img_per_object

    # validamos que el bucket exista
    if bucket:
        buckets: list = [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]
        
        if bucket not in buckets:
            raise HTTPException(status_code=404, detail=f"El nombre del Bucket '{bucket}' esta mal escrito o no existe.")
    else:
        raise HTTPException(status_code=404, detail="Debes ingresar el nombre del Bucket")
    del buckets
    
    # validamos que los archivos origin y alternative existan
    for path, type in zip([path_origin_file, path_alternative_file],["origin-file", "alternative-file"]):
        
        if path:
            if isinstance(path, str):
                
                extencion = path.split(".")[-1]
                
                if extencion == "json":
                    try:
                        s3.head_object(Bucket=bucket, Key=path)
                    except:
                        raise HTTPException(status_code=404, detail=f"{type}: El archivo '{path}' no existe o esta mal escrito")
                else:
                    raise HTTPException(status_code=404, detail=f"{type}: El archivo '{path}' debe de ser se tipo json no de '{extencion}'")
            else:
                raise HTTPException(status_code=404, detail=f"{type}: El parametro debe de ser de tipo str no de {type(path)}")
        else:
            raise HTTPException(status_code=404, detail=f"{type}: No se puede dejar vacio este atributo.")
    del extencion
    
    # validamos que la direccion donde ese encuentran las imagenes existan
    for path, type in zip([path_origin_img, path_alternative_img],["origin-image", "alternative-image"]):
        
        if path:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=path)
            if "Contents" not in response:
                raise HTTPException(status_code=404, detail=f"{type}: La ruta ingresada no existe o esta mal escrita.")
        else:
            raise HTTPException(status_code=404, detail=f"{type}: Debes ingresar la direccion de las imagenes.")
    del response
    
    # descargamos los archivos del origin y el aternative
    PATH_TRASH: str = "trash/s3/json/"
    
    file_origin: str = os.path.join(PATH_TRASH, path_origin_file.split("/")[-1])
    file_alternative: str = os.path.join(PATH_TRASH, path_alternative_file.split("/")[-1])
    
    # descargamos los archivos json
    for path_local, path_s3 in zip([file_origin, file_alternative], [path_origin_file, path_alternative_file]):
        
        s3.download_file(
            bucket, 
            path_s3,
            path_local
        )
    
    # # pasamos los json file a df y despues borramos esos archivos
    
    df_origin: pd.DataFrame = pd.read_json(file_origin)
    os.remove(file_origin)
    
    df_aternative: pd.DataFrame = pd.read_json(file_alternative)
    os.remove(file_alternative)
    
    WORK_DIR: str = "trash/fastdup/"
    input_dir: list = []
    
    if img_per_object == 0 or img_per_object == None:
        input_dir = [
            f"s3://{bucket}/{path_origin_img}", # path images origin
            f"s3://{bucket}/{path_alternative_img}" # path images alternative
        ]
    else:
        pass
    
    fd = fastdup.create(WORK_DIR)
    fd.run(input_dir, threshold= 0.5, overwrite= True, high_accuracy= True)
    similarity = fd.similarity()
    
    similarity.to_json("trash/s3/json/similarity.json")
    
    return {
        "bucket": bucket,
        "path_origin_file": path_origin_file,
        "path_alternative_file": path_alternative_file,
        "path_origin_img": path_origin_img,
        "path_alternative_img": path_alternative_img,
        "img_per_object": img_per_object
    }