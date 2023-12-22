from fastapi import FastAPI, HTTPException
from .schemas import Matching_images
from .s3 import s3
import pandas as pd
import fastdup
import json
import os

app = FastAPI()


@app.post("/matching/image/")
async def matching_images(matching_data: Matching_images):
    bucket = matching_data.bucket
    path_origin_file = matching_data.path_origin_file
    path_alternative_file = matching_data.path_alternative_file
    path_origin_img = matching_data.path_origin_img
    path_alternative_img = matching_data.path_alternative_img
    path_report = matching_data.path_report
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
    
    # validamos que la direccion donde se va a guardar el reporte exista y que el nombre del archivo no este siendo usado por otro
    if path_report:
        
        # validamos que exista la carpeta
        path = "/".join(path_report.split("/")[:-1])
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=path)
        if "Contents" not in response:
                raise HTTPException(status_code=404, detail=f"{type}: La ruta ingresada donde se va a guardar el archivo no existe o esta mal escrita.")
        
        # validamos que el nombre del archivo dado no este siendo usado por otro archivo
        file_name = path_report.split("/")[-1]
        
        if file_name.split(".")[-1] == "json":
            try:
                s3.head_object(Bucket=bucket, Key=path)
                raise HTTPException(f"El archivo {file_name} ya existe en {path}.")
            except:
                pass
        else:
            raise HTTPException(status_code=404, detail=f"La extencion del reporte debe de ser de tipo json.")
        
    else:
        raise HTTPException(status_code=404, detail="Debes ingresar la direccion donde se va a guardar el reporte")
    
    # descargamos los archivos del origin y el aternative
    PATH_TRASH: str = "trash/s3/"
    
    file_origin: str = os.path.join(PATH_TRASH, path_origin_file.split("/")[-1])
    file_alternative: str = os.path.join(PATH_TRASH, path_alternative_file.split("/")[-1])
    
    # descargamos los archivos json
    for path_local, path_s3 in zip([file_origin, file_alternative], [path_origin_file, path_alternative_file]):
        
        s3.download_file(
            bucket, 
            path_s3,
            path_local
        )
    
    # pasamos los json file a df y despues borramos esos archivos
    
    df_origin: pd.DataFrame = pd.read_json(file_origin)
    os.remove(file_origin)
    
    df_aternative: pd.DataFrame = pd.read_json(file_alternative)
    os.remove(file_alternative)
    
    WORK_DIR: str = "trash/fastdup/"
    FIELD_NAME_IMAGES: str = "product_images"
    input_dir: list = []
    
    # abquirimos el nombre de los archivos para armar un file txt con la ruta de cada imagen para pasarlo como argumento al input_dir
    list_images_name_origin: list = df_origin[FIELD_NAME_IMAGES].to_list()
    list_images_name_alternative: list = df_aternative[FIELD_NAME_IMAGES].to_list()
    
    for path_s3, list_img in zip([path_origin_img, path_alternative_img], [list_images_name_origin, list_images_name_alternative]):
        
        for images in list_img: 
            
            if img_per_object == 0:
                amount = len(list_img)
            else:
                if len(list_img) <= img_per_object:
                    amount = len(list_img)
                if len(list_img) > img_per_object:
                    amount = img_per_object
                    
            for img  in images[0:amount]:
                if img:
                    input_dir.append(
                        f"s3://{bucket}/{path_s3}{img}\n"
                    )
                
    path_files_s3: str = "trash/fastdup/address_files_s3.txt"
    with open(path_files_s3, "w", encoding="utf8") as file:
        for path in input_dir:
            file.write(path)
        
    fd = fastdup.create(WORK_DIR)
    fd.run(path_files_s3, threshold= 0.5, overwrite= True, high_accuracy= True)
    similarity = fd.similarity()
    os.remove(path_files_s3)
    
    for col_name in ["filename_from", "filename_to"]:
        similarity[col_name] = similarity[col_name].apply(lambda x : x.split("/")[-1])

    # aqui empieza el anailisis de la data de cuales fueron las imagenes con similitud
    result = pd.DataFrame()
    matches: dict = {}

    # concatenamos los elemontos que se encuentren en filename_from del origin
    for index, row in df_origin.iterrows():
        
        product_images = row["product_images"]
        search = similarity[similarity["filename_from"].isin(product_images) & ~(similarity["filename_to"].isin(product_images))]
        
        if len(search) != 0:
            
            result = pd.concat([result, search])

    result = result.reset_index(drop=True)

    # eliminamos los elementos que se encuentren en filename_to del origin
    for index, row in df_origin.iterrows():
        
        product_images = row["product_images"]
        
        search = result[result["filename_to"].isin(product_images)].index
            
        if len(search) != 0:   
            result = result.drop(index=search)

    result = result.reset_index(drop=True)

    for index, row in result.iterrows():
        
        # buscamos el sku que corresponde al archivo
        
        filename_origin = row["filename_from"]
        filename_alternative = row["filename_to"]
        
        for index, row in df_origin.iterrows():
            if filename_origin in row["product_images"]:
                ref_origin = row["sku"]
                break
        
        for index, row in df_aternative.iterrows():
            if filename_alternative in row["product_images"]:
                ref_alternative = row["sku"]
                break
        
        if ref_origin not in matches:
            matches[ref_origin] = ref_alternative
    
    local_path: str = "trash/reports/" + path_report.split("/")[-1]
    
    # creamos un archivo json donde guardaremos el match
    with open(local_path, "w", encoding="utf8") as file:
        json.dump(matches, file, indent=4)
    
    # subimos el reporte al s3
    s3.upload_file(local_path, bucket, path_report)
    
    # borramos el archivo local 
    os.remove(local_path)
    
    return {
        "matches_found": len(matches),
        "route_where_is_report": path_report,
        "report_file_name": path_report.split("/")[-1]
    }