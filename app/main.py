from fastapi import FastAPI, HTTPException
from .schemas import Matching_images
from .s3 import S3
import pandas as pd
import fastdup
import json
import os

app = FastAPI()

review_path = lambda path : path if path.endswith("/") else path + "/"

@app.post("/matching/image/")
async def matching_images(matching_data: Matching_images):
    
    bucket = matching_data.bucket
    path_origin_file = matching_data.path_origin_file
    path_alternative_file = matching_data.path_alternative_file
    path_origin_img =  review_path(matching_data.path_origin_img)
    path_alternative_img =  review_path(matching_data.path_alternative_img)
    path_report = matching_data.path_report
    img_per_object = matching_data.img_per_object
    setting = matching_data.setting

    try:
        s3 = S3(bucket=bucket)
    except:
        detail["bucket"] = {
            "msm" : "El nombre del bucket no existe o esta mal escrito.",
            "bad_bucket": bucket
        }
        raise HTTPException(status_code=404, detail= detail)
        
    # validamos las direcciones de los archivos
    
    is_correct_file, error_route_file = s3.valid_file([path_origin_file, path_alternative_file])
    is_correct_img,  error_route_img = s3.valid_route([path_origin_img, path_alternative_img])
    
    # Validamos que la ruta donde se va a guardar el reporte exista
    is_correct_report_path, error_route_report_path = s3.valid_route(
        review_path(
            "/".join(path_report.split("/")[:-1])
            )
        )
    # Validamos que el archivo no exista en la ruta proporcionada
    existe_report, route_report_exists = s3.valid_file(path_report)
    
    detail : dict = {}
    
    if not is_correct_file:
        detail["route_files"] = {
            "msm" : "Las rutas de los archivos no existen o estan mal escritos.",
            "bad_routes": error_route_file
        }
        
    if not is_correct_img:
        detail["route_img"] = {
            "msm" : "Las rutas de las imagenes no existen o estan mal escritos.",
            "bad_routes": error_route_img
        }
    
    if not is_correct_report_path:
        detail["route_report"] = {
            "msm" : "La ruta donde se va a guardar el reporte esta mal escrito o no existe.",
            "bad_routes": error_route_report_path
        }
    
    if existe_report:
        detail["route_report"] = {
            "msm" : f"Ya existe un archivo con el nombre '{path_report.split('/')[-1]}' debes ingresar otro nombre para que este no se sobre escriba.",
            "file_exists": path_report
        }
    
    if len(detail):
        raise HTTPException(status_code=404, detail= detail)
    
    del error_route_file, is_correct_file
    del error_route_img, is_correct_img 
    del is_correct_report_path, error_route_report_path
    del existe_report, route_report_exists
    
    
    # finalización de la parte de validación de data
    
    PATH_TRASH: str = "trash/s3/"
    WORK_DIR: str = "trash/fastdup/"
    FIELD_NAME_IMAGES: str = "product_images"
    
    # descargamos los archivos del origin y el aternative
    # lo pasamos a df y despues borramos el archivo
    
    origin_local_file = s3.download_file(PATH_TRASH, path_origin_file)
    df_origin: pd.DataFrame = pd.read_json(origin_local_file)
    os.remove(origin_local_file)
    
    alternative_local_file = s3.download_file(PATH_TRASH, path_alternative_file)
    df_aternative: pd.DataFrame = pd.read_json(alternative_local_file)
    os.remove(alternative_local_file)
    
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
    
    # la lista de imagenes que vamos a analizar lo apsamos a txt para que fastdup las procese
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

    for _, row in result.iterrows():
        
        # buscamos el sku que corresponde al archivo
        
        filename_origin = row["filename_from"]
        filename_alternative = row["filename_to"]
        
        for _, row in df_origin.iterrows():
            if filename_origin in row["product_images"]:
                ref_origin = row["sku"]
                break
        
        for _, row in df_aternative.iterrows():
            if filename_alternative in row["product_images"]:
                ref_alternative = row["sku"]
                break
        
        if ref_origin not in matches:
            matches[ref_origin] = ref_alternative
    
    LOCAL_REPORT_PATH: str = "trash/reports/" + path_report.split("/")[-1]
    
    # creamos un archivo json donde guardaremos el match
    with open(LOCAL_REPORT_PATH, "w", encoding="utf8") as file:
        json.dump(matches, file, indent=4)
    
    path_report_copy = review_path("/".join(path_report.split("/")[:-1]))
    
    # subimos el reporte al s3
    s3.upload_file(LOCAL_REPORT_PATH, path_report_copy)
    
    # borramos el archivo local 
    os.remove(LOCAL_REPORT_PATH)
    
    return {
        "matches_found": len(matches),
        "route_where_is_report": path_report,
        "report_file_name": path_report.split("/")[-1]
    }