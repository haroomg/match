from fastapi import FastAPI, HTTPException
from app.functions import add_metadata, search_parameter
from app.schemas import Matching_images
from app.s3 import S3
import pandas as pd
import fastdup
import sqlite3
import shutil
import ijson
import json
import os 

# constantes
# Direccion de los archivos temporales
PATH_IMAGES = "trash/img"
PATH_REPORT = "trash/reports/"
PATH_JSON = "trash/s3/"
PATH_DB = "trash/db/"
PATH_FASTDUP = "trash/fastdup/"

# Nombre de las tablas de db
TABLE_NAME = ["origin", "alternative"]

# queries para SQLite
CREATE_TABLE = "CREATE TABLE IF NOT EXISTS {0} (file_name VARCHAR(1024), ref VARCHAR(256))"
INSERT = "INSERT INTO {0} (file_name, ref) VALUES (?, ?)"
SELECT = "SELECT ref FROM {0} WHERE file_name = '{1}'"

review_path = lambda path : path if path.endswith("/") else path + "/"

def Matching_images(response: dict = None) -> dict:


    bucket = response["bucket"]
    path_origin_file = response["path_origin_file"]
    path_alternative_file = response["path_alternative_file"]
    path_origin_img = review_path(response["path_origin_img"])
    path_alternative_img = review_path(response["path_alternative_img"])
    origin_file_name_imgs = response["setting"]["origin_file_name_imgs"]
    alternative_file_name_imgs = response["setting"]["alternative_file_name_imgs"]
    ref_origin = response["setting"]["ref_origin"]
    ref_alternative = response["setting"]["ref_alternative"]
    path_report_s3 = response["path_report"]
    

    # analizamos la data obtenida del response 
    try:
        s3 = S3(bucket=bucket)
        fd = fastdup.create(PATH_FASTDUP)
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
            "/".join(path_report_s3.split("/")[:-1])
            )
        )
    # Validamos que el archivo no exista en la ruta proporcionada
    existe_report, route_report_exists = s3.valid_file(path_report_s3)
    
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
            "msm" : f"Ya existe un archivo con el nombre '{os.path.basename(path_report)}' debes ingresar otro nombre para que este no se sobre escriba.",
            "file_exists": path_report
        }
    
    if len(detail):
        raise HTTPException(status_code=404, detail= detail)
    
    del error_route_file, is_correct_file
    del error_route_img, is_correct_img 
    del is_correct_report_path, error_route_report_path
    del existe_report, route_report_exists
    

    if "img_per_object" in response:
        img_per_object = response["img_per_object"]
    else:
        img_per_object = 0
    
    alternative_search_parameter = response["setting"]["alternative_search_parameter"]

    if "origin_search_parameter" in response["setting"]:
        if len(response["setting"]["origin_search_parameter"]):
            origin_search_parameter = response["setting"]["origin_search_parameter"]
        else:
            origin_search_parameter = None
    else:
        origin_search_parameter = None

    if "alternative_search_parameter" in response["setting"]:
        if len(response["setting"]["alternative_search_parameter"]):
            alternative_search_parameter = response["setting"]["alternative_search_parameter"]
        else:
            alternative_search_parameter = None
    else:
        alternative_search_parameter = None
        
    
    # finalización de la parte de validación de data
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------
    

    # s3 data path
    s3_json_path =  [path_origin_file, path_alternative_file]

    # s3 img path
    s3_img_path = [path_origin_img, path_alternative_img]

    field_name_file_images = [origin_file_name_imgs, alternative_file_name_imgs]
    field_name_ref = [ref_origin, ref_alternative]

    # parametros de busqueda
    parameters = [origin_search_parameter, alternative_search_parameter]

    # filename db and images
    db_name = "trash/db/db_filename.sqlite"
    filename_images = "trash/fastdup/path_images.txt"
    path_report = os.path.join(PATH_REPORT, os.path.basename(path_report_s3))

    # creamos la db donde vamos a guardar el nombre de las imagenes junto con su referencia
    with sqlite3.connect(db_name) as con:
        
        # creamos el archivo donde vamos a guardar la direccion de las imagenes 
        with open(filename_images, "w", encoding="utf-8") as file:

            for i, path in enumerate(s3_json_path):

                # descargamos el archivo de la data scrapeada
                json_path = s3.download_file(PATH_JSON, path)

                if parameters[i]:

                    search_parameter(
                        json_path,
                        parameters[i]
                    )
                
                # abrimos el archivo i lo recorremos con un iterador por trozo
                with open(json_path, "r", encoding="utf-8") as json_file:
                    
                    # creamos la tabla donde se va guardar los nombres junto con su referencia
                    con.execute(CREATE_TABLE.format(TABLE_NAME[i]))
                    objets_json = ijson.items(json_file, "item")
                    
                    for obj in objets_json:
                        
                        images_name = obj[field_name_file_images[i]]
                        ref = obj[field_name_ref[i]]
                        amount = img_per_object if len(images_name) >= img_per_object else len(images_name)
                        images_name = images_name[:amount]

                        for image in images_name:
                            
                            # si el valor es false se lo salta
                            if image:
                            
                                path_s3_img = os.path.join(s3_img_path[i], image)
                                con.execute(INSERT.format(TABLE_NAME[i]), (image, ref))
                                path_local_img = s3.download_file(PATH_IMAGES, path_s3_img)
                                file.write(path_local_img+"\n")
            
                os.remove(json_path)
                con.commit()
    
    # revisamos las imagenes con fastdup
    fd.run(filename_images, threshold= 0.5, overwrite= True, high_accuracy= True)
    # pedimos las imagenes que son invalidas
    invalid_img_s3: list = fd.invalid_instances()["filename"].to_list()

    if len(invalid_img_s3):
        for damaged_file in invalid_img_s3:
            add_metadata(damaged_file)
        
        # analizo las imagenes de nuevo
        fd.run(filename_images, threshold= 0.5, overwrite= True, high_accuracy= True)

    similarity = fd.similarity()

    # cambiamos el la direccion de las imagenes para que solo sea el nombre del archivo
    for col_name in ["filename_from", "filename_to"]: 
        similarity[col_name] = similarity[col_name].apply(lambda x : os.path.basename(x))

    matches = {}

    with sqlite3.connect(db_name) as con:
        
        for i, row in similarity.iterrows():

            filename_from = row["filename_from"]
            code_ref_origin = con.execute(SELECT.format("origin", filename_from)).fetchone()

            if code_ref_origin:

                filename_to = row["filename_to"]
                code_ref_aternative = con.execute(SELECT.format("alternative", filename_to)).fetchone()
                
                if not code_ref_aternative:
                    continue
                else:
                    code_ref_origin = code_ref_origin[0]
                    code_ref_aternative = code_ref_aternative[0]

                    if code_ref_origin not in matches:
                        matches[code_ref_origin] = {code_ref_aternative: row["distance"]}
                    else:
                        matches[code_ref_origin][code_ref_aternative] = row["distance"]

    report = {
        "response": response,
        "macht_founds": len(matches),
        "matches": matches
    }

    with open(path_report, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)
    
    
    s3.upload_file(path_report, review_path(os.path.dirname(path_report_s3)))
    os.remove(path_report)

    # borramos los archivos generados
    for name in ["fastdup", "img", "reports", "s3", "db"]:
        path = f"trash/{name}"
        shutil.rmtree(path)
        os.makedirs(path)

    return {
        "response": response,
        "macht_founds": len(matches),
        "invalid_img": invalid_img_s3
    }


response = {
    "bucket": "hydrahi4ai",

    "path_origin_file": "ajio-myntra/origin/20240112/New_collector_20240112_172137.success.json",
    "path_alternative_file": "ajio-myntra/alternative/20240110/Myntra__Marianfer_Cruz_20240111_095149.success.json",

    "path_origin_img": "ajio-myntra/origin/20240112/",
    "path_alternative_img": "ajio-myntra/alternative/20240110/",

    "path_report": "ajio-myntra/reports/matchin_ajio_myntra_1.json",

    "img_per_object": 1,
    
    "setting": {
        "origin_file_name_imgs": "product_images",
        "alternative_file_name_imgs": "product_images",
        
        "ref_origin": "sku",
        "ref_alternative": "sku",
        
        "origin_search_parameter": {
            "brand": "2bme"
        },
        "alternative_search_parameter": {
            "brand": "2Bme"
        }
    }
}

Matching_images(response)