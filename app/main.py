from .functions import add_metadata, delete_directory_content, search_parameter, assign_reference_to_image
from fastapi import FastAPI, HTTPException
from .schemas import Matching_images
import pandas as pd
from .s3 import S3
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
    
    request: dict = {
        "bucket": matching_data.bucket,
        "path_origin_file": matching_data.path_origin_file,
        "path_alternative_file": matching_data.path_alternative_file,
        "path_origin_img": matching_data.path_origin_img,
        "path_alternative_img": matching_data.path_alternative_img,
        "path_report": matching_data.path_report,
        "img_per_object": matching_data.img_per_object,
        "setting": matching_data.setting
    }

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
            "msm" : f"Ya existe un archivo con el nombre '{os.path.basename(path_report)}' debes ingresar otro nombre para que este no se sobre escriba.",
            "file_exists": path_report
        }
    
    if len(detail):
        raise HTTPException(status_code=404, detail= detail)
    
    del error_route_file, is_correct_file
    del error_route_img, is_correct_img 
    del is_correct_report_path, error_route_report_path
    del existe_report, route_report_exists
    
    
    # finalización de la parte de validación de data
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------
    
    PATH_TRASH: str = "trash/s3/"
    WORK_DIR: str = "trash/fastdup/"
    
    origin_field_name_images: str = setting["origin_file_name_imgs"]
    alternative_field_name_images: str = setting["alternative_file_name_imgs"]
    
    ref_origin_field_name: str = setting["ref_origin"]
    ref_alternative_field_name: str = setting["ref_alternative"]
    
    # descargamos los archivos del origin y el aternative y lo pasamos a df y despues borramos el archivo
    
    # ___________________ data_start : origin _________________________________#
    #3 no necesariamente tiene que ser un df o si pero hay que iterarlo
    # origin
    origin_local_file = s3.download_file(PATH_TRASH, path_origin_file)
    
    if "origin_search_parameter" in setting:
        
        if len(setting["origin_search_parameter"]):
            
            df_origin: pd.DataFrame = search_parameter(
                pd.read_json(origin_local_file),
                setting["origin_search_parameter"]
            )
        
        else:
            df_origin: pd.DataFrame = pd.read_json(origin_local_file)
        
    else: 
        df_origin: pd.DataFrame = pd.read_json(origin_local_file)
        
    # una vez obtenido el df_origin, asignamos cada una de las imagenes a su punto de referencia
    origin_img_with_ref: pd.DataFrame = assign_reference_to_image(df_origin, origin_field_name_images, ref_origin_field_name)
    # borramos el archivo
    os.remove(origin_local_file)
    
    # abquirimos el nombre de los archivos del json de origin
    files_img_origin: list = df_origin[origin_field_name_images].to_list()
    
    # una vez obtenido el nombre de los archivo eliminamos la variable de df_origin
    del df_origin
    
    # ___________________ data_end  _________________________________#
    
    # ______________________________________________________________________________#
    
    # ___________________ data_start : alternative _________________________________#
    #3 no necesariamente tiene que ser un df o si pero hay que iterarlo
    # alternative
    alternative_local_file = s3.download_file(PATH_TRASH, path_alternative_file)
    
    if "alternative_search_parameter" in setting:
        
        if len(setting["alternative_search_parameter"]):
        
            df_alternative: pd.DataFrame = search_parameter(
                pd.read_json(alternative_local_file),
                setting["alternative_search_parameter"]
            )
        else:
            df_alternative: pd.DataFrame = pd.read_json(alternative_local_file)
    
    else:
        df_alternative: pd.DataFrame = pd.read_json(alternative_local_file)
    
    # una vez obtenido el df_alternative, asignamos cada una de las imagenes a su punto de referencia
    alternative_img_with_ref: pd.DataFrame = assign_reference_to_image(df_alternative, alternative_field_name_images, ref_alternative_field_name)
    # borramos el archivo
    os.remove(alternative_local_file)
    
    # abquirimos el nombre de los archivos del json de alternative
    files_img_alternative: list = df_alternative[alternative_field_name_images].to_list()
    
    # una vez obtenido el nombre de los archivo eliminamos la variable de df_alternative
    del df_alternative
    
    # ___________________ data_end  _________________________________#
    
    # aqui se van a guardar las direcciones de las imagenes en el s3
    input_dir: list = []
    
    for path_s3, list_img in zip([path_origin_img, path_alternative_img], [files_img_origin, files_img_alternative]):
        
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
    
    # la lista de imagenes que vamos a analizar lo pasamos a txt para que fastdup las procese
    path_files_s3: str = "trash/fastdup/address_files_s3.txt"
    with open(path_files_s3, "w", encoding="utf8") as file:
        for path in input_dir:
            file.write(path)
    
    # ejecutamos el analisis de fastdup
    fd = fastdup.create(WORK_DIR)
    fd.run(path_files_s3, threshold= 0.5, overwrite= True, high_accuracy= True)
    
    # Validamos si existen imagines corruptas
    fastdup_path: str = "trash/fastdup/tmp/"
    path_bucket: str = f"s3://{bucket}/"
    
    invalid_img_s3: list = [os.path.join(fastdup_path, path.replace(path_bucket, "")) for path in fd.invalid_instances()["filename"].to_list()]
    
    # si hay una imagen dañada, la arreglamos y ejecutamos el analisis de nuevo de manera local
    if len(invalid_img_s3): 
        
        for img_path in invalid_img_s3:
            # acomodamos las imagenes que no contienen metadata
            add_metadata(img_path)
        
        # Extraigo el nombre de las imagens de se descargaron del s3 y le pasamos su direccion local
        local_images: list = [os.path.join(fastdup_path, path.replace(path_bucket, "").replace("\n", "")) for path in input_dir]
    
        # borramos el input_dir para liberar la memoria
        del input_dir
        
        # corremos de nuevo el fastdup
        fd.run(local_images, threshold= 0.5, overwrite= True, high_accuracy= True)
        
        # Volvemos a pedir las imagenes que estan corruptas que no se puedieron reparar, notificamos el error
        invalid_img_s3: list = fd.invalid_instances()["filename"].to_list()
        
    else:
        # borramos el input_dir para liberar la memoria
        del input_dir
    
    similarity = fd.similarity()
    # elimiamos el archivo
    os.remove(path_files_s3)
    
    # cambiamos el la direccion de las imagenes para que solo sea el nombre del archivo
    for col_name in ["filename_from", "filename_to"]: 
        similarity[col_name] = similarity[col_name].apply(lambda x : os.path.basename(x))

    # aqui empieza el anailisis de la data de cuales fueron las imagenes con similitud
    matches: dict = {}
    
    # removemos los file del artenative en la columna "filename_from"
    similarity = similarity[similarity["filename_from"].isin(origin_img_with_ref["file_name"].to_list())]
    # removemos los file del origin en la columna "filename_to"
    similarity = similarity[similarity["filename_to"].isin(alternative_img_with_ref["file_name"].to_list())]

    for _, row in similarity.iterrows():
        
        # buscamos el ref que corresponde al archivo
        filename_origin = row["filename_from"]
        filename_alternative = row["filename_to"]
        
        #3
        # buscamos el codigo de referencia de en nuestro df de img_with_ref
        code_origin_ref = str(origin_img_with_ref.loc[origin_img_with_ref["file_name"] == filename_origin, "ref"].values[0])
        code_alternative_ref = str(alternative_img_with_ref.loc[alternative_img_with_ref["file_name"] == filename_alternative, "ref"].values[0])
        
        #3
        if code_origin_ref not in matches:
            matches[code_origin_ref] = [
                {
                    "alternative_ref": code_alternative_ref,
                    "distance": row["distance"]
                }
            ]
        else:
            if code_alternative_ref not in [ref["alternative_ref"] for ref in matches[code_origin_ref]]:
                matches[code_origin_ref].append(
                    {
                        "alternative_ref": code_alternative_ref,
                        "distance": row["distance"]
                    }
                )
    
    # borramos los archivos generados por fastdup
    delete_directory_content(WORK_DIR)
    
    # creamos un archivo json donde guardaremos el match
    if len(matches):
        
        local_report_path: str = os.path.join("trash/reports/", os.path.basename(path_report))
        
        report: dict = {
            "request": request,
            "matches": matches
        }
        
        with open(local_report_path, "w", encoding="utf8") as file:
            json.dump(report, file, indent=4)
        
        path_report_copy = review_path("/".join(path_report.split("/")[:-1]))

        # subimos el reporte al s3
        s3.upload_file(local_report_path, path_report_copy)

        # borramos el archivo local 
        os.remove(local_report_path)

        msm: dict = {
            "matches_found": len(matches), # numero de matches encontrados
            "route_where_is_report": path_report, # ruta donde esta guardada el archivo de los matches
            "report_file_name": os.path.basename(path_report), # nombre del reporte 
            "used_configuration": setting, # confguracion usada para la busqueda
            "damaged_images": invalid_img_s3 # lista con la direccion de las imagenes que estan dañadas y que no se pudieron reparar
        }
        
        return msm
        
    else:
        print("No se encontraron matches, por lo tanto no se puede generar un reporte.")

        msm: dict = {
            "msm": "No se encontraron matches."
        }
        
        return msm
