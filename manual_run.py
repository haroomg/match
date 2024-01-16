# Esta es una forma de correr la api de manera local

from app.functions import add_metadata, delete_directory_content, search_parameter, assign_reference_to_image
from app.schemas import Matching_images
from app.s3 import S3
import pandas as pd
import fastdup
import shutil
import json
import os

review_path = lambda path : path if path.endswith("/") else path + "/"

def matching_images(respose: dict = None):
    
    bucket = respose["bucket"]
    path_origin_file = respose["path_origin_file"]
    path_alternative_file = respose["path_alternative_file"]
    path_origin_img =  respose["path_origin_img"]
    path_alternative_img = respose["path_alternative_img"]
    path_report = respose["path_report"]
    img_per_object = respose["img_per_object"]
    setting = respose["setting"]
    
    s3 = S3(bucket=bucket)
    
    # finalización de la parte de validación de data
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------
    
    # files fastdup
    WORK_DIR: str = "trash/fastdup/"
    files_in_trash_fastdup = os.listdir(WORK_DIR)
    
    if len(files_in_trash_fastdup):
        direcction_fastdup_name =str(int(files_in_trash_fastdup.pop()) + 1)
    else:
        direcction_fastdup_name = "1"
    
    path_fastdup = os.path.join(WORK_DIR, direcction_fastdup_name)
    os.makedirs(path_fastdup)
    
    origin_field_name_images: str = setting["origin_file_name_imgs"]
    alternative_field_name_images: str = setting["alternative_file_name_imgs"]
    
    ref_origin_field_name: str = setting["ref_origin"]
    ref_alternative_field_name: str = setting["ref_alternative"]
        
    # descargamos los archivos del origin y el aternative y lo pasamos a df y despues borramos el archivo
    
    # ___________________ data_start : origin _________________________________#
    #3 no necesariamente tiene que ser un df o si pero hay que iterarlo
    # origin
    
    with open(path_origin_file, "r", encoding="utf8") as file_origin:

        if "origin_search_parameter" in setting:
            
            if len(setting["origin_search_parameter"]):
                #3 Cambiar
                origin_object: list = search_parameter(
                    json.load(file_origin),
                    setting["origin_search_parameter"]
                )
                
            else:
                origin_object: list = json.load(file_origin)
        else: 
            origin_object: list = json.load(file_origin)
        
    # una vez obtenido el origin_object, asignamos cada una de las imagenes a su punto de referencia
    origin_img_with_ref: dict = assign_reference_to_image(origin_object, origin_field_name_images, ref_origin_field_name)
    
    # abquirimos el nombre de los archivos del json de origin
    files_img_origin: list = [images[origin_field_name_images] for images in origin_object]
    
    # una vez obtenido el nombre de los archivo eliminamos la variable de origin_object
    del origin_object
    
    # ___________________ data_end  _________________________________#
    
    # ______________________________________________________________________________#
    
    # ___________________ data_start : alternative _________________________________#
    #3 no necesariamente tiene que ser un df o si pero hay que iterarlo
    # alternative
    
    with open(path_alternative_file, "r", encoding="utf8") as file_aternative:

        if "alternative_search_parameter" in setting:
            
            if len(setting["alternative_search_parameter"]):
                #3 cambiar
                alternative_object: list = search_parameter(
                    json.load(file_aternative),
                    setting["alternative_search_parameter"]
                )
            else:
                alternative_object: list = json.load(file_aternative)
        else:
            alternative_object: list = json.load(file_aternative)
    
    # una vez obtenido el alternative_object, asignamos cada una de las imagenes a su punto de referencia
    alternative_img_with_ref: dict = assign_reference_to_image(alternative_object, alternative_field_name_images, ref_alternative_field_name)
    
    # abquirimos el nombre de los archivos del json de alternative
    files_img_alternative: list = [images[alternative_field_name_images] for images in alternative_object]
    
    # una vez obtenido el nombre de los archivo eliminamos la variable de alternative_object
    del alternative_object
    
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
    
    # eliminamos el espacio en memoria que ocupa files_img_origin y files_img_alternative
    del files_img_origin, files_img_alternative
    
    # la lista de imagenes que vamos a analizar lo pasamos a txt para que fastdup las procese
    path_files_s3: str = os.path.join(path_fastdup, "address_files_s3.txt")
    with open(path_files_s3, "w", encoding="utf8") as file:
        for path in input_dir:
            file.write(path)
    
    # ejecutamos el analisis de fastdup
    fd = fastdup.create(path_fastdup)
    fd.run(path_files_s3, threshold= 0.5, overwrite= True, high_accuracy= True)
    
    # Validamos si existen imagines corruptas
    fastdup_path: str = os.path.join(path_fastdup, "tmp")
    path_bucket: str = f"s3://{bucket}/"
    
    invalid_img_s3: list = [os.path.join(fastdup_path, path.replace(path_bucket, "")) for path in fd.invalid_instances()["filename"].to_list()]
    
    # si hay una imagen dañada, la arreglamos y ejecutamos el analisis de nuevo de manera local
    if len(invalid_img_s3): 
        
        for img_path in invalid_img_s3:
            # acomodamos las imagenes que no contienen metadata
            add_metadata(img_path)
        
        # Extraemos el las direccion de las imagenes que se descargaron de manera local
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
    
    #3 borramos la carpeta creada temporalmente en el fastdup
    shutil.rmtree(path_fastdup)
    
    # creamos un archivo json donde guardaremos el match
    if len(matches):
        
        local_report_path: str = os.path.join("trash/reports/", os.path.basename(path_report))
        
        report: dict = {
            "request": respose,
            "matches": matches
        }
        
        with open(local_report_path, "w", encoding="utf8") as file:
            json.dump(report, file, indent=4)
        
        path_report_copy = review_path("/".join(path_report.split("/")[:-1]))

        # subimos el reporte al s3
        s3.upload_file(local_report_path, path_report_copy)

        # borramos el archivo local 
        os.remove(local_report_path)
        
        print(f"Se encontraron {len(matches)} y se caban de guardar en la direccion:\n{path_report}")

    else:
        print("No se encontraron matches, por lo tanto no se puede generar un reporte.")

response = {
    "bucket": "hydrahi4ai",

    "path_origin_file": "data/json/New_collector_20240112_172137.success.json",
    "path_alternative_file": "data/json/Myntra__Marianfer_Cruz_20240111_095149.success.json",

    "path_origin_img": "ajio-myntra/origin/20240112/",
    "path_alternative_img": "ajio-myntra/alternative/20240110/",

    "path_report": "ajio-myntra/reports/matchin_ajio_myntra_1.json",

    "img_per_object": 2,
    
    "setting": {
        "origin_file_name_imgs": "product_images",
        "alternative_file_name_imgs": "product_images",
        
        "ref_origin": "sku",
        "ref_alternative": "sku",
        
        # "origin_search_parameter": {
        # "brand": "U.S. POLO ASSN"
        # },
        # "alternative_search_parameter": {
        # "brand": "U.S. POLO ASSN"
        # }
    }
}

matching_images(respose=response)