from app.functions import search_parameter_fast
from app.main2 import matching_images
from dotenv import load_dotenv
from app.s3 import S3
import shutil
import json
import os 

with open("data/json/brads.json", "r", encoding="utf-8") as file:
    brands = json.load(file)

load_dotenv(".env")
s3 = S3(os.environ.get("AWS_BUCKET_NAME"))

response = {
    "bucket": "hydrahi4ai",

    "path_origin_file": "ajio-myntra/origin/20240112/New_collector_20240112_172137.success.json",
    "path_alternative_file": "ajio-myntra/alternative/20240110/Myntra__Marianfer_Cruz_20240111_095149.success.json",

    "path_origin_img": "ajio-myntra/origin/20240112/",
    "path_alternative_img": "ajio-myntra/alternative/20240110/",

    "path_report": "ajio-myntra/reports/matchin_ajio_myntra_1.json",

    "img_per_object": 0,
    
    "setting": {
        "origin_file_name_imgs": "product_images",
        "alternative_file_name_imgs": "product_images",
        
        "ref_origin": "sku",
        "ref_alternative": "sku",
        
        "origin_search_parameter": {
            "brand": ""
        },
        "alternative_search_parameter": {
            "brand": ""
        }
    }
}

response_copy = response.copy()


for obj in brands:

    ajio_brand = obj["ajio_brand"]
    myntra_brand = obj["myntra_brand"]

    response["setting"]["origin_search_parameter"]["brand"] = ajio_brand
    response["setting"]["alternative_search_parameter"]["brand"] = myntra_brand

    response["path_report"] = f"ajio-myntra/reports/matchin_ajio_myntra_brand_{ajio_brand}.json"

    is_valid = []
    paths = [response["path_origin_file"], response["path_alternative_file"]]
    parameters = [response["setting"]["origin_search_parameter"], response["setting"]["alternative_search_parameter"]]

    for i, path_s3 in enumerate([response["path_origin_file"], response["path_alternative_file"]]):
        path_file = s3.download_file("trash/s3/", path_s3)
        is_valid.append(search_parameter_fast(path_file, parameters[i]))
        os.remove(path_file)


    if all(is_valid):
        matching_images(response)
    else:
        print("error con los inputs")

    for name in ["fastdup", "img", "reports", "s3", "db"]:
        path = f"trash/{name}"
        shutil.rmtree(path)
        os.makedirs(path)


