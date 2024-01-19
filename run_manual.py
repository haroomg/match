from app.main2 import matching_images
import json

with open("data/json/brads.json", "r", encoding="utf-8") as file:
    brands = json.load(file)


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


# si da error usa este comando
import shutil
import os
for name in ["fastdup", "img", "reports", "s3", "db"]:
    path = f"trash/{name}"
    shutil.rmtree(path)
    os.makedirs(path)


for obj in brands:

    ajio_brand = obj["ajio_brand"]
    myntra_brand = obj["myntra_brand"]

    response["setting"]["origin_search_parameter"]["brand"] = ajio_brand
    response["setting"]["alternative_search_parameter"]["brand"] = myntra_brand

    response["path_report"] = f"ajio-myntra/reports/matchin_ajio_myntra_brand_{ajio_brand}.json"

    matching_images(response)
    response = response_copy


