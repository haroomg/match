from pydantic import BaseModel, validator
from typing import Union

class Matching_images(BaseModel):
    
    bucket: str
    path_origin_file: str
    path_alternative_file: str
    path_origin_img: str
    path_alternative_img: str
    path_report: str
    img_per_object: Union[int, None] = 0
    setting: Union[dict, None] = {
        "file_name_imgs": "",
        "ref_origin": "sku",
        "ref_alternative": "sku"
    }
    
    # 3
    # aqui deberia de ir la validacion de los tipos de datos y validar otras cosas 