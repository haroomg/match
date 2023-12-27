from pydantic import BaseModel, validator
from typing import Union

class Matching_images(BaseModel):
    
    bucket: str
    path_origin_file: str
    path_alternative_file: str
    path_origin_img: str
    path_alternative_img: str
    path_report: str
    img_per_object: int | None = 0
    setting: dict | None = None
    
    # 3
    # aqui deberia de ir la validacion de los tipos de datos y validar otras cosas 

class Fix_image(BaseModel):
    
    route_img_s3: str
    
    # 3
    # aqui deberia de ir la validacion de los tipos de datos y validar otras cosas 