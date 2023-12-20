from pydantic import BaseModel
from typing import Union

class Matching_images(BaseModel):
    
    bucket: str
    path_origin_file: str
    path_alternative_file: str
    path_origin_img: str
    path_alternative_img: str
    img_per_object: Union[int, None]

