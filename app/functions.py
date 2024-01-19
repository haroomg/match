import imageio.v2 as imageio
from typing import Union
from PIL import Image
import pandas as pd
import datetime 
import requests
import shutil
import sys
import os


def download_image(url: str = None, save_path: str = None) -> str:
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"La imagen se ha descargado y guardado en: {save_path}")
        
        return save_path
        
    except requests.exceptions.RequestException as e:
        print(f"No se pudo descargar la imagen: {e}")


def copy_file(file_path: Union[str, list] = None, destiny: str = None) -> None:
    
    if isinstance(file_path, str):
        file_path = [file_path]
    
    for archivo in file_path:
        shutil.copy(archivo, destiny)


def delete_directory_content(directory: str = None) -> str:
    
    try:
        
        for archivo in os.listdir(directory):
            
            ruta_completa = os.path.join(directory, archivo)
            
            if os.path.isfile(ruta_completa):
                os.remove(ruta_completa)
            elif os.path.isdir(ruta_completa):
                shutil.rmtree(ruta_completa)
                
        print(f"El contenido del directory '{directory}' ha sido eliminado correctamente.")
        
        return directory
        
    except FileNotFoundError:
        print(f"El directory '{directory}' no existe.")
        
    except Exception as e:
        print(f"Se produjo un error al eliminar el contenido del directory '{directory}': {str(e)}")


def add_metadata(img_path: str = None, metadata: dict = None) -> str:
    
    if not os.path.exists(img_path):
        raise ValueError(f"La Dirección proporcionada no existe o esta mal escrita:\n{img_path}")
    
    if not metadata:
        # Si el usario no define una metadata, nosotros agregamos
        metadata = {
            "msm": "Esta imagen no contenia Metadata",
            "repair_day": datetime.date.today()
        }
    elif not isinstance(metadata, dict):
        raise ValueError(f"El atributo de Metadata debe ser de tipo dict, no de tipo {type(metadata).__name__}")
    
    try:
        file_name = os.path.basename(img_path)
        
        image = imageio.imread(img_path)
        
        # re-escribimos la imagen con la metadata agregada
        imageio.imwrite(img_path, image, **metadata)
        print(f"Se acaba de agregar la metadata en el archivo {file_name}")
        
    except TypeError as e:
        print(e)
        
    finally:
        return img_path


def search_parameter(json_path: str = None, parameter: dict = None) -> None:
    
    df = pd.read_json(json_path)
    
    df_columns = df.columns
    
    # validamos que el parametro este en el df, en caso de que no lo quitamos
    for key in parameter.copy():
        if key not in df_columns:
            
            print(f"{key} no se encuentra en el df.")
            del parameter[key]
    
    if not len(parameter):
        print("Ninguno de los parámetros proporcionados se encuentran en el df por lo tanto lo retornamos como estaba.")
        df.to_json(json_path, orient="records")
        return 
    
    # validamos los valores de busqueda
    for key,value in parameter.copy().items():
        
        if isinstance(value, (str, int, float, bool)):
            
            search = df[ df[key] == value]
            if not len(search):
                print(f"No se encontro en valor '{value}' en la columna '{key}' por lo tanto no se va a buscar.")
                del parameter[key]
        
        elif isinstance(value, list):
            
            search = df[df[key].isin(value)]
            if not len(search):
                print(f"No se encontro los valores '{value}' en la columna '{key}' por lo tanto no se va a buscar.")
                del parameter[key]
        
        elif isinstance(value, dict):
            print(f"No se aceptan parametros de tipo {type(value).__name__}")
            del parameter[key]
    
    if not len(parameter):
        print("Ninguno de los parámetros proporcionados se encuentran en el df por lo tanto lo retornamos como estaba.")
        df.to_json()
    
    # si todo sale bien empezamos a realizar las busquedas en el df
    
    for key, value in parameter.items():
        
        if isinstance(value, (str, int, float, bool)):
            
            df = df[ df[key] == value]
        
        elif isinstance(value, list):
            
            df = df[df[key].isin(value)]
    
    # Reacomodamos el json con los nuevos datos filtrados
    df.to_json(json_path, orient="records")
    return 


#2 se puede modificar para que no sea un df sino solo un object
def assign_reference_to_image(df: pd.DataFrame = None, list_images_name: str = None, ref_name: str = None) -> pd.DataFrame:
    
    img_with_ref = []
    
    for _, row in df.iterrows():
        
        ref: any = row[ref_name]
        imges_list: list = row[list_images_name]
        
        for img_name in imges_list:
            
            img_with_ref.append(
                {
                    "file_name": img_name,
                    "ref": ref
                }
            )
    
    df_images = pd.DataFrame(img_with_ref)
    
    return df_images


def getsize_var(var: any = None) -> float:
    """Retorna el peso de una variable en Mb"""
    return sys.getsizeof(var) / (1024**2)


def getsize_file(path: str = None) -> float:
    """Retorna el peso de un archivo en Mb"""
    return os.path.getsize(path) / (1024**2)