import imageio.v2 as imageio
from PIL import Image
import shutil
import datetime 
import os

def delete_directory_content(directory):
    
    try:
        
        for archivo in os.listdir(directory):
            
            ruta_completa = os.path.join(directory, archivo)
            
            if os.path.isfile(ruta_completa):
                os.remove(ruta_completa)
            elif os.path.isdir(ruta_completa):
                shutil.rmtree(ruta_completa)
                
        print(f"El contenido del directory '{directory}' ha sido eliminado correctamente.")
        
    except FileNotFoundError:
        print(f"El directory '{directory}' no existe.")
        
    except Exception as e:
        print(f"Se produjo un error al eliminar el contenido del directory '{directory}': {str(e)}")


def add_metadata(img_path: str = None, metadata: dict = None) -> str:
    
    if not os.path.exists(img_path):
        print(img_path)
        raise ValueError("La Dirección proporcionada no existe o esta mal escrita.")
    
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