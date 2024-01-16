# Estrutura de los Directorios:

-   .venv:
        Entorno virtual de python.
        #1 status: ignorada.

-   app:
        contiene los scripts de python, osea el proyecto en si.

-   data:
        Contiene los datos que usaremos como prueba o para descragar archivos en el.
        #1 status: ignorada.

-   sandbox:
        Contiene los archivos de .ipynb para ejecutar las pruebas.

-   trash:
        Carpetas o archivos que vamos a guardar para despues borrarlos.
        #1 status: ignorada.

    ## leyenda:
        #1 status: quiere decir que la carpeta o archivo va a ser ignorada por el git y docker

_____________________________________________________________________________________________________________________________________________________________________________

# FatsDup:

        Para poder ejecutar la comparacion de imagenes en el S3. Primero se tiene que descargar y configurar en la distro
        de linux que se esta usando awscli.

                $ sudo apt install awscli
                $ aws configure
                $ aws s3 ls s3://<your bucket name>/
        
        Usa las varibles de entornos que tienes en el .env para poder conectarte al S3.

_____________________________________________________________________________________________________________________________________________________________________________

# FastApi:

        comando para desplegar el servidor:
                $ uvicorn app.main:app --reload
        o tambine se puede usar:
                $ python3 run.py

        # Request: Ejemplo de una request.
<!-- 
        {
                "bucket": "hydrahi4ai",
                "path_origin_file": "ajio-myntra/origin/20231214/New_collector_20231214_154733.success.json",
                "path_alternative_file": "ajio-myntra/alternative/20231219/Myntra__Marianfer_Cruz_20231219_195028.success.json",
                "path_origin_img": "ajio-myntra/origin/20231214/",
                "path_alternative_img": "ajio-myntra/alternative/20231219/",
                "path_report": "ajio-myntra/reports/test2.json",
                "img_per_object": 0,
                
                "setting": {
                        
                        "origin_file_name_imgs": "product_images",
                        "alternative_file_name_imgs": "product_images",
                        
                        "ref_origin": "sku",
                        "ref_alternative": "sku",
                        
                        "origin_search_parameter": {
                        "category": "Footwear",
                        "brand": "bucik",
                        "gender": "Men",
                        "discountPercent": "80% off"
                        },
                        "alternative_search_parameter": {
                        "brand": "BUCIK",
                        "color": ["Brown"]
                        }
                }
        } 
-->

_____________________________________________________________________________________________________________________________________________________________________________

# Mejoras:
        - Hay que adactar el script para que lea los archivos en el s3 de manera directa sin que tenga que descargarlo, todo eso sera en el ec2