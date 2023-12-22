from dotenv import load_dotenv
import boto3
import os

class S3():
    
    def __init__(self, bucket) -> None:
        
        
        # Obtenemos los env
        load_dotenv("../.env")

        # Crea una instancia del cliente de S3
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
            )
        
        # validamos la instancia de s3
        try:
            self.buckets: list = self.get_buckets()
        except:
            raise ValueError("Error en la coneccion al s3, revisa que las credenciales esten bien escritas")
        
        if bucket in self.buckets:
            self.bucket = bucket
        else:
            raise ValueError(f"El {bucket} no existe o esta mal escrito")


    def get_buckets(self) -> list:
        
        return [bucket["Name"] for bucket in self.s3.list_buckets()["Buckets"]]


    def download_file(
        self,
        local_route: str = None,
        file_s3_route: str = None
    ) -> None:
        
        file_s3_route = file_s3_route.replace(self.bucket, "") if file_s3_route.startswith(self.bucket) else file_s3_route
        file_name_s3: str = file_s3_route.split("/")
        local_route += file_name_s3 if local_route.endswith("/") else f"/{file_name_s3}"
        
        try:
            self.s3.download_file(
                self.bucket, 
                file_s3_route,
                local_route
                )
            
            print(f"El archivo {file_name_s3} acaba de ser descargado en la ruta {local_route}")
            
            return 
        except ValueError as e:
            print(e)


    def upload_file(
        self, 
        local_route: str = None, 
        s3_route: str = None
        ) -> None:
        
        file_name_local = local_route.split("/")[-1]
        s3_route = s3_route.replace(self.bucket, "") if s3_route.startswith(self.bucket) else s3_route
        s3_route += file_name_local if s3_route.endswith("/") else f"/{file_name_local}"
        
        try:
            self.s3.upload_file(
                local_route,
                self.bucket,
                s3_route
                )
            
            print(f"El archivo {file_name_local} se ha subido en s3://{self.bucket}/{s3_route}")
            
            return
        
        except ValueError as e:
            print(e)


    def valid_route(
        self,
        s3_route: str = None
        ) -> bool:
        
        
        """Sirve para validar si una ruta or archivo existe en el s3."""
        
        if not s3_route.endswith("/"):
            
            print("La ruta ingresado no contiene '/' al final de esta, ingresalo para que no de error.")
            s3_route += "/"
        
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=s3_route)
        
        if "Contents" in response:
            print("La ruta existe.")
            return True
        else:
            print("La ruta no existe o esta mal escrita")
            return False


