from dotenv import load_dotenv
from typing import Union
import boto3
import os

class S3():
    
    def __init__(self, bucket: str = None) -> None:
        
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
        
        # si no ingresa el bucket quiere decir que lo debemos buscar en el .env
        if bucket == None:
            bucket = os.environ.get("AWS_BUCKET_NAME")
            
            if not bucket:
                raise ValueError("No hay un env que contenga el nombre del bucket.") 
        
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
    ) -> str:
        
        file_s3_route = file_s3_route.replace(self.bucket, "") if file_s3_route.startswith(self.bucket) else file_s3_route
        file_name_s3: str = os.path.basename(file_s3_route)
        local_route += file_name_s3 if local_route.endswith("/") else f"/{file_name_s3}"
        
        try:
            self.s3.download_file(
                self.bucket, 
                file_s3_route,
                local_route
                )
            
            # print(f"El archivo {file_name_s3} acaba de ser descargado en la ruta {local_route}")
            
            return local_route
        
        except ValueError as e:
            print(e)


    def upload_file(
        self, 
        local_route: Union[str, list] = None, 
        s3_route: str = None
        ) -> None:
        
        if isinstance(local_route, str):
            local_route = [local_route]
        
        if isinstance(s3_route, str):
            s3_route = [s3_route]
        
        for local_file, s3_file in zip(local_route, s3_route):
            
            s3_file = s3_file.replace(self.bucket, "") if s3_file.startswith(self.bucket) else s3_file
            file_name = os.path.basename(local_file)

            try:
                self.s3.upload_file(
                    local_file,
                    self.bucket,
                    os.path.join(s3_file, file_name)
                    )
                
                print(f"El archivo {file_name} se ha subido en s3://{self.bucket}/{s3_file}")
                
                return
            
            except ValueError as e:
                print(e)


    def valid_route(
        self,
        s3_route: Union[str, list] = None
        ) -> tuple:
        
        """Sirve para validar un archivo en el s3."""
        
        if isinstance(s3_route, str):
            s3_route = [s3_route]
        
        all_correct = []
        
        for route in s3_route:
            
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=route)
            
            if "Contents" in response:
                print(f"La ruta {route} existe.")
                all_correct.append(True)
            else:
                print(f"La ruta {route} no existe o esta mal escrita")
                all_correct.append(False)
        
        if all(all_correct):
            return True, None
        else:
            error_route = [route for route, correct in zip(s3_route, all_correct) if not correct]
            print("Las siguientes rutas no existen o están mal escritas:")
            for route in error_route:
                print(route)
            
            return False, error_route


    def valid_file(
        self,
        s3_route_file: Union[str, list] = None
        ) -> tuple:
        
        if isinstance(s3_route_file, str):
            s3_route_file = [s3_route_file]
        
        """Sirve para validar si una ruta en el s3."""
        
        all_correct: list = []
        
        for route in s3_route_file:
            
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix= route)
            
            if "Contents" in response:
                print(f"La ruta '{route}' existe.")
                all_correct.append(True)
            else:
                # print(f"La ruta '{route}' no existe o esta mal escrita")
                all_correct.append(False)
    
        if all(all_correct):
            return True, None
        else:
            error_route = [route for route, correct in zip(s3_route_file, all_correct) if not correct]
            print("Las siguientes rutas no existen o están mal escritas:")
            for route in error_route:
                print(route)
                
            return False, error_route


    def update_file(
        self,
        s3_file: Union[str, list] = None,
        local_file: Union[str, list] = None
    ) -> None:
        
        if isinstance(s3_file, str):
            s3_file = [s3_file]
        
        if isinstance(local_file, str):
            local_file = [local_file]
        
        if len(s3_file) != len(local_file):
            raise ValueError("El len de ambos de s3_file y local_file deben ser iguales ")
        
        print(f"{len(s3_file)} van a ser actualizados en el bucket {self.bucket}")
        
        for s3_route, local_route in zip(s3_file, local_file):
            
            s3_route = s3_route.replace(self.bucket, "") if s3_route.startswith(self.bucket) else s3_route
            file_name = os.path.basename(local_route)
            
            try:
                self.s3.upload_file(
                    local_route, 
                    self.bucket, 
                    s3_route
                    )
            
                print(f"El archivo {file_name} acaba de ser actualizado.")
            
            except TypeError as e:
                print(e)


    def delete_file(
        self, 
        s3_path_file
    ) -> None:
        
        # Te la debo
        pass
# falta acomodar algunos errores que son MUCHOSSSSSS pero muy mucho errores
