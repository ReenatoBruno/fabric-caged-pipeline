import os 
from config.paths import FABRIC_SHORTCUT_PREFIX

# Converte um caminho local de arquivo em um caminho de destino (Lakehouse) 
# simulado para o ambiente Fabric, utilizando um atalho (Shortcut) pré-definido.

def simulate_fabric_path(local_path: str, 
                         shortcut_folder: str = "amzn-caged-bucket-renato/caged/") -> str:
    
    filename = os.path.basename(local_path)
    
    return os.path.join(FABRIC_SHORTCUT_PREFIX, shortcut_folder, filename).replace("\\", "/")

# Bloc de compatibilidade local (stub/Mocking)

class MockFS:
    def cp(self, source, dest, recursive):
        print(f"[MOCK COPY] {source} -> {dest} (recursive={recursive})")

class MockMsSparkUtils:
    fs = MockFS()

mssparkutils = MockMsSparkUtils()
