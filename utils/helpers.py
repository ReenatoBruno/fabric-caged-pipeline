import os 

from config.paths import FABRIC_SHORTCUT_PREFIX


def simulate_fabric_path(local_path: str, 
                         shortcut_folder: str = "amzn-caged-bucket-renato/caged/") -> str:
    
    filename = os.path.basename(local_path)
    
    return os.path.join(FABRIC_SHORTCUT_PREFIX, shortcut_folder, filename).replace("\\", "/")