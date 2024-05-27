"""Create datalake in main directory"""

import os

STRUCTURE_FILE = 'src/1_datalake/datalake_structure.txt'

def get_datalake_dirs():
    """Returns datalake directories stored in the file structure.txt"""
    
    if not os.path.exists(STRUCTURE_FILE):
        raise FileNotFoundError(f"File {STRUCTURE_FILE} not found")
    
    with open(STRUCTURE_FILE, 'r') as f:
        dirs = [dir.strip() for dir in f]
    return dirs

def create_datalake(dirs):
    """Creates datalake in main directory"""
    
    for path in dirs:
        if not os.path.exists(path):
            os.makedirs(path)

def main():
    """Orquestar la creacion del datalake"""
    
    dirs = get_datalake_dirs()
    create_datalake(dirs)

if __name__ == "__main__":
    main()
