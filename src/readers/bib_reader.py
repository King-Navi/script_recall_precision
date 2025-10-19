import bibtexparser
import os
from ..const.constants import SCIENCEDIRECT_FOLDER_ABS
"""
Reader for BibTeX
"""


def read_dir(dirpath = SCIENCEDIRECT_FOLDER_ABS, file_extension:str = "bib")-> list:
    files_list = []
    for filename in os.listdir(dirpath):
        if filename.endswith(file_extension):
            file_path = os.path.join(dirpath, filename)
            files_list.append(file_path)
            with open(file_path, 'r') as f:
                content = f.read()
                content = read(file_path)
                print(f"Content of {filename}:\n{content}\n---")


def read(file_bib_path):
    with open(file_bib_path, encoding="utf-8") as bibfile:
        bib_database = bibtexparser.load(bibfile)

    for entry in bib_database.entries[:]:
        print("\n🔹 ID:", entry.get("ID"))
        print("   Tipo:", entry.get("ENTRYTYPE"))
        print("   Título:", entry.get("title"))
        print("   Autores:", entry.get("author"))
        print("   Año:", entry.get("year"))

