# -*- coding: utf-8 -*-
"""
Created on Thu Feb 25 23:48:17 2021
Test Bevouac
@author: MEHDI MAAREF
à l'attention de Mr FlorianBreton
"""
""""Module 1"""
#imports 

import csv

import json
import tempfile
import numpy as np
import pandas as pd

# IMPORTING MODULES
import os
import zipfile
import tarfile
import gzip
import shutil
import requests

#extraction du fichier suivant l'url et téléchargement dans le fichier souhaité 
# ARCHIVE EXTENSIONS
ZIP_EXTENSION = ".zip"
TAR_EXTENSION = ".tar"
TAR_GZ_EXTENSION = ".tar.gz"
TGZ_EXTENSION = ".tgz"
GZ_EXTENSION = ".gz"
EMPTY_URL_ERROR = "ERROR: URL should not be empty."
FILENAME_ERROR = "ERROR: Filename should not be empty."
UNKNOWN_FORMAT = "ERROR: Unknown file format. Can't extract."
with tempfile.TemporaryDirectory() as tmpdirname:
        print('created temporary directory', tmpdirname)

#enregistrer le fichier souhaité dans le fichier concerné dans notre cas enregistré dans le fichier de téléchargement
def download_dataset(url, target_path="data/", keep_download=True, overwrite_download=False):
    """Downloads dataset from a url.
    url: string, a dataset path
    target_path: string, path where data will be downloaded
    keep_download: boolean, keeps the original file after extraction
    overwrite_download: boolean, stops download if dataset already exists
    """
    if url == "" or url is None:
        raise Exception(EMPTY_URL_ERROR)

    filename = get_filename(url)
    file_location = get_file_location(target_path, filename)

    os.makedirs(tmpdirname, exist_ok=True) #your downloading target path 

    if os.path.exists(file_location) and not overwrite_download:
        print(f"File already exists at {file_location}. Use: 'overwrite_download=True' to \
overwrite download")
        extract_file(target_path, filename)
        return

    print(f"Downloading file from {url} to {file_location}.")
    # Download
    with open(file_location, 'wb') as f:
        with requests.get(url, allow_redirects=True, stream=True) as resp:
            for chunk in resp.iter_content(chunk_size = 512):  #chunk_size in bytes
                if chunk:
                    f.write(chunk)

    print("Finished downloading.")
    print("Extracting the file now ...")
    extract_file(os.path.join(tmpdirname, '') , filename)

    if not keep_download:
        os.remove(file_location)

def extract_file(target_path, filename):
    """Extract file based on file extension
    target_path: string, location where data will be extracted
    filename: string, name of the file along with extension
    """
    if filename == "" or filename is None:
        raise Exception(FILENAME_ERROR)

    file_location = get_file_location(target_path, filename)

    if filename.endswith(ZIP_EXTENSION):
        print("Extracting zip file...")
        zipf = zipfile.ZipFile(file_location, 'r')
        zipf.extractall(target_path)
        zipf.close()
        print(' E N D')
    elif filename.endswith(TAR_EXTENSION) or \
         filename.endswith(TAR_GZ_EXTENSION) or \
         filename.endswith(TGZ_EXTENSION):
        print("Extracting tar file")
        tarf = tarfile.open(file_location, 'r')
        tarf.extractall(target_path)
        tarf.close()
        print(' E N D')
    elif filename.endswith(GZ_EXTENSION):
        print("Extracting gz file")
        out_file = file_location[:-3]
        with open(file_location, "rb") as f_in:
            with open(out_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
                print(' E N D')
    else:
        print(UNKNOWN_FORMAT)

def get_filename(url):
    """Extract filename from file url"""
    filename = os.path.basename(url)
    return filename

def get_file_location(target_path, filename):
    """ Concatenate download directory and filename"""
    return target_path + filename
#download information and locations of the file (temporary directory)

print('name of file: ',get_filename("https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/2020/full.csv.gz"))
download_dataset("https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/2020/full.csv.gz",os.path.join(tmpdirname, ''),keep_download=True, overwrite_download=False)

pass #pass the  ignoring error message: the python code does not recognize the path, because the file is temporary
#this normally is not recommended, but we use it to overcome the error messages and download the dataset
#properly

#Traitement data 

file_location = get_file_location(os.path.join(tmpdirname, ''), 'full.csv.gz')#obtaining file location
#pandas support zip file reads

#reading file
data_splited = pd.read_csv(file_location)
data_splited #voici le fichier avec ses caractéristiques
print(data_splited.shape, 'shape of data')
#del(data_splited['my_name'])#adding column
data_splited
data_splited['NOM_CANDIDAT'] = 'MAAREF'#adding column with my name 


data_splited = data_splited[['NOM_CANDIDAT','id_mutation', 'date_mutation', 'numero_disposition', 'nature_mutation',
       'valeur_fonciere', 'adresse_numero', 'adresse_suffixe',
       'adresse_nom_voie', 'adresse_code_voie', 'code_postal', 'code_commune',
       'nom_commune', 'code_departement', 'ancien_code_commune',
       'ancien_nom_commune', 'id_parcelle', 'ancien_id_parcelle',
       'numero_volume', 'lot1_numero', 'lot1_surface_carrez', 'lot2_numero',
       'lot2_surface_carrez', 'lot3_numero', 'lot3_surface_carrez',
       'lot4_numero', 'lot4_surface_carrez', 'lot5_numero',
       'lot5_surface_carrez', 'nombre_lots', 'code_type_local', 'type_local',
       'surface_reelle_bati', 'nombre_pieces_principales',
       'code_nature_culture', 'nature_culture', 'code_nature_culture_speciale',
       'nature_culture_speciale', 'surface_terrain', 'longitude', 'latitude',
       ]]#new column NOM_CANDIDAT in the first position

#new data frame with column NOM_CANDIDAT added


adresse = pd.concat([data_splited["adresse_numero"], data_splited["adresse_nom_voie"], data_splited["nom_commune"],data_splited["code_postal"]], axis=1) 
adresse["pays"] = 'FRANCE' #adding the column 'pays'
adresse['code_postal'] = adresse['code_postal'].values.astype(int) #converting postal code to int

#creating a dataframe adresse that we will use to merge the columns 
data_splited['adresse_string'] = adresse[adresse.columns[0:]].apply(
    lambda x: ' '.join(x.dropna().astype(str)),
    axis=1) #merging different columns from the data frame adresse and creating the column 'adresse_string'

#data tranformed with the right form
#NUMERO_RUE NOM_RUE NOM_VILLE CODE_POSTAL PAYS
#delete Nan lines from the dataframe in the columns 'longitudes ' and 'latitudes'

data_splited.dropna(subset=["longitude"], axis=0, inplace=True)
data_splited.dropna(subset=["latitude"], axis=0, inplace=True)

data_splited['valeur_fonciere'] = data_splited['valeur_fonciere'].values.astype(int)
data_splited['longitude'] = data_splited['longitude'].values.astype(float)
data_splited['latitude'] = data_splited['latitude'].values.astype(float)
print(data_splited.shape, '--> new shape after dropping NaN values in columns longitude and latitude')

data_splited.reset_index(drop=True, inplace=True)#reset index with new number of lines 


"""Module 2 - import de données"""

#data_splited['date_mutation'] = pd.to_datetime(data_splited['date_mutation'])
data_splited = data_splited.sort_values('date_mutation')

data_splited.reset_index(drop=True, inplace=True)#reset index with new number of lines 
#data_splited.head(500)#We use head to obtain the 500 recent dates 

"""API Module 2"""





with tempfile.TemporaryDirectory() as tmpdirname2:
    data_splited.head(500).to_csv(os.path.join(tmpdirname2, '')+"Data_module2.csv")
        
    f = open(os.path.join(tmpdirname2, '')+"Data_module2.csv")
    print('make sure io.wrapper is convenient type', type(f))
    
    
    
    post_url = "https://api.airtable.com/v0/appqmv1skloVlLyKV/DVF"
    post_headers = {
        "Authorization" : "Bearer key9SsqMTpaKecsn0",
        "Content-Type": "application/json"
    }
    
    #f = open('C:\\Users\Lenovo\Desktop\Bevouac\Data_module2.csv')
    csv_f = csv.DictReader(f, delimiter=',')
    
    for row in csv_f:
        
        
        data = {
    
      "records": [
        {
          "fields": {
              "NOM_CANDIDAT": row['NOM_CANDIDAT'],
              "id_mutation": row['id_mutation'],
              "date_mutation": row['date_mutation'],
              "numero_disposition": int(row['numero_disposition']),
              "nature_mutation": row['nature_mutation'],
              "valeur_fonciere": int(row['valeur_fonciere']),
              "adresse_numero": row['adresse_numero'],
              "adresse_suffixe": row['adresse_suffixe'],
              "adresse_nom_voie": row['adresse_nom_voie'],
              "adresse_code_voie": row['adresse_code_voie'],
              "code_postal": row['code_postal'],
              "code_commune": row['code_commune'],
              "nom_commune": row['nom_commune'],
              "code_departement": row['code_departement'],
              "ancien_code_commune": row['ancien_code_commune'],
              "ancien_nom_commune": row['ancien_nom_commune'],
              "id_parcelle": row['id_parcelle'],
              "ancien_id_parcelle": row['ancien_id_parcelle'],
              "numero_volume": row['numero_volume'],
              "lot1_numero": row['lot1_numero'],
              "lot1_surface_carrez": row['lot1_surface_carrez'],
              "lot2_numero": row['lot2_numero'],
              "lot2_surface_carrez": row['lot2_surface_carrez'],
              "lot3_numero": row['lot3_numero'],
              "lot3_surface_carrez": row['lot3_surface_carrez'],
              "lot4_numero": row['lot4_numero'],
              "lot4_surface_carrez": row['lot4_surface_carrez'],
              "lot5_numero": row['lot5_numero'],
              "lot5_surface_carrez": row['lot5_surface_carrez'],
              "nombre_lots": row['nombre_lots'],
              "code_type_local": row['code_type_local'],
              "type_local": row['type_local'],
              "surface_reelle_bati": row['surface_reelle_bati'],
              "nombre_pieces_principales": row['nombre_pieces_principales'],
              "code_nature_culture": row['code_nature_culture'],
              "nature_culture": row['nature_culture'],
              "code_nature_culture_speciale": row['code_nature_culture_speciale'],
              "nature_culture_speciale": row['nature_culture_speciale'],
              "surface_terrain": row['surface_terrain'],
              "longitude": float(row['longitude']),
              "latitude": float(row['latitude']),
              "adresse_string": row['adresse_string']
    
                }
    
            },
      ],
    
            
       
    
        
    }
    
    
    
        print(post_url)
        print(data)
    
        post_airtable_request = requests.post(post_url, headers = post_headers, json = data)
        print(post_airtable_request.status_code)


"""Module 3 : Flux de données"""

        
#code of module 3
#print 500 new lines for example

with open("Data_module2.csv", "r") as readfile:
    lines = readfile.readlines()
    
    list_num = len((lines))

with tempfile.TemporaryDirectory() as tmpdirname3:
        new_data = data_splited.loc[list_num:list_num+500,:]#concaténation des lignes inédites + 500 nouvelles lignes
        new_data.to_csv(os.path.join(tmpdirname3, '')+"new_data.csv")
        f2 = open(os.path.join(tmpdirname3, '')+"new_data.csv")
        print('make sure io.wrapper is convenient type', type(f2))
        


        post_url = "https://api.airtable.com/v0/appqmv1skloVlLyKV/DVF"
        post_headers = {
            "Authorization" : "Bearer key9SsqMTpaKecsn0",
            "Content-Type": "application/json"
        }

        #f = open('C:\\Users\Lenovo\Desktop\Bevouac\Data_module2.csv')
        csv_f2 = csv.DictReader(f2, delimiter=',')

        for row in csv_f2:


            data = {

          "records": [
            {
              "fields": {
                  "NOM_CANDIDAT": row['NOM_CANDIDAT'],
                  "id_mutation": row['id_mutation'],
                  "date_mutation": row['date_mutation'],
                  "numero_disposition": int(row['numero_disposition']),
                  "nature_mutation": row['nature_mutation'],
                  "valeur_fonciere": int(row['valeur_fonciere']),
                  "adresse_numero": row['adresse_numero'],
                  "adresse_suffixe": row['adresse_suffixe'],
                  "adresse_nom_voie": row['adresse_nom_voie'],
                  "adresse_code_voie": row['adresse_code_voie'],
                  "code_postal": row['code_postal'],
                  "code_commune": row['code_commune'],
                  "nom_commune": row['nom_commune'],
                  "code_departement": row['code_departement'],
                  "ancien_code_commune": row['ancien_code_commune'],
                  "ancien_nom_commune": row['ancien_nom_commune'],
                  "id_parcelle": row['id_parcelle'],
                  "ancien_id_parcelle": row['ancien_id_parcelle'],
                  "numero_volume": row['numero_volume'],
                  "lot1_numero": row['lot1_numero'],
                  "lot1_surface_carrez": row['lot1_surface_carrez'],
                  "lot2_numero": row['lot2_numero'],
                  "lot2_surface_carrez": row['lot2_surface_carrez'],
                  "lot3_numero": row['lot3_numero'],
                  "lot3_surface_carrez": row['lot3_surface_carrez'],
                  "lot4_numero": row['lot4_numero'],
                  "lot4_surface_carrez": row['lot4_surface_carrez'],
                  "lot5_numero": row['lot5_numero'],
                  "lot5_surface_carrez": row['lot5_surface_carrez'],
                  "nombre_lots": row['nombre_lots'],
                  "code_type_local": row['code_type_local'],
                  "type_local": row['type_local'],
                  "surface_reelle_bati": row['surface_reelle_bati'],
                  "nombre_pieces_principales": row['nombre_pieces_principales'],
                  "code_nature_culture": row['code_nature_culture'],
                  "nature_culture": row['nature_culture'],
                  "code_nature_culture_speciale": row['code_nature_culture_speciale'],
                  "nature_culture_speciale": row['nature_culture_speciale'],
                  "surface_terrain": row['surface_terrain'],
                  "longitude": float(row['longitude']),
                  "latitude": float(row['latitude']),
                  "adresse_string": row['adresse_string']

                    }

                },
          ],





        }



            print(post_url)
            print(data)

            post_airtable_request = requests.post(post_url, headers = post_headers, json = data)
            print(post_airtable_request.status_code)
            
        
print('Extraction completed in API --> 1000 lines')













