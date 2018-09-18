
from azure_statoil_walker.azure_statoil_walker import *
from plant import Plant,PlantRegistry
from helper import getDataFolderPath
from azure.datalake.store import core, lib, multithread
from constants import Constants
from helper import getDataFolderPath
import os


def authenticate():
    '''
    two phase authentication
    :return: token
    '''
    return lib.auth()

def load_one_file_example():
    token = authenticate()
    df=read_df_from_azure('dataplatformdlsprod','/raw/corporate/PI%20System%20Manager%20Sleipner/sensordata/1142-SLB/W-21-LT___039_/W-21-LT___039__2013.csv',token)
    print(len(df))


def readTagAndFileURL(plant:Plant):
    token=authenticate()
    for i in walk_and_tag_azure(plant.datalakeBase,Constants.DATA_LAKE_STORE,token):
        filePath=i[0]
        tagName=i[1]
        print(tagName + '->' + filePath)

def find_unique_in_list(filename):
    filepath=os.path.join(getDataFolderPath(),filename)
    with open(filepath) as f:
        list_all = f.readlines()
        list_all = [x.strip() for x in list_all]
        print("before removing: "+str(len(list_all)))
        list_all = list(set(list_all))
        print("after removing: " + str(len(list_all)))
        for x in list_all:
            print(x)

def find_defferene_lists(filename1,filename2):
    '''
    file - file2
    :param filename1:
    :param filename2:
    :return: print the items in first set, but not in the second set.
    '''

    filepath1=os.path.join(getDataFolderPath(),filename1)
    filepath2 = os.path.join(getDataFolderPath(), filename2)
    with open(filepath1) as f:
        list_1 = f.readlines()
        list_1 = [x.strip() for x in list_1]
    with open(filepath2) as f:
        list_2 = f.readlines()
        list_2 = [x.strip() for x in list_2]
    retset=list(set(list_1)-set(list_2))
    retset.sort()
    for x in retset:
        print(x)

if __name__ == '__main__':
    load_one_file_example()
    readTagAndFileURL(PlantRegistry.GRANE)

    print(os.environ['SOME_VAR'])
    find_unique_in_list("GRA_23_CT_0001_Lube_Model")
    find_defferene_lists('GRA_23_CT_0001_available','GRA_23_CT_0001_All_FROM_DB')

