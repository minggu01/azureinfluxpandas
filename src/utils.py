
from azure_statoil_walker.azure_statoil_walker import *
from plant import Plant,PlantRegistry

from azure.datalake.store import core, lib, multithread
from helper import Helper
from constants import Constants
from influxdb import DataFrameClient
from influxdb import InfluxDBClient


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
    for i in walk_and_tag_azure(plant.getDatalakeBase(),Constants.DATA_LAKE_STORE,token):
        filePath=i[0]
        tagName=i[1]
        print(tagName + '->' + filePath)

def query_influx(password,database,query_template,taglist,serverurl='localhost',port='8086',username='admin'):
    """
    :param serverurl:
    :param port:
    :param username:
    :param password:
    :param database:
    :param querytemplate:
    :param taglist:
    :return:
    """
    client = DataFrameClient(serverurl, port, username, password, database)

    total_df=None
    for tagname in taglist:
        query = query_template % (tagname,tagname)
        #print(query)
        _single_result=client.query(query)
        _single_df=_single_result['data']
        if _single_df is not None:
            if total_df is None:
                total_df=_single_df
            else:
                total_df=pd.concat([total_df,_single_df],axis=1)
    return total_df

def try_query_influx():
    query_template = """SELECT mean("Value") AS "%s" FROM "data"."autogen"."data" WHERE time > '2018-08-01' AND "tag" = '%s' GROUP BY time(10m) FILL(null)"""
    taglist = ['GRA-YE  -23-0753Y.PV', 'GRA-UV  -23-0682.PV', 'GRA-TE  -23-0696.PV']

    result=query_influx(password='admin',database='data',query_template=query_template,taglist=taglist)
    print(result)


if __name__ == '__main__':
    #load_one_file_example()
    #print(getPlantDLFolder(PlantEnum.GRANE))
    #readTagAndFileURL(PlantRegistry.GRA)sn
    try_query_influx()
