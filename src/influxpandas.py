'''
This module is used to import/aggregate data from influxdb, transform it into a pandas dataframe and further save it as csv file
'''
from azure_statoil_walker.azure_statoil_walker import *
from plant import Plant,PlantRegistry

from azure.datalake.store import core, lib, multithread
from helper import *
from constants import Constants
from influxdb import DataFrameClient
from influxdb import InfluxDBClient

def query_influx(host, port, database, username, password, query_template, taglist):
    """
    :param host:
    :param port:
    :param username:
    :param password:
    :param database:
    :param querytemplate:
    :param taglist:
    :return:
    """
    client = DataFrameClient(host, port, username, password, database)

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
    query_template = """SELECT mean("Value") AS "%s" FROM "data"."autogen"."data" WHERE time > '2014-01-01' AND "tag" = '%s' GROUP BY time(10m) FILL(null)"""
    taglist = ['GRA-YE  -23-0753Y.PV', 'GRA-UV  -23-0682.PV', 'GRA-TE  -23-0696.PV']

    password=os.environ['INFLUX_PASSWORD']
    result=query_influx(Constants.INFLUX_DB_HOST, Constants.INFLUX_DB_PORT, Constants.INFLUX_DB_NAME,Constants.INFLUX_DB_USER,password,query_template=query_template,taglist=taglist)
    print(result)


if __name__ == '__main__':
    try_query_influx()
