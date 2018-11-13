'''
This module is used to fetch data fra azure data lake and store it in a local file folder
'''
import json
import logging
import timeit

import multiprocessing
from azure.datalake.store import lib
from azure.datalake.store.lib import DataLakeCredential

from azure_statoil_walker import read_df_from_azure, walk_and_tag_azure
from helper import *
from plant import PlantRegistry
from azure.datalake.store import core
import azure.datalake
import contextlib

logger = multiprocessing.log_to_stderr()

logger.setLevel(logging.WARNING)


def main():

    '''
    This part define the parameters necessary to run the code
    :return:
    '''

    azure_data_store = Constants.DATA_LAKE_STORE

    base_path = PlantRegistry.GRANE.datalakeBase

    #token_cache='token_catch'
    token_cache = None

    taglist='GRA_23_CT_0001'

    target_root_folder=getDataFolderPath()

    years = range(2000, 2019)

    parallelization=16


    ''' following is the code to do the work'''

    pool = multiprocessing.Pool(parallelization)
    before = timeit.default_timer()


    if token_cache:
        try:
            logger.debug("Attempting to open token cache %s" % token_cache)
            with open(token_cache, 'r') as f:
                token_dict = json.load(f)
                token = DataLakeCredential(token_dict)
                logger.debug("Token cache read from %s" % token_cache)
        except (IOError, ValueError):
            logger.debug("Error when opening token cache, creating new token")
            token = lib.auth()
            token_dict = token.token
            with open(token_cache, 'w') as f:
                json.dump(token_dict, f)
    else:
        token = lib.auth()

    if taglist:
        if taglist:
            logger.debug("Attempting to parse tags from the taglist %s" % taglist)
            filename=taglist
            filename=os.path.join(getConfFolderPath(),filename)
            with open(filename) as f:
                tag_list = f.readlines()
                tag_list = [x.strip() for x in tag_list]
        generator = walk_tags(base_path, tag_list, years)
    else:  # Crawling
        generator = walk_and_tag_azure(base_path, azure_data_store, token, '.csv')

    for tag, file_name, file_path in generator:
        pool.apply_async(download_save,
                             (token, azure_data_store, tag, file_name, file_path, target_root_folder))
        #azure_fetch_and_push_to_influx(token, azure_data_store, file_path, measurement_tags, influx_settings)

    pool.close()
    pool.join()

    after = timeit.default_timer()
    logger.debug("Processing took %s seconds" % (after - before))

def download_save(token, azure_data_store, tag_name, file_name, file_path, target_root_folder):
    """ Reads the file in file_path from the azure datalake azure_data_store using access token token,
            attempts to read it as a csv into panda. Expects the csv to have 4 columns of types String, Number, Timestamp,
            and Number. Only the timestamp and number is used, in a timestamp -> number mapping. The resulting DataFrame
            is returned
            """
    adls_file_system_client = core.AzureDLFileSystem(token, store_name=azure_data_store)
    try:
        logger.debug("Attempting to open file {} on azure_data_store {}".format(file_path, azure_data_store))
        with adls_file_system_client.open(file_path, 'rb') as f:
            print("Parsing file {}".format(file_path))
            local_folder_path=os.path.join(target_root_folder,tag_name)
            local_file_path = os.path.join(local_folder_path, file_name)
            contents=f.read()
            if not os.path.exists(local_folder_path):
                os.makedirs(local_folder_path)
            with contextlib.suppress(FileNotFoundError):
                os.remove(local_file_path)
            with open(local_file_path,'w+') as local_file:
                local_file.write(contents)

    except azure.datalake.store.exceptions.FileNotFoundError:
        print("Azure File not found: %s" % file_path)
        logger.warning("Azure File not found: %s" % file_path)

def walk_tags(base_path, tag_list, years):
    '''
    generate the filepath list
    :param base_path: azure datalake plant base path
    :param tag_list: ims tag list
    :param years: years to generate the data file path
    :return:
    '''
    for tag in tag_list:
        for year in years:
            file_name = "{}_{}.csv".format(tag, year)
            file_path = base_path + "/{}/{}".format(tag, file_name)

            yield tag, file_name,file_path

if __name__ == '__main__':
    main()
