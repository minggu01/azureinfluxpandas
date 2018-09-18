'''
This file is used to fetch data fra azure data lake to influx db
'''
import argparse
import json
import logging
import timeit

import multiprocessing
import os
from azure.datalake.store import lib
from azure.datalake.store.lib import DataLakeCredential
from influxdb import DataFrameClient

from azure_statoil_walker.azure_statoil_walker import read_df_from_azure, walk_and_tag_azure
from constants import Constants

from helper import getDataFolderPath
from dbSchema import DBSchema
logger = multiprocessing.log_to_stderr()
logger.setLevel(logging.WARN)


def write_to_influx(df, tags, host, port, user, password, db_name, batch_size=10000, time_precision='s'):
    """ Writes a data-frame with tags to influx, with time_precision=s. """
    print("Write DataFrame with Tags {}, with length: {}".format(tags, len(df)))
    client = DataFrameClient(host, port, user, password, db_name)
    if not client.write_points(df, db_name, tags, time_precision=time_precision, batch_size=batch_size):
        logger.error("Writing to influx failed for tags: {}".format(tags))


def azure_fetch_and_push_to_influx(token, azure_data_store, file_path, tag_map, influx_settings):
    """ Reads the file in file_path from the azure datalake azure_data_store using access token token,
     attempts to read it as a csv into panda. Expects the csv to have 4 columns of types String, Number, Timestamp,
     and Number. Only the timestamp and number is used, in a timestamp -> number mapping. This is written to the
     influxdb provided in influx_settings. influx_settings is expected to be on the format
     (host, port, user, password, db_name, batch_size).
     """
    df = read_df_from_azure(azure_data_store, file_path, token)
    (host, port, user, password, db_name, batch_size) = influx_settings
    write_to_influx(df, tag_map, host, port, user, password, db_name, batch_size)


def parse_args():
    influxAdminPass=os.environ['INFLUX_ADMIN_PASSWORD']
    tagListFile="GRA_23_CT_0001"
    starting_year="2010"
    ending_year="2019"

    """Parse the args from main."""
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--debug', required=False, action='store_true',
                        help='Run in debug mode (turns on extra logging)')
    parser.add_argument('--host', type=str, required=False,
                        default=Constants.INFLUX_DB_HOST,
                        help='hostname of InfluxDB http API')
    parser.add_argument('--dbname', type=str, required=False,
                        default=Constants.INFLUX_DB_NAME,
                        help='Influxdb database name')
    parser.add_argument('--port', type=int, required=False, default=Constants.INFLUX_DB_PORT,
                        help='port of InfluxDB http API')
    parser.add_argument('--user', type=str, required=False, default=Constants.WORKING_PLANT,
                        help='Username for influxdb')
    parser.add_argument('--password', type=str, required=False, default=influxAdminPass,
                        help='Password for influxdb')
    parser.add_argument('--batch-size', type=int, required=False, default=10000,
                        help='Batch size towards influxdb')
    parser.add_argument('--taglist', type=str, required=False, default=tagListFile,
                        help='File with list of tags to download. If false do recursive downloads of all')
    parser.add_argument('--from-year', type=int, required=False, default=starting_year,
                        help='Lower bound of year to include from taglist')
    parser.add_argument('--to-year', type=int, required=False, default=ending_year,
                        help='Upper bound of year to include from taglist')
    parser.add_argument('--include', type=str, required=False, default=".*",
                        help='Regexp of files to include when doing recursive download')
    parser.add_argument('--exclude', type=str, required=False, default="a^",
                        help='Regexp of files to exclude when doing recursive download')
    parser.add_argument('--base-path', type=str, required=False,
                        default=Constants.WORKING_PLANT.datalakeBase,
                        help='Base path of either recursive search or tags')
    parser.add_argument('--data_store_name', type=str, required=False,
                        default=Constants.DATA_LAKE_STORE,
                        help='Azure data store name')
    parser.add_argument('--para', type=int, required=False, default=16,
                        help='Level of parallelization to use')
    parser.add_argument('--token-cache', type=str, required=False, default=None,
                        help='File to cache the azure token in. If it exists, use it as a token, '
                             'otherwise store token in it.')
    return parser.parse_args()

def main():
    args = parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    parallelization = args.para
    base_path = args.base_path

    years = range(args.from_year, args.to_year)

    azure_data_store = args.data_store_name

    host = args.host
    port = args.port
    user = args.user
    password = args.password
    db_name = args.dbname
    batch_size = args.batch_size

    # Disable the Statoil proxy
    os.environ['NO_PROXY'] = host

    influx_settings = (host, port, user, password, db_name, batch_size)

    pool = multiprocessing.Pool(parallelization)

    before = timeit.default_timer()

    tag_map = dict() # A map from a Statoil tag (name of a measurement) to the influx-tags for it (e.g. well-name etc)

    if True:  # Azure stuff
        crawling = not (args.taglist or args.tagframe)  # Will we be crawling for tags, or read them from file
        if crawling:
            if args.token_cache:
                try:
                    logger.debug("Attempting to open token cache %s" % args.token_cache)
                    with open(args.token_cache, 'r') as f:
                        token_dict = json.load(f)
                        token = DataLakeCredential(token_dict)
                        logger.debug("Token cache read from %s" % args.token_cache)
                except (IOError, ValueError):
                    logger.debug("Error when opening token cache, creating new token")
                    token = lib.auth()
                    token_dict = token.token
                    with open(args.token_cache, 'w') as f:
                        json.dump(token_dict, f)
            else:
                token = lib.auth()

        if args.taglist:
            if args.taglist:
                logger.debug("Attempting to parse tags from the taglist %s" % args.taglist)
                filename=args.taglist
                filename=os.path.join(getDataFolderPath(),filename)
                with open(filename) as f:
                    tag_list = f.readlines()
                    tag_list = [x.strip() for x in tag_list]
            generator = walk_tags(base_path, tag_list, years)
        else:  # Crawling
            generator = walk_and_tag_azure(base_path, azure_data_store, token, args.include, args.exclude)

        for file_path, tag in generator:
            measurement_tags = dict()
            measurement_tags.update({DBSchema.TAGKEY_TAG: tag})
            measurement_tags.update({DBSchema.TAGKEY_PLANT:Constants.WORKING_PLANT.shortName})
            measurement_tags.update({DBSchema.TAGKEY_TAGTYPE: DBSchema.TAGVALUE_TAGTYPE_IMS})
            measurement_tags.update({DBSchema.TAGKEY_DATATYPE: DBSchema.TAGVALUE_DATATYPE_RAW})
            pool.apply_async(azure_fetch_and_push_to_influx,
                                 (token, azure_data_store, file_path, measurement_tags, influx_settings,))

    pool.close()
    pool.join()

    after = timeit.default_timer()
    print("Processing took %s seconds" % (after - before))


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
            file_path = base_path + "/{}/{}_{}.csv".format(tag, tag, year)
            yield file_path, tag

if __name__ == '__main__':
    main()
