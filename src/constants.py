from plant import PlantRegistry

'''
Created on 4. sep. 2017

@author: MINGGU
'''

class Constants:
    '''
    This class includes the information that should be configurable by the end user through modifying the config.ini file.
    '''

    DATA_FOLDER_NAME= 'data'
    SRC_FOLDER_NAME='src'

    DATA_LAKE_STORE='dataplatformdlsprod'
    INFLUX_DB_HOST='localhost'
    INFLUX_DB_PORT='8086'
    INFLUX_DB_NAME='data'
    INFLUX_DB_USER='root'

    WORKING_PLANT=PlantRegistry.GRANE





