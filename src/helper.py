from constants import Constants
import os


def getProjectRootPath():
    '''
    :return: project root folder path
    '''
    currPath=os.path.dirname(os.path.realpath(__file__))
    while not currPath.endswith(Constants.SRC_FOLDER_NAME) :
        currPath=os.path.dirname(currPath)
    return os.path.dirname(currPath)

def getDataFolderPath():
    '''

    :return: data folder absolute path
    '''
    return os.path.join(getProjectRootPath(),Constants.DATA_FOLDER_NAME)


if __name__ == '__main__':
    print(getProjectRootPath())
    print(getDataFolderPath())
