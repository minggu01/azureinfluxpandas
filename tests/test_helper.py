from helper import *

def test_getProjectRootPath():
    currPath=os.path.realpath(__file__)
    assert currPath.startswith(getProjectRootPath())

def test_getDataFolderPath():
    assert getDataFolderPath().endswith(Constants.DATA_FOLDER_NAME)