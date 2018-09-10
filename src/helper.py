import os
from constants import Constants
class Helper:
    '''
    this class defines the utility functions
    '''

    @staticmethod
    def getProjectRoot():
        '''
        this function finds the project root folder given that the src folder is the first level sub-folder
        in the project
        :return:
        '''
        cur_dir_path_ = os.path.dirname(os.path.realpath(__file__))
        while not cur_dir_path_.endswith('src'):
            cur_dir_path_ = os.path.abspath(os.path.join(cur_dir_path_, os.pardir))
        cur_dir_path_=os.path.abspath(os.path.join(cur_dir_path_, os.pardir))
        return cur_dir_path_

    @staticmethod
    def getDataFolderPath():
        project_path=Helper.getProjectRoot()
        return os.path.join(project_path, Constants.DATA_FOLDER_NAME)

if __name__ == '__main__':
    print(Helper.getProjectRoot())
    print(Helper.getDataFolderPath())
    print(Helper.getDBConnStr())