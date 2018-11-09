class Plant:
    '''
    This calss defines the plant specific information
    '''
    def __init__(self, shortName:str, datalakeBase:str):
        self.shortName=shortName
        self.datalakeBase=datalakeBase

    @property
    def shortName(self):
        return self._shortName

    @shortName.setter
    def shortName(self,shortName:str):
        self._shortName=shortName

    @property
    def datalakeBase(self):
        return self._datalakeBase

    @datalakeBase.setter
    def datalakeBase(self,datalakeBase:str):
        self._datalakeBase=datalakeBase


class PlantRegistry:
    '''
    registry class for different plant definitions, and helper method to retrieve these definitions
    '''
    GRANE = Plant('GRA', '/raw/corporate/Aspen MS - IP21 Grane/sensordata/1755-GRA')
    KRISTIN= Plant('KRI', '/raw/corporate/PI System Operation North/sensordata/1175-KRI')

    _registry_=[GRANE,KRISTIN]

    @classmethod
    def findPlantByName(cls, plant_short_name:str):
        '''

        :param plant_short_name:
        :return: given the plant short name, retrieve the Plant definition
        '''
        retPlant=None
        for plant in cls._registry_:
            if plant.shortName==plant_short_name:
                retPlant=plant
        return retPlant


if __name__ == '__main__':
    print(PlantRegistry.GRANE.shortName)
    print('azure folder for KRI is: '+PlantRegistry.findPlantByName('KRI').datalakeBase)