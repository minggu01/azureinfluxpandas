class Plant:
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
    GRANE = Plant('GRA', '/raw/corporate/Aspen MS - IP21 Grane/sensordata/1755-GRA')
    AASGARDA= ''
    AASGARDB= ''
    GINAKROG= ''

if __name__ == '__main__':
    print(PlantRegistry.GRANE.shortName)