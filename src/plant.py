class Plant:
    _name_=None
    _datalake_base_=None

    def getName(self)->str:
        return self._name_
    def getDatalakeBase(self)->str:
        return self._datalake_base_
    def __init__(self,name,datalakeBase):
        self._name_=name
        self._datalake_base_=datalakeBase

class PlantRegistry:
    GRA = Plant('GRA', '/raw/corporate/Aspen MS - IP21 Grane/sensordata/1755-GRA')
    ASGA=''
    ASGB=''
    GKR=''

if __name__ == '__main__':
    print(PlantRegistry.GRA.getName())