from test.model.o_db_test_city import TestCity
from test.model.o_db_test_coordinates import TestCoordinates
from test.model.o_db_test_obj import TestObject

__author__ = 'daill'

class TestLocation(TestObject):
    def __init__(self, name:str, city:TestCity, coordinates:TestCoordinates):
        self.name = name
        self.city = city
        self.coordinates = coordinates

    def persistent_attributes(self):
        return ['name', 'city', 'coordinates']

