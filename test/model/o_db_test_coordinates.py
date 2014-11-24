from test.model.o_db_test_obj import TestObject

__author__ = 'daill'

class TestCoordinates(TestObject):
    def __init__(self, lat, lng):
        self.lat = lat
        self.lng = lng

    def persistent_attributes(self):
        return ['lat', 'lng']