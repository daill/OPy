from test.model.o_db_test_obj import TestObject

__author__ = 'daill'

class TestCity(TestObject):
    def __init__(self, name:str):
        self.name = name

    def persistent_attributes(self):
        return ['name']