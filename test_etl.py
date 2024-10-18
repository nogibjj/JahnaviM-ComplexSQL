'''This script is to test the etl.py script'''

from mylib.query import create_record, read_db, update_ca, delete_ca
from mylib.extract import extract
from mylib.transform_load import trans_load

# Test CRUD
def test_create_record():
    '''tests if create_record function works as expected'''
    extract()
    trans_load()
    output = create_record()
    assert len(output) == 1

def test_read_db():
    '''tests if read_db function works as expected'''
    output = read_db()
    assert len(output) == 5 

def test_update_ca():
    '''tests if update_ca function works as expected'''
    output = update_ca()
    assert len(output) == 1

def test_delete_ca():
    '''tests if delete_ca function works as expected'''
    output = delete_ca()
    assert len(output) == 5
