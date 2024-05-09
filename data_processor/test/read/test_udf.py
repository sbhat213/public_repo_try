from data_processor.sql.udf import unique_id

def test_udf():
    str = unique_id()
    assert type("string") == type(str)