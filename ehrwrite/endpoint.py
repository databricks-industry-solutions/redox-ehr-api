"""
 Class to handle writing to endpoints

   kwargs = specific endpoint 
"""


class RedoxApi():

    def __init__(self, auth):
        self.auth = auth


    def write_record(self, endpoint, action, data, retries = 3, **kwargs):
        pass #TODO

    @pandas_udf
    def write_partition(self, endpoint, action, data, **kwargs): 
        pass #TODO


    def write_df(self, df, num_connections, **kwargs):
        pass #TODO 

    
