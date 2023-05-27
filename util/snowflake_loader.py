class SnowflakeLoader:
    def __init__(_self, conn):
        _self.cnx = conn

    def load_data(_self, query):
        cursor = _self.cnx.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        return df