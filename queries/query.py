class Query:
    """
    Base class for queries used in this project
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted query, adjusted to current db materializaiton

        Parameters
        ----------
        fields : list[tuple[str, dict, bool]]
            List of tuples of the fields in the database table.

        Returns
        -------
        str
        """

    def columns_used(self):
        """
        Get the columns used in the query

        Returns
        -------
        list[str]
        """
        return []

    def _get_field_accesses(self, fields: list[tuple[str, dict, bool]]) -> dict:

        used_columns = self.columns_used()

        if fields is None:
            return {col: None for col in used_columns}

        data_types = dict()

        for col, access_query, materialized in fields:
            if col in used_columns:
                if materialized:
                    data_types[col] = None
                else:
                    data_types[col] = access_query["type"]

        return data_types

    def _json(self, tbl: str, col: str, dt: str):
        """
        Extract the column 

        Parameters
        ----------
        tbl : str
            The table alias to extract from
        col : str
            The column name to extract
        dt : str | None
            The date type of the column to extract

        Returns
        -------
        str
            The column extracted from json. If `dt` is None, there is no json extraction

        """
        if dt is None:
            return f"{tbl}.{col}"

        # elif dt == "VARCHAR":
        #     return f"{tbl}.raw_json->>'{col}'"

        return f"CAST({tbl}.raw_json->>'{col}' AS {dt})"
