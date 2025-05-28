from enum import Enum


class MaterializationStrategy(Enum):
    FIRST_ITERATION = 1


POOR_FIELD_WEIGHT = 0.02
GOOD_FIELD_WEIGHT = 0.72


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
        dts : dict[str, dict[str, str]]
            Dictionary with data types for give columns of CTEs.
            E.g. {"o": {"o_orderkey": "extracted_list[1]::INT"}}

        Returns
        -------
        str
        """
        raise NotImplementedError()

    def columns_used(self) -> list[str]:
        """
        Get the columns used in the query

        Returns
        -------
        list[str]
        """
        columns = []
        for clause, col_list in self.columns_used_with_position().items():
            if clause == "join":
                for field, join_fields in col_list.items():
                    columns.extend([field] * len(join_fields))
            elif clause == "self_join":
                for field, no_self_joins in col_list.items():
                    columns.extend([field] * 2 * no_self_joins)
            elif clause == "where":
                columns.extend(col_list)
            else:
                # continue
                columns.extend(col_list)
        return columns

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """

        Get the columns used in TPC-H Query 1 along with their position in the query
        (e.g., SELECT, WHERE, GROUP BY, ORDER BY clauses).

        Returns
        -------
        dict
            A dictionary with the following keys:
            - 'select': list of column names used in the SELECT clause.
            - 'where': list of column names used in the WHERE clause that are not joins.
            - 'group_by': list of column names used in the GROUP BY clause.
            - 'order_by': list of column names used in the ORDER BY clause.
            - 'join': list of column names used in a join operation (including WHERE).

        NOTE
        Must be implemented by each subclass to list all cols in select/where/join/etc.
        """
        raise NotImplementedError()

    def join_field_has_filter(self, field: str) -> bool | None:
        """
        Check if the table of the the join field has a filter
        """
        assert field in self.columns_used()
        assert field in self.columns_used_with_position()["join"]

        return self.get_join_field_has_filter(field)

    def get_join_field_has_filter(self, field: str) -> bool:
        """
        Query specific implementation of the join field filter
        """
        raise NotImplementedError("Subclass must implement this method")

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        The number of times a there is a direct filter on the table of the JOIN field.
        Raise ValueError if the field is not a join field.
        """
        raise NotImplementedError("Subclass must implement this method")

    def where_field_has_direct_filter(self, field: str) -> bool | None:
        """
        Check if the where field has a direct filter
        """
        assert field in self.columns_used()
        assert field in self.columns_used_with_position()["where"]

        return self.get_where_field_has_direct_filter(field)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has directly applicable filter
        """
        raise NotImplementedError("Subclass must implement this method")

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the join field has no direct filter
        """
        raise NotImplementedError("Subclass must implement this method")

    def columns_used_in_join(self) -> dict[str, list[str | None]]:
        """
        Get the columns used in the join operation
        """
        return self.columns_used_with_position()["join"]

    def columns_used_in_select(self) -> list[str]:
        """
        Get the columns used in the select operation
        """
        return self.columns_used_with_position()["select"]

    def columns_used_in_where(self) -> list[str]:
        """
        Get the columns used in the where operation
        """
        return self.columns_used_with_position()["where"]

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

    def _json(self, tbl: str, col: str, dt: str | None):
        """
        Extract the column

        Parameters
        ----------
        tbl : str
            The table alias to extract from
        col : str
            The column name to extract
        dts : dict[str, dict[str, str]]
            Dictionary with data types for give columns of CTEs.
            E.g. {"o": {"o_orderkey": "extracted_list[1]::INT"}}

        Returns
        -------
        str
            The column extracted from json. If `dt` is None, there is no json extraction

        """
        # dt = dts.get(col)
        if dt is None:
            return f"{tbl}.{col}"

        # elif dt == "VARCHAR":
        #     return f"{tbl}.raw_json->>'{col}'"
        access_query = f"json_extract_string({tbl}.raw_json, '{col}')"
        if dt != 'VARCHAR':
            access_query += f'::{dt}'
        return access_query

        # return f"CAST({tbl}.raw_json->>'{col}' AS {dt})"
        # return f"CAST({tbl}.raw_json->>'{col}' AS {dt})"

    def get_column_weights(self):
        weights = {
            field: POOR_FIELD_WEIGHT for field in self.columns_used()
        }

        for clause, col_list in self.columns_used_with_position().items():
            if clause == "join":
                for field in col_list.keys():
                    weights[field] += GOOD_FIELD_WEIGHT * \
                        self.get_join_field_has_no_direct_filter(field)
            elif clause == "where":
                for field in col_list:
                    weights[field] += GOOD_FIELD_WEIGHT * \
                        self.get_where_field_has_direct_filter(field)

        if 's_nationkey' in weights:
            weights['s_nationkey'] = 0
        return weights
