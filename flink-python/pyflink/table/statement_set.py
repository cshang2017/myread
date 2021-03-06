
from pyflink.table.table_result import TableResult
from pyflink.util.utils import to_j_explain_detail_arr

__all__ = ['StatementSet']


class StatementSet(object):
    """
    A StatementSet accepts DML statements or Tables,
    the planner can optimize all added statements and Tables together
    and then submit as one job.

    .. note::

        The added statements and Tables will be cleared
        when calling the `execute` method.

    .. versionadded:: 1.11.0
    """

    def __init__(self, _j_statement_set, t_env):
        self._j_statement_set = _j_statement_set
        self._t_env = t_env

    def add_insert_sql(self, stmt):
        """
        add insert statement to the set.

        :param stmt: The statement to be added.
        :type stmt: str
        :return: current StatementSet instance.
        :rtype: pyflink.table.StatementSet

        .. versionadded:: 1.11.0
        """
        self._j_statement_set.addInsertSql(stmt)
        return self

    def add_insert(self, target_path, table, overwrite=False):
        """
        add Table with the given sink table name to the set.

        :param target_path: The path of the registered :class:`~pyflink.table.TableSink` to which
                            the :class:`~pyflink.table.Table` is written.
        :type target_path: str
        :param table: The Table to add.
        :type table: pyflink.table.Table
        :param overwrite: The flag that indicates whether the insert
                          should overwrite existing data or not.
        :type overwrite: bool
        :return: current StatementSet instance.
        :rtype: pyflink.table.StatementSet

        .. versionadded:: 1.11.0
        """
        self._j_statement_set.addInsert(target_path, table._j_table, overwrite)
        return self

    def explain(self, *extra_details):
        """
        returns the AST and the execution plan of all statements and Tables.

        :param extra_details: The extra explain details which the explain result should include,
                              e.g. estimated cost, changelog mode for streaming
        :type extra_details: tuple[ExplainDetail] (variable-length arguments of ExplainDetail)
        :return: All statements and Tables for which the AST and execution plan will be returned.
        :rtype: str

        .. versionadded:: 1.11.0
        """
        j_extra_details = to_j_explain_detail_arr(extra_details)
        return self._j_statement_set.explain(j_extra_details)

    def execute(self):
        """
        execute all statements and Tables as a batch.

        .. note::
            The added statements and Tables will be cleared when executing this method.

        :return: execution result.

        .. versionadded:: 1.11.0
        """
        self._t_env._before_execute()
        return TableResult(self._j_statement_set.execute())
