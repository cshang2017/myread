
from abc import ABC


import datetime
import decimal
import pyarrow as pa
import pytz
from apache_beam.coders import Coder
from apache_beam.coders.coders import FastCoder, LengthPrefixCoder
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.portability import common_urns
from apache_beam.typehints import typehints

from pyflink.fn_execution import coder_impl as slow_coder_impl
try:
    from pyflink.fn_execution import fast_coder_impl as coder_impl
except ImportError:
    coder_impl = slow_coder_impl
from pyflink.fn_execution import flink_fn_execution_pb2
from pyflink.fn_execution.sdk_worker_main import pipeline_options
from pyflink.table.types import Row, TinyIntType, SmallIntType, IntType, BigIntType, BooleanType, \
    FloatType, DoubleType, VarCharType, VarBinaryType, DecimalType, DateType, TimeType, \
    LocalZonedTimestampType, RowType, RowField, to_arrow_type, TimestampType, ArrayType

FLINK_SCALAR_FUNCTION_SCHEMA_CODER_URN = "flink:coder:schema:scalar_function:v1"
FLINK_TABLE_FUNCTION_SCHEMA_CODER_URN = "flink:coder:schema:table_function:v1"
FLINK_SCALAR_FUNCTION_SCHEMA_ARROW_CODER_URN = "flink:coder:schema:scalar_function:arrow:v1"


__all__ = ['FlattenRowCoder', 'RowCoder', 'BigIntCoder', 'TinyIntCoder', 'BooleanCoder',
           'SmallIntCoder', 'IntCoder', 'FloatCoder', 'DoubleCoder',
           'BinaryCoder', 'CharCoder', 'DateCoder', 'TimeCoder',
           'TimestampCoder', 'ArrayCoder', 'MapCoder', 'DecimalCoder', 'ArrowCoder']


class TableFunctionRowCoder(FastCoder):
    """
    Coder for Table Function Row.
    """
    def __init__(self, flatten_row_coder):
        self._flatten_row_coder = flatten_row_coder

    def _create_impl(self):
        return coder_impl.TableFunctionRowCoderImpl(self._flatten_row_coder.get_impl())

    def to_type_hint(self):
        return typehints.List

    @Coder.register_urn(FLINK_TABLE_FUNCTION_SCHEMA_CODER_URN, flink_fn_execution_pb2.Schema)
    def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
        return TableFunctionRowCoder(FlattenRowCoder([from_proto(f.type)
                                                      for f in schema_proto.fields]))

    def __repr__(self):
        return 'TableFunctionRowCoder[%s]' % repr(self._flatten_row_coder)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and self._flatten_row_coder == other._flatten_row_coder)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._flatten_row_coder)


class FlattenRowCoder(FastCoder):
    """
    Coder for Row. The decoded result will be flattened as a list of column values of a row instead
    of a row object.
    """

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def _create_impl(self):
        return coder_impl.FlattenRowCoderImpl([c.get_impl() for c in self._field_coders])

    def is_deterministic(self):
        return all(c.is_deterministic() for c in self._field_coders)

    def to_type_hint(self):
        return typehints.List

    @Coder.register_urn(FLINK_SCALAR_FUNCTION_SCHEMA_CODER_URN, flink_fn_execution_pb2.Schema)
    def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
        return FlattenRowCoder([from_proto(f.type) for f in schema_proto.fields])

    def __repr__(self):
        return 'FlattenRowCoder[%s]' % ', '.join(str(c) for c in self._field_coders)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and len(self._field_coders) == len(other._field_coders)
                and [self._field_coders[i] == other._field_coders[i] for i in
                     range(len(self._field_coders))])

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._field_coders)


class RowCoder(FlattenRowCoder):
    """
    Coder for Row.
    """

    def __init__(self, field_coders):
        super(RowCoder, self).__init__(field_coders)

    def _create_impl(self):
        return coder_impl.RowCoderImpl([c.get_impl() for c in self._field_coders])

    def get_impl(self):
        return self._create_impl()

    def to_type_hint(self):
        return Row

    def __repr__(self):
        return 'RowCoder[%s]' % ', '.join(str(c) for c in self._field_coders)


class CollectionCoder(FastCoder):
    """
    Base coder for collection.
    """
    def __init__(self, elem_coder):
        self._elem_coder = elem_coder

    def _create_impl(self):
        raise NotImplementedError

    def get_impl(self):
        return self._create_impl()

    def is_deterministic(self):
        return self._elem_coder.is_deterministic()

    def to_type_hint(self):
        return []

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and self._elem_coder == other._elem_coder)

    def __repr__(self):
        return '%s[%s]' % (self.__class__.__name__, repr(self._elem_coder))

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._elem_coder)


class ArrayCoder(CollectionCoder):
    """
    Coder for Array.
    """

    def __init__(self, elem_coder):
        self._elem_coder = elem_coder
        super(ArrayCoder, self).__init__(elem_coder)

    def _create_impl(self):
        return coder_impl.ArrayCoderImpl(self._elem_coder.get_impl())


class MapCoder(FastCoder):
    """
    Coder for Map.
    """

    def __init__(self, key_coder, value_coder):
        self._key_coder = key_coder
        self._value_coder = value_coder

    def _create_impl(self):
        return coder_impl.MapCoderImpl(self._key_coder.get_impl(), self._value_coder.get_impl())

    def get_impl(self):
        return self._create_impl()

    def is_deterministic(self):
        return self._key_coder.is_deterministic() and self._value_coder.is_deterministic()

    def to_type_hint(self):
        return {}

    def __repr__(self):
        return 'MapCoder[%s]' % ','.join([repr(self._key_coder), repr(self._value_coder)])

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and self._key_coder == other._key_coder
                and self._value_coder == other._value_coder)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash([self._key_coder, self._value_coder])


class DeterministicCoder(FastCoder, ABC):
    """
    Base Coder for all deterministic Coders.
    """

    def is_deterministic(self):
        return True

    def get_impl(self):
        return self._create_impl()


class BigIntCoder(DeterministicCoder):
    """
    Coder for 8 bytes long.
    """

    def _create_impl(self):
        return coder_impl.BigIntCoderImpl()

    def to_type_hint(self):
        return int


class TinyIntCoder(DeterministicCoder):
    """
    Coder for Byte.
    """

    def _create_impl(self):
        return coder_impl.TinyIntCoderImpl()

    def to_type_hint(self):
        return int


class BooleanCoder(DeterministicCoder):
    """
    Coder for Boolean.
    """

    def _create_impl(self):
        return coder_impl.BooleanCoderImpl()

    def to_type_hint(self):
        return bool


class SmallIntCoder(DeterministicCoder):
    """
    Coder for Short.
    """

    def _create_impl(self):
        return coder_impl.SmallIntCoderImpl()

    def to_type_hint(self):
        return int


class IntCoder(DeterministicCoder):
    """
    Coder for 4 bytes int.
    """

    def _create_impl(self):
        return coder_impl.IntCoderImpl()

    def to_type_hint(self):
        return int


class FloatCoder(DeterministicCoder):
    """
    Coder for Float.
    """

    def _create_impl(self):
        return coder_impl.FloatCoderImpl()

    def to_type_hint(self):
        return float


class DoubleCoder(DeterministicCoder):
    """
    Coder for Double.
    """

    def _create_impl(self):
        return coder_impl.DoubleCoderImpl()

    def to_type_hint(self):
        return float


class DecimalCoder(DeterministicCoder):
    """
    Coder for Decimal.
    """

    def __init__(self, precision, scale):
        self.precision = precision
        self.scale = scale

    def _create_impl(self):
        return coder_impl.DecimalCoderImpl(self.precision, self.scale)

    def to_type_hint(self):
        return decimal.Decimal


class BinaryCoder(DeterministicCoder):
    """
    Coder for Byte Array.
    """

    def _create_impl(self):
        return coder_impl.BinaryCoderImpl()

    def to_type_hint(self):
        return bytes


class CharCoder(DeterministicCoder):
    """
    Coder for Character String.
    """
    def _create_impl(self):
        return coder_impl.CharCoderImpl()

    def to_type_hint(self):
        return str


class DateCoder(DeterministicCoder):
    """
    Coder for Date
    """

    def _create_impl(self):
        return coder_impl.DateCoderImpl()

    def to_type_hint(self):
        return datetime.date


class TimeCoder(DeterministicCoder):
    """
    Coder for Time.
    """

    def _create_impl(self):
        return coder_impl.TimeCoderImpl()

    def to_type_hint(self):
        return datetime.time


class TimestampCoder(DeterministicCoder):
    """
    Coder for Timestamp.
    """

    def __init__(self, precision):
        self.precision = precision

    def _create_impl(self):
        return coder_impl.TimestampCoderImpl(self.precision)

    def to_type_hint(self):
        return datetime.datetime


class LocalZonedTimestampCoder(DeterministicCoder):
    """
    Coder for LocalZonedTimestamp.
    """

    def __init__(self, precision, timezone):
        self.precision = precision
        self.timezone = timezone

    def _create_impl(self):
        return coder_impl.LocalZonedTimestampCoderImpl(self.precision, self.timezone)

    def to_type_hint(self):
        return datetime.datetime


class ArrowCoder(DeterministicCoder):
    """
    Coder for Arrow.
    """
    def __init__(self, schema, row_type, timezone):
        self._schema = schema
        self._row_type = row_type
        self._timezone = timezone

    def _create_impl(self):
        return slow_coder_impl.ArrowCoderImpl(self._schema, self._row_type, self._timezone)

    def to_type_hint(self):
        import pandas as pd
        return pd.Series

    @Coder.register_urn(FLINK_SCALAR_FUNCTION_SCHEMA_ARROW_CODER_URN,
                        flink_fn_execution_pb2.Schema)
    def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
        def _to_arrow_schema(row_type):
            return pa.schema([pa.field(n, to_arrow_type(t), t._nullable)
                              for n, t in zip(row_type.field_names(), row_type.field_types())])

        def _to_data_type(field_type):
            if field_type.type_name == flink_fn_execution_pb2.Schema.TINYINT:
                return TinyIntType(field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.SMALLINT:
                return SmallIntType(field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.INT:
                return IntType(field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.BIGINT:
                return BigIntType(field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.BOOLEAN:
                return BooleanType(field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.FLOAT:
                return FloatType(field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.DOUBLE:
                return DoubleType(field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.VARCHAR:
                return VarCharType(0x7fffffff, field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.VARBINARY:
                return VarBinaryType(0x7fffffff, field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.DECIMAL:
                return DecimalType(field_type.decimal_info.precision,
                                   field_type.decimal_info.scale,
                                   field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.DATE:
                return DateType(field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.TIME:
                return TimeType(field_type.time_info.precision, field_type.nullable)
            elif field_type.type_name == \
                    flink_fn_execution_pb2.Schema.LOCAL_ZONED_TIMESTAMP:
                return LocalZonedTimestampType(field_type.local_zoned_timestamp_info.precision,
                                               field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.TIMESTAMP:
                return TimestampType(field_type.timestamp_info.precision, field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.ARRAY:
                return ArrayType(_to_data_type(field_type.collection_element_type),
                                 field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.TypeName.ROW:
                return RowType(
                    [RowField(f.name, _to_data_type(f.type), f.description)
                     for f in field_type.row_schema.fields], field_type.nullable)
            else:
                raise ValueError("field_type %s is not supported." % field_type)

        def _to_row_type(row_schema):
            return RowType([RowField(f.name, _to_data_type(f.type)) for f in row_schema.fields])

        timezone = pytz.timezone(pipeline_options.view_as(DebugOptions).lookup_experiment(
            "table.exec.timezone"))
        row_type = _to_row_type(schema_proto)
        return ArrowCoder(_to_arrow_schema(row_type), row_type, timezone)

    def __repr__(self):
        return 'ArrowCoder[%s]' % self._schema


class PassThroughLengthPrefixCoder(LengthPrefixCoder):
    """
    Coder which doesn't prefix the length of the encoded object as the length prefix will be handled
    by the wrapped value coder.
    """
    def __init__(self, value_coder):
        super(PassThroughLengthPrefixCoder, self).__init__(value_coder)

    def _create_impl(self):
        return coder_impl.PassThroughLengthPrefixCoderImpl(self._value_coder.get_impl())

    def __repr__(self):
        return 'PassThroughLengthPrefixCoder[%s]' % self._value_coder


Coder.register_structured_urn(
    common_urns.coders.LENGTH_PREFIX.urn, PassThroughLengthPrefixCoder)

type_name = flink_fn_execution_pb2.Schema
_type_name_mappings = {
    type_name.TINYINT: TinyIntCoder(),
    type_name.SMALLINT: SmallIntCoder(),
    type_name.INT: IntCoder(),
    type_name.BIGINT: BigIntCoder(),
    type_name.BOOLEAN: BooleanCoder(),
    type_name.FLOAT: FloatCoder(),
    type_name.DOUBLE: DoubleCoder(),
    type_name.BINARY: BinaryCoder(),
    type_name.VARBINARY: BinaryCoder(),
    type_name.CHAR: CharCoder(),
    type_name.VARCHAR: CharCoder(),
    type_name.DATE: DateCoder(),
    type_name.TIME: TimeCoder(),
}


def from_proto(field_type):
    """
    Creates the corresponding :class:`Coder` given the protocol representation of the field type.

    :param field_type: the protocol representation of the field type
    :return: :class:`Coder`
    """
    field_type_name = field_type.type_name
    coder = _type_name_mappings.get(field_type_name)
    if coder is not None:
        return coder
    if field_type_name == type_name.ROW:
        return RowCoder([from_proto(f.type) for f in field_type.row_schema.fields])
    if field_type_name == type_name.TIMESTAMP:
        return TimestampCoder(field_type.timestamp_info.precision)
    if field_type_name == type_name.LOCAL_ZONED_TIMESTAMP:
        timezone = pytz.timezone(pipeline_options.view_as(DebugOptions).lookup_experiment(
            "table.exec.timezone"))
        return LocalZonedTimestampCoder(field_type.local_zoned_timestamp_info.precision, timezone)
    elif field_type_name == type_name.ARRAY:
        return ArrayCoder(from_proto(field_type.collection_element_type))
    elif field_type_name == type_name.MAP:
        return MapCoder(from_proto(field_type.map_info.key_type),
                        from_proto(field_type.map_info.value_type))
    elif field_type_name == type_name.DECIMAL:
        return DecimalCoder(field_type.decimal_info.precision,
                            field_type.decimal_info.scale)
    else:
        raise ValueError("field_type %s is not supported." % field_type)
