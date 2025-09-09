using System.Globalization;
using MySqlConnector.Core;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Payloads;
using MySqlConnector.Protocol.Serialization;

namespace MySqlConnector;

public sealed class MySqlDbColumn : DbColumn
{
	internal MySqlDbColumn(int ordinal, ColumnDefinitionPayload column, bool allowZeroDateTime, MySqlDbType mySqlDbType)
	{
		var columnTypeMetadata = TypeMapper.Instance.GetColumnTypeMetadata(mySqlDbType);

		var type = columnTypeMetadata.DbTypeMapping.ClrType;
		var columnSize = type == typeof(string) || type == typeof(Guid) ?
			column.ColumnLength / column.CharacterSet.BytesPerCharacter :
			column.ColumnType == ColumnType.Vector ? column.ColumnLength / 4 :
			column.ColumnLength;

		AllowDBNull = (column.ColumnFlags & ColumnFlags.NotNull) == 0;
		BaseCatalogName = null;
		BaseColumnName = column.PhysicalName;
		BaseSchemaName = column.SchemaName;
		BaseTableName = column.PhysicalTable;
		ColumnName = column.Name;
		ColumnOrdinal = ordinal;
		ColumnSize = columnSize > int.MaxValue ? int.MaxValue : unchecked((int) columnSize);
		DataType = (allowZeroDateTime && type == typeof(DateTime)) ? typeof(MySqlDateTime) : type;
		DataTypeName = columnTypeMetadata.SimpleDataTypeName;
		if (mySqlDbType == MySqlDbType.String)
			DataTypeName += string.Format(CultureInfo.InvariantCulture, "({0})", columnSize);
		IsAliased = column.PhysicalName != column.Name;
		IsAutoIncrement = (column.ColumnFlags & ColumnFlags.AutoIncrement) != 0;
		IsExpression = false;
		IsHidden = false;
		IsKey = (column.ColumnFlags & ColumnFlags.PrimaryKey) != 0;
		IsLong = column.ColumnLength > 255 &&
			((column.ColumnFlags & ColumnFlags.Blob) != 0 || column.ColumnType is ColumnType.TinyBlob or ColumnType.Blob or ColumnType.MediumBlob or ColumnType.LongBlob);
		IsReadOnly = false;
		IsUnique = (column.ColumnFlags & ColumnFlags.UniqueKey) != 0;
		if (column.ColumnType is ColumnType.Decimal or ColumnType.NewDecimal)
		{
			NumericPrecision = (int) column.ColumnLength;
			if ((column.ColumnFlags & ColumnFlags.Unsigned) == 0)
				NumericPrecision--;
			if (column.Decimals > 0)
				NumericPrecision--;
		}
		NumericScale = column.Decimals;
		ProviderType = mySqlDbType;
		TableName = column.Table;
	}

	public MySqlDbType ProviderType { get; }

	/// <summary>
	/// Gets the name of the table that the column belongs to. This will be the alias if the table is aliased in the query.
	/// </summary>
	public string TableName { get; }
}
