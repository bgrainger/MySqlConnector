using System;
using System.Text;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Protocol.Payloads
{
	internal sealed class ColumnDefinitionPayload
	{
		public string Name
		{
			get
			{
				if (!m_readNames)
					ReadNames();
				return m_name!;
			}
		}

		public CharacterSet CharacterSet { get; }

		public uint ColumnLength { get; }

		public ColumnType ColumnType { get; }

		public ColumnFlags ColumnFlags { get; }

		public string SchemaName
		{
			get
			{
				if (!m_readNames)
					ReadNames();
				return m_schemaName!;
			}
		}

		public string CatalogName
		{
			get
			{
				if (!m_readNames)
					ReadNames();
				return m_catalogName!;
			}
		}

		public string Table
		{
			get
			{
				if (!m_readNames)
					ReadNames();
				return m_table!;
			}
		}

		public string PhysicalTable
		{
			get
			{
				if (!m_readNames)
					ReadNames();
				return m_physicalTable!;
			}
		}

		public string PhysicalName
		{
			get
			{
				if (!m_readNames)
					ReadNames();
				return m_physicalName!;
			}
		}

		public byte Decimals { get; }

		public static ColumnDefinitionPayload Create(ResizableArraySegment<byte> arraySegment)
		{
			var reader = new ByteArrayReader(arraySegment);
			SkipLengthEncodedByteString(ref reader); // catalog
			SkipLengthEncodedByteString(ref reader); // schema
			SkipLengthEncodedByteString(ref reader); // table
			SkipLengthEncodedByteString(ref reader); // physical table
			SkipLengthEncodedByteString(ref reader); // name
			SkipLengthEncodedByteString(ref reader); // physical name
			reader.ReadByte(0x0C); // length of fixed-length fields, always 0x0C
			var characterSet = (CharacterSet) reader.ReadUInt16();
			var columnLength = reader.ReadUInt32();
			var columnType = (ColumnType) reader.ReadByte();
			var columnFlags = (ColumnFlags) reader.ReadUInt16();
			var decimals = reader.ReadByte(); // 0x00 for integers and static strings, 0x1f for dynamic strings, double, float, 0x00 to 0x51 for decimals
			reader.ReadByte(0); // reserved byte 1
			reader.ReadByte(0); // reserved byte 2

			return new ColumnDefinitionPayload(arraySegment, characterSet, columnLength, columnType, columnFlags, decimals);
		}

		private static void SkipLengthEncodedByteString(ref ByteArrayReader reader)
		{
			var length = checked((int) reader.ReadLengthEncodedInteger());
			reader.Offset += length;
		}

		private ColumnDefinitionPayload(ResizableArraySegment<byte> originalData, CharacterSet characterSet, uint columnLength, ColumnType columnType, ColumnFlags columnFlags, byte decimals)
		{
			OriginalData = originalData;
			CharacterSet = characterSet;
			ColumnLength = columnLength;
			ColumnType = columnType;
			ColumnFlags = columnFlags;
			Decimals = decimals;
		}

		private void ReadNames()
		{
			var reader = new ByteArrayReader(OriginalData);
			m_catalogName = Encoding.UTF8.GetString(reader.ReadLengthEncodedByteString());
			m_schemaName = Encoding.UTF8.GetString(reader.ReadLengthEncodedByteString());
			m_table = Encoding.UTF8.GetString(reader.ReadLengthEncodedByteString());
			m_physicalTable = Encoding.UTF8.GetString(reader.ReadLengthEncodedByteString());
			m_name = Encoding.UTF8.GetString(reader.ReadLengthEncodedByteString());
			m_physicalName = Encoding.UTF8.GetString(reader.ReadLengthEncodedByteString());
			m_readNames = true;
		}

		ResizableArraySegment<byte> OriginalData { get; }

		bool m_readNames;
		string? m_name;
		string? m_schemaName;
		string? m_catalogName;
		string? m_table;
		string? m_physicalTable;
		string? m_physicalName;
	}
}
