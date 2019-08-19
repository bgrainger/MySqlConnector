using System.Collections.Generic;
using MySqlConnector.Core;
using Xunit;

namespace MySqlConnector.Tests
{
	public class CachedProcedureTests
	{
		[Theory]
		[MemberData(nameof(CreateParseableParameters))]
		public void ParseParameters(string sql, object[] expected)
		{
			var actual = CachedProcedure.ParseParameters(sql);
			Assert.Equal(expected.Length, actual.Count);
			for (int i = 0; i < expected.Length; i++)
			{
				var expectedParameter = (CachedParameter) expected[i];
				var actualParameter = actual[i];
				Assert.Equal(expectedParameter.Position, actualParameter.Position);
				Assert.Equal(expectedParameter.Direction, actualParameter.Direction);
				Assert.Equal(expectedParameter.Name, actualParameter.Name);
				Assert.Equal(expectedParameter.MySqlDbType, actualParameter.MySqlDbType);
			}
		}

		public static IEnumerable<object[]> CreateParseableParameters()
		{
			return new[]
			{
				new object[]
				{
					"", new object[0],
				},
				new object[]
				{
					"/* no, parameters */", new object[0],
				},
				new object[]
				{
					"IN test INT", new object[]
					{
						new CachedParameter(1, "IN", "test", "INT", false, 0),
					}
				},
				new object[]
				{
					"IN test INT UNSIGNED", new object[]
					{
						new CachedParameter(1, "IN", "test", "INT", true, 0),
					}
				},
				new object[]
				{
					"-- IN ignored INT UNSIGNED,\r\nIN notignored INT", new object[]
					{
						new CachedParameter(1, "IN", "notignored", "INT", false, 0),
					}
				},
				new object[]
				{
					"IN param1 INT,\r\nIN param2 INT", new object[]
					{
						new CachedParameter(1, "IN", "param1", "INT", false, 0),
						new CachedParameter(2, "IN", "param2", "INT", false, 0),
					}
				},
				new object[]
				{
					"IN /* ignored BIGINT,\r\nIN*/ param1 INT", new object[]
					{
						new CachedParameter(1, "IN", "param1", "INT", false, 0),
					}
				},
				new object[]
				{
					"IN param1 INT(11)", new object[]
					{
						new CachedParameter(1, "IN", "param1", "INT", false, 11),
					}
				},
				new object[]
				{
					"param1 BIGINT(21) UNSIGNED ZEROFILL", new object[]
					{
						new CachedParameter(1, "IN", "param1", "BIGINT", true, 21),
					}
				},
				new object[]
				{
					"param1 VARCHAR(63)", new object[]
					{
						new CachedParameter(1, "IN", "param1", "VARCHAR", false, 63),
					}
				},
				new object[]
				{
					"param1 VARCHAR(63) CHARSET latin1", new object[]
					{
						new CachedParameter(1, "IN", "param1", "VARCHAR", false, 63),
					}
				},
				new object[]
				{
					"param1 VARCHAR(63) COLLATE utf8bin", new object[]
					{
						new CachedParameter(1, "IN", "param1", "VARCHAR", false, 63),
					}
				},
				new object[]
				{
					"param1 VARCHAR(63) CHARACTER SET latin1 COLLATE latin1_bin", new object[]
					{
						new CachedParameter(1, "IN", "param1", "VARCHAR", false, 63),
					}
				},
				new object[]
				{
					"`par``am` INT", new object[]
					{
						new CachedParameter(1, "IN", "par`am", "INT", false, 0),
					}
				},
				new object[]
				{
					"OUT param DECIMAL(10,5)", new object[]
					{
						new CachedParameter(1, "OUT", "param", "DECIMAL", false, 0),
					}
				},
				new object[]
				{
					"INOUT param LONGTEXT", new object[]
					{
						new CachedParameter(1, "INOUT", "param", "LONGTEXT", false, 0),
					}
				},
				new object[]
				{
					@"IN param1 DATETIME(6),
-- ignored1
OUT param2 /* ignore */ INT,
param3 DECIMAL(20,10),
inout param4 VARCHAR(63) CHARSET latin1,
param5 bigint(20) unsigned zerofill,
out param6 bool",
					new object[]
					{
						new CachedParameter(1, "IN", "param1", "DATETIME", false, 6),
						new CachedParameter(2, "OUT", "param2", "INT", false, 0),
						new CachedParameter(3, "IN", "param3", "DECIMAL", false, 0),
						new CachedParameter(4, "INOUT", "param4", "VARCHAR", false, 63),
						new CachedParameter(5, "IN", "param5", "BIGINT", true, 20),
						new CachedParameter(6, "OUT", "param6", "TINYINT", false, 1),
					}
				},
				new object[]
				{
					@"
param1 boolean,
param2 nvarchar,
param3 real(20,10),
	-- ignored INT	
param4 INTEGER(3)
",
					new object[]
					{
						new CachedParameter(1, "IN", "param1", "TINYINT", false, 1),
						new CachedParameter(2, "IN", "param2", "VARCHAR", false, 0),
						new CachedParameter(3, "IN", "param3", "DOUBLE", false, 20),
						new CachedParameter(4, "IN", "param4", "INT", false, 3),
					}
				},
			};
		}

		[Theory]
		[InlineData("INT", "INT", false, 0)]
		[InlineData("INTEGER", "INT", false, 0)]
		[InlineData("INT(11)", "INT", false, 11)]
		[InlineData("INTEGER(11)", "INT", false, 11)]
		[InlineData("INT(11) UNSIGNED", "INT", true, 11)]
		[InlineData("INT(11) ZEROFILL", "INT", false, 11)]
		[InlineData("INT(11) UNSIGNED ZEROFILL", "INT", true, 11)]
		[InlineData("BIGINT(20)", "BIGINT", false, 20)]
		[InlineData("TINYINT(1) UNSIGNED", "TINYINT", true, 1)]
		[InlineData("BOOL", "TINYINT", false, 1)]
		[InlineData("NUMERIC(30,20)", "DECIMAL", false, 30)]
		[InlineData("VARCHAR(300)", "VARCHAR", false, 300)]
		[InlineData("VARCHAR(300) CHARSET utf8mb4", "VARCHAR", false, 300)]
		[InlineData("VARCHAR(300) COLLATE ascii_general_ci", "VARCHAR", false, 300)]
		[InlineData("BINARY(16)", "BINARY", false, 16)]
		[InlineData("CHAR(36)", "CHAR", false, 36)]
		public void ParseDataType(string sql, string expectedDataType, bool expectedUnsigned, int expectedLength)
		{
			var dataType = CachedProcedure.ParseDataType(sql, out var unsigned, out var length);
			Assert.Equal((expectedDataType, expectedUnsigned, expectedLength), (dataType, unsigned, length));
		}
	}
}
