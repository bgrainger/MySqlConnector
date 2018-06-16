using System;
using MySql.Data.MySqlClient;
using MySqlConnector.Protocol.Serialization;
using Xunit;

namespace MySqlConnector.Tests
{
	public class ByteBufferWriterTests
	{
		[Theory]
		[InlineData(1)]
		[InlineData(10)]
		[InlineData(100)]
		public void WriteString(int length)
		{
			var expected = new byte[length * 3];
			for (var i = 0; i < length; i++)
			{
				expected[i * 3] = 0xEE;
				expected[i * 3 + 1] = 0x80;
				expected[i * 3 + 2] = 0x81;
			}

			var writer = new ByteBufferWriter();
			var input = char.ConvertFromUtf32(0xE001);
			for (var i = 0; i < length; i++)
				writer.Write(input);
			Assert.Equal(expected, writer.ArraySegment);
		}
	}
}
