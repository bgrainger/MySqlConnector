using System.Text;
using MySql.Data.MySqlClient;
using MySqlConnector.Core;
using MySqlConnector.Utilities;
using Xunit;

namespace MySqlConnector.Tests
{
	public class StatementPreparerTests
	{
		[Theory]
		[InlineData("SELECT Id\nFROM mytable\nWHERE column1 = 2\nAND column2 = @param")]
		[InlineData("SELECT Id\nFROM mytable\nWHERE column1 = 2  -- mycomment\nAND column2 = @param")]
		[InlineData("SELECT Id\nFROM mytable\nWHERE column1 = 2 -- mycomment\nAND column2 = @param")]
		[InlineData("SELECT Id\nFROM mytable\nWHERE column1 = 2 -- mycomment\n  AND column2 = @param")]
		public void Bug429(string sql)
		{
			var parameters = new MySqlParameterCollection();
			parameters.AddWithValue("@param", 123);
			var parsedSql = GetParsedSql(sql, parameters);
			Assert.Equal(sql.Replace("@param", "123"), parsedSql);
		}

		[Theory]
		[InlineData(@"SELECT /* * / @param */ 1;")]
		[InlineData("SELECT # @param \n1;")]
		[InlineData("SELECT -- @param \n1;")]
		public void ParametersIgnoredInComments(string sql)
		{
			Assert.Equal(sql, GetParsedSql(sql));
		}

		[Theory]
		[InlineData("SELECT '@param';")]
		[InlineData("SELECT \"@param\";")]
		[InlineData("SELECT `@param`;")]
		[InlineData("SELECT 'test\\'@param';")]
		[InlineData("SELECT \"test\\\"@param\";")]
		[InlineData("SELECT 'test''@param';")]
		[InlineData("SELECT \"test\"\"@param\";")]
		[InlineData("SELECT `test``@param`;")]
		public void ParametersIgnoredInStrings(string sql)
		{
			Assert.Equal(sql, GetParsedSql(sql));
		}

		private static string GetParsedSql(string input, MySqlParameterCollection parameters = null)
		{
			using (var writer = new StatementWriter())
			{
				var preparer = new StatementPreparer(writer, input, parameters ?? new MySqlParameterCollection(), StatementPreparerOptions.None);
				var bytes = preparer.ParseAndBindParameters().Slice(1);
				return Encoding.UTF8.GetString(bytes);
			}
		}
	}
}
