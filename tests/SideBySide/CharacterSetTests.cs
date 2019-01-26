using Dapper;
#if !BASELINE
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Serialization;
#endif
using Xunit;

namespace SideBySide
{
	public class CharacterSetTests : IClassFixture<DatabaseFixture>
	{
		public CharacterSetTests(DatabaseFixture database)
		{
			m_database = database;
		}

#if !BASELINE
		[Fact]
		public void MaxLength()
		{
			using var reader = m_database.Connection.ExecuteReader(@"select coll.ID, cs.MAXLEN from information_schema.collations coll inner join information_schema.character_sets cs using(CHARACTER_SET_NAME);");
			while (reader.Read())
			{
				var characterSet = (CharacterSet) reader.GetInt32(0);
				var maxLength = reader.GetInt32(1);

				Assert.Equal(maxLength, ProtocolUtility.GetBytesPerCharacter(characterSet));
			}
		}
#endif

		readonly DatabaseFixture m_database;
	}
}
