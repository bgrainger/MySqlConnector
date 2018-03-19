namespace MySqlConnector.Protocol
{
	internal enum CommandKind
	{
		Quit = 1,
		InitDatabase = 2,
		Query = 3,
		Ping = 14,
		ChangeUser = 17,
		StatementPrepare = 22,
		StatementExecute = 23,
		ResetConnection = 31,
	}
}
