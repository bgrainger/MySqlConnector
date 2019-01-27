using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using MySqlConnector.Utilities;

namespace MySql.Data.MySqlClient
{
	public sealed class MySqlConnectionStringBuilder : DbConnectionStringBuilder
	{
		public MySqlConnectionStringBuilder()
		{
		}

		public MySqlConnectionStringBuilder(string? connectionString)
		{
			ConnectionString = connectionString;
		}

		// Base Options
		public string? Server
		{
			get => MySqlConnectionStringOption.Server.GetValue(this);
			set => MySqlConnectionStringOption.Server.SetValue(this, value);
		}

		public uint Port
		{
			get => MySqlConnectionStringOption.Port.GetValue(this);
			set => MySqlConnectionStringOption.Port.SetValue(this, value);
		}

		public string? UserID
		{
			get => MySqlConnectionStringOption.UserID.GetValue(this);
			set => MySqlConnectionStringOption.UserID.SetValue(this, value);
		}

		public string? Password
		{
			get => MySqlConnectionStringOption.Password.GetValue(this);
			set => MySqlConnectionStringOption.Password.SetValue(this, value);
		}

		public string? Database
		{
			get => MySqlConnectionStringOption.Database.GetValue(this);
			set => MySqlConnectionStringOption.Database.SetValue(this, value);
		}

		public MySqlLoadBalance LoadBalance
		{
			get => MySqlConnectionStringOption.LoadBalance.GetValue(this);
			set => MySqlConnectionStringOption.LoadBalance.SetValue(this, value);
		}

		public MySqlConnectionProtocol ConnectionProtocol
		{
			get => MySqlConnectionStringOption.ConnectionProtocol.GetValue(this);
			set => MySqlConnectionStringOption.ConnectionProtocol.SetValue(this, value);
		}

		public string? PipeName
		{
			get => MySqlConnectionStringOption.PipeName.GetValue(this);
			set => MySqlConnectionStringOption.PipeName.SetValue(this, value);
		}

		// SSL/TLS Options
		public MySqlSslMode SslMode
		{
			get => MySqlConnectionStringOption.SslMode.GetValue(this);
			set => MySqlConnectionStringOption.SslMode.SetValue(this, value);
		}

		public string? CertificateFile
		{
			get => MySqlConnectionStringOption.CertificateFile.GetValue(this);
			set => MySqlConnectionStringOption.CertificateFile.SetValue(this, value);
		}

		public string? CertificatePassword
		{
			get => MySqlConnectionStringOption.CertificatePassword.GetValue(this);
			set => MySqlConnectionStringOption.CertificatePassword.SetValue(this, value);
		}

		public string? CACertificateFile
		{
			get => MySqlConnectionStringOption.CACertificateFile.GetValue(this);
			set => MySqlConnectionStringOption.CACertificateFile.SetValue(this, value);
		}

		public MySqlCertificateStoreLocation CertificateStoreLocation
		{
			get => MySqlConnectionStringOption.CertificateStoreLocation.GetValue(this);
			set => MySqlConnectionStringOption.CertificateStoreLocation.SetValue(this, value);
		}

		public string? CertificateThumbprint
		{
			get => MySqlConnectionStringOption.CertificateThumbprint.GetValue(this);
			set => MySqlConnectionStringOption.CertificateThumbprint.SetValue(this, value);
		}

		// Connection Pooling Options
		public bool Pooling
		{
			get => MySqlConnectionStringOption.Pooling.GetValue(this);
			set => MySqlConnectionStringOption.Pooling.SetValue(this, value);
		}

		public uint ConnectionLifeTime
		{
			get => MySqlConnectionStringOption.ConnectionLifeTime.GetValue(this);
			set => MySqlConnectionStringOption.ConnectionLifeTime.SetValue(this, value);
		}

		public bool ConnectionReset
		{
			get => MySqlConnectionStringOption.ConnectionReset.GetValue(this);
			set => MySqlConnectionStringOption.ConnectionReset.SetValue(this, value);
		}

		public uint ConnectionIdlePingTime
		{
			get => MySqlConnectionStringOption.ConnectionIdlePingTime.GetValue(this);
			set => MySqlConnectionStringOption.ConnectionIdlePingTime.SetValue(this, value);
		}

		public uint ConnectionIdleTimeout
		{
			get => MySqlConnectionStringOption.ConnectionIdleTimeout.GetValue(this);
			set => MySqlConnectionStringOption.ConnectionIdleTimeout.SetValue(this, value);
		}

		public uint MinimumPoolSize
		{
			get => MySqlConnectionStringOption.MinimumPoolSize.GetValue(this);
			set => MySqlConnectionStringOption.MinimumPoolSize.SetValue(this, value);
		}

		public uint MaximumPoolSize
		{
			get => MySqlConnectionStringOption.MaximumPoolSize.GetValue(this);
			set => MySqlConnectionStringOption.MaximumPoolSize.SetValue(this, value);
		}

		// Other Options
		public bool AllowPublicKeyRetrieval
		{
			get => MySqlConnectionStringOption.AllowPublicKeyRetrieval.GetValue(this);
			set => MySqlConnectionStringOption.AllowPublicKeyRetrieval.SetValue(this, value);
		}

		public bool AllowUserVariables
		{
			get => MySqlConnectionStringOption.AllowUserVariables.GetValue(this);
			set => MySqlConnectionStringOption.AllowUserVariables.SetValue(this, value);
		}

		public bool AllowZeroDateTime
		{
			get => MySqlConnectionStringOption.AllowZeroDateTime.GetValue(this);
			set => MySqlConnectionStringOption.AllowZeroDateTime.SetValue(this, value);
		}

		public string? ApplicationName
		{
			get => MySqlConnectionStringOption.ApplicationName.GetValue(this);
			set => MySqlConnectionStringOption.ApplicationName.SetValue(this, value);
		}

		public bool AutoEnlist
		{
			get => MySqlConnectionStringOption.AutoEnlist.GetValue(this);
			set => MySqlConnectionStringOption.AutoEnlist.SetValue(this, value);
		}

		public string? CharacterSet
		{
			get => MySqlConnectionStringOption.CharacterSet.GetValue(this);
			set => MySqlConnectionStringOption.CharacterSet.SetValue(this, value);
		}

		public uint ConnectionTimeout
		{
			get => MySqlConnectionStringOption.ConnectionTimeout.GetValue(this);
			set => MySqlConnectionStringOption.ConnectionTimeout.SetValue(this, value);
		}

		public bool ConvertZeroDateTime
		{
			get => MySqlConnectionStringOption.ConvertZeroDateTime.GetValue(this);
			set => MySqlConnectionStringOption.ConvertZeroDateTime.SetValue(this, value);
		}

		public MySqlDateTimeKind DateTimeKind
		{
			get => MySqlConnectionStringOption.DateTimeKind.GetValue(this);
			set => MySqlConnectionStringOption.DateTimeKind.SetValue(this, value);
		}

		public uint DefaultCommandTimeout
		{
			get => MySqlConnectionStringOption.DefaultCommandTimeout.GetValue(this);
			set => MySqlConnectionStringOption.DefaultCommandTimeout.SetValue(this, value);
		}

		public bool ForceSynchronous
		{
			get => MySqlConnectionStringOption.ForceSynchronous.GetValue(this);
			set => MySqlConnectionStringOption.ForceSynchronous.SetValue(this, value);
		}

		public MySqlGuidFormat GuidFormat
		{
			get => MySqlConnectionStringOption.GuidFormat.GetValue(this);
			set => MySqlConnectionStringOption.GuidFormat.SetValue(this, value);
		}

		public bool IgnoreCommandTransaction
		{
			get => MySqlConnectionStringOption.IgnoreCommandTransaction.GetValue(this);
			set => MySqlConnectionStringOption.IgnoreCommandTransaction.SetValue(this, value);
		}

		public bool IgnorePrepare
		{
			get => MySqlConnectionStringOption.IgnorePrepare.GetValue(this);
			set => MySqlConnectionStringOption.IgnorePrepare.SetValue(this, value);
		}

		public bool InteractiveSession
		{
			get => MySqlConnectionStringOption.InteractiveSession.GetValue(this);
			set => MySqlConnectionStringOption.InteractiveSession.SetValue(this, value);
		}

		public uint Keepalive
		{
			get => MySqlConnectionStringOption.Keepalive.GetValue(this);
			set => MySqlConnectionStringOption.Keepalive.SetValue(this, value);
		}

		public bool OldGuids
		{
			get => MySqlConnectionStringOption.OldGuids.GetValue(this);
			set => MySqlConnectionStringOption.OldGuids.SetValue(this, value);
		}

		public bool PersistSecurityInfo
		{
			get => MySqlConnectionStringOption.PersistSecurityInfo.GetValue(this);
			set => MySqlConnectionStringOption.PersistSecurityInfo.SetValue(this, value);
		}

		public string? ServerRsaPublicKeyFile
		{
			get => MySqlConnectionStringOption.ServerRsaPublicKeyFile.GetValue(this);
			set => MySqlConnectionStringOption.ServerRsaPublicKeyFile.SetValue(this, value);
		}

		public string? ServerSPN
		{
			get => MySqlConnectionStringOption.ServerSPN.GetValue(this);
			set => MySqlConnectionStringOption.ServerSPN.SetValue(this, value);
		}

		public bool TreatTinyAsBoolean
		{
			get => MySqlConnectionStringOption.TreatTinyAsBoolean.GetValue(this);
			set => MySqlConnectionStringOption.TreatTinyAsBoolean.SetValue(this, value);
		}

		public bool UseAffectedRows
		{
			get => MySqlConnectionStringOption.UseAffectedRows.GetValue(this);
			set => MySqlConnectionStringOption.UseAffectedRows.SetValue(this, value);
		}

		public bool UseCompression
		{
			get => MySqlConnectionStringOption.UseCompression.GetValue(this);
			set => MySqlConnectionStringOption.UseCompression.SetValue(this, value);
		}

		public bool UseXaTransactions
		{
			get => MySqlConnectionStringOption.UseXaTransactions.GetValue(this);
			set => MySqlConnectionStringOption.UseXaTransactions.SetValue(this, value);
		}

		// Other Methods
		public override bool ContainsKey(string key)
		{
			var option = MySqlConnectionStringOption.TryGetOptionForKey(key);
			return option != null && base.ContainsKey(option.Key);
		}

		public override bool Remove(string key)
		{
			var option = MySqlConnectionStringOption.TryGetOptionForKey(key);
			return option != null && base.Remove(option.Key);
		}

		public override object? this[string key]
		{
			get => MySqlConnectionStringOption.GetOptionForKey(key).GetObject(this);
			set => base[MySqlConnectionStringOption.GetOptionForKey(key).Key] = Convert.ToString(value, CultureInfo.InvariantCulture);
		}

		internal string GetConnectionString(bool includePassword)
		{
			var connectionString = ConnectionString;
			if (includePassword)
				return connectionString;

			if (m_cachedConnectionString != connectionString)
			{
				var csb = new MySqlConnectionStringBuilder(connectionString);
				foreach (string key in Keys)
					foreach (var passwordKey in MySqlConnectionStringOption.Password.Keys)
						if (string.Equals(key, passwordKey, StringComparison.OrdinalIgnoreCase))
							csb.Remove(key);
				m_cachedConnectionStringWithoutPassword = csb.ConnectionString;
				m_cachedConnectionString = connectionString;
			}

			return m_cachedConnectionStringWithoutPassword!;
		}

		string? m_cachedConnectionString;
		string? m_cachedConnectionStringWithoutPassword;
	}

	internal abstract class MySqlConnectionStringOption
	{
		// Base Options
		public static readonly MySqlConnectionStringNullableOption<string> Server;
		public static readonly MySqlConnectionStringOption<uint> Port;
		public static readonly MySqlConnectionStringNullableOption<string> UserID;
		public static readonly MySqlConnectionStringNullableOption<string> Password;
		public static readonly MySqlConnectionStringNullableOption<string> Database;
		public static readonly MySqlConnectionStringOption<MySqlLoadBalance> LoadBalance;
		public static readonly MySqlConnectionStringOption<MySqlConnectionProtocol> ConnectionProtocol;
		public static readonly MySqlConnectionStringNullableOption<string> PipeName;

		// SSL/TLS Options
		public static readonly MySqlConnectionStringOption<MySqlSslMode> SslMode;
		public static readonly MySqlConnectionStringNullableOption<string> CertificateFile;
		public static readonly MySqlConnectionStringNullableOption<string> CertificatePassword;
		public static readonly MySqlConnectionStringOption<MySqlCertificateStoreLocation> CertificateStoreLocation;
		public static readonly MySqlConnectionStringNullableOption<string> CertificateThumbprint;
		public static readonly MySqlConnectionStringNullableOption<string> CACertificateFile;

		// Connection Pooling Options
		public static readonly MySqlConnectionStringOption<bool> Pooling;
		public static readonly MySqlConnectionStringOption<uint> ConnectionLifeTime;
		public static readonly MySqlConnectionStringOption<bool> ConnectionReset;
		public static readonly MySqlConnectionStringOption<uint> ConnectionIdlePingTime;
		public static readonly MySqlConnectionStringOption<uint> ConnectionIdleTimeout;
		public static readonly MySqlConnectionStringOption<uint> MinimumPoolSize;
		public static readonly MySqlConnectionStringOption<uint> MaximumPoolSize;

		// Other Options
		public static readonly MySqlConnectionStringOption<bool> AllowPublicKeyRetrieval;
		public static readonly MySqlConnectionStringOption<bool> AllowUserVariables;
		public static readonly MySqlConnectionStringOption<bool> AllowZeroDateTime;
		public static readonly MySqlConnectionStringNullableOption<string> ApplicationName;
		public static readonly MySqlConnectionStringOption<bool> AutoEnlist;
		public static readonly MySqlConnectionStringNullableOption<string> CharacterSet;
		public static readonly MySqlConnectionStringOption<uint> ConnectionTimeout;
		public static readonly MySqlConnectionStringOption<bool> ConvertZeroDateTime;
		public static readonly MySqlConnectionStringOption<MySqlDateTimeKind> DateTimeKind;
		public static readonly MySqlConnectionStringOption<uint> DefaultCommandTimeout;
		public static readonly MySqlConnectionStringOption<bool> ForceSynchronous;
		public static readonly MySqlConnectionStringOption<MySqlGuidFormat> GuidFormat;
		public static readonly MySqlConnectionStringOption<bool> IgnoreCommandTransaction;
		public static readonly MySqlConnectionStringOption<bool> IgnorePrepare;
		public static readonly MySqlConnectionStringOption<bool> InteractiveSession;
		public static readonly MySqlConnectionStringOption<uint> Keepalive;
		public static readonly MySqlConnectionStringOption<bool> OldGuids;
		public static readonly MySqlConnectionStringOption<bool> PersistSecurityInfo;
		public static readonly MySqlConnectionStringNullableOption<string> ServerRsaPublicKeyFile;
		public static readonly MySqlConnectionStringNullableOption<string> ServerSPN;
		public static readonly MySqlConnectionStringOption<bool> TreatTinyAsBoolean;
		public static readonly MySqlConnectionStringOption<bool> UseAffectedRows;
		public static readonly MySqlConnectionStringOption<bool> UseCompression;
		public static readonly MySqlConnectionStringOption<bool> UseXaTransactions;

		public static MySqlConnectionStringOption? TryGetOptionForKey(string key) =>
			s_options.TryGetValue(key, out var option) ? option : null;

		public static MySqlConnectionStringOption GetOptionForKey(string key) =>
			TryGetOptionForKey(key) ?? throw new ArgumentException("Option '{0}' not supported.".FormatInvariant(key));

		public string Key => m_keys[0];
		public IReadOnlyList<string> Keys => m_keys;

		public abstract object? GetObject(MySqlConnectionStringBuilder builder);

		protected MySqlConnectionStringOption(IReadOnlyList<string> keys)
		{
			m_keys = keys;
		}

		private static void AddOption(MySqlConnectionStringOption option)
		{
			foreach (string key in option.m_keys)
				s_options.Add(key, option);
		}

		static MySqlConnectionStringOption()
		{
			s_options = new Dictionary<string, MySqlConnectionStringOption>(StringComparer.OrdinalIgnoreCase);

			// Base Options
			AddOption(Server = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "Server", "Host", "Data Source", "DataSource", "Address", "Addr", "Network Address" },
				defaultValue: ""));

			AddOption(Port = new MySqlConnectionStringOption<uint>(
				keys: new[] { "Port" },
				defaultValue: 3306u));

			AddOption(UserID = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "User Id", "UserID", "Username", "Uid", "User name", "User" },
				defaultValue: ""));

			AddOption(Password = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "Password", "pwd" },
				defaultValue: ""));

			AddOption(Database = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "Database", "Initial Catalog" },
				defaultValue: ""));

			AddOption(LoadBalance = new MySqlConnectionStringOption<MySqlLoadBalance>(
				keys: new[] { "LoadBalance", "Load Balance" },
				defaultValue: MySqlLoadBalance.RoundRobin));

			AddOption(ConnectionProtocol = new MySqlConnectionStringOption<MySqlConnectionProtocol>(
				keys: new[] { "ConnectionProtocol", "Connection Protocol", "Protocol" },
				defaultValue: MySqlConnectionProtocol.Socket));

			AddOption(PipeName = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "PipeName", "Pipe", "Pipe Name" },
				defaultValue: "MYSQL"));

			// SSL/TLS Options
			AddOption(SslMode = new MySqlConnectionStringOption<MySqlSslMode>(
				keys: new[] { "SSL Mode", "SslMode" },
				defaultValue: MySqlSslMode.Preferred));

			AddOption(CertificateFile = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "CertificateFile", "Certificate File" },
				defaultValue: null));

			AddOption(CertificatePassword = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "CertificatePassword", "Certificate Password" },
				defaultValue: null));

			AddOption(CACertificateFile = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "CACertificateFile", "CA Certificate File" },
				defaultValue: null));

			AddOption(CertificateStoreLocation = new MySqlConnectionStringOption<MySqlCertificateStoreLocation>(
				keys: new[] { "CertificateStoreLocation", "Certificate Store Location" },
				defaultValue: MySqlCertificateStoreLocation.None));

			AddOption(CertificateThumbprint = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "CertificateThumbprint", "Certificate Thumbprint", "Certificate Thumb Print" },
				defaultValue: null));

			// Connection Pooling Options
			AddOption(Pooling = new MySqlConnectionStringOption<bool>(
				keys: new[] { "Pooling" },
				defaultValue: true));

			AddOption(ConnectionLifeTime = new MySqlConnectionStringOption<uint>(
				keys: new[] { "Connection Lifetime", "ConnectionLifeTime" },
				defaultValue: 0));

			AddOption(ConnectionReset = new MySqlConnectionStringOption<bool>(
				keys: new[] { "Connection Reset", "ConnectionReset" },
				defaultValue: true));

			AddOption(ConnectionIdlePingTime = new MySqlConnectionStringOption<uint>(
				keys: new[] { "Connection Idle Ping Time", "ConnectionIdlePingTime" },
				defaultValue: 0));

			AddOption(ConnectionIdleTimeout = new MySqlConnectionStringOption<uint>(
				keys: new[] { "Connection Idle Timeout", "ConnectionIdleTimeout" },
				defaultValue: 180));

			AddOption(MinimumPoolSize = new MySqlConnectionStringOption<uint>(
				keys: new[] { "Minimum Pool Size", "Min Pool Size", "MinimumPoolSize", "minpoolsize" },
				defaultValue: 0));

			AddOption(MaximumPoolSize = new MySqlConnectionStringOption<uint>(
				keys: new[] { "Maximum Pool Size", "Max Pool Size", "MaximumPoolSize", "maxpoolsize" },
				defaultValue: 100));

			// Other Options
			AddOption(AllowPublicKeyRetrieval = new MySqlConnectionStringOption<bool>(
				keys: new[] { "AllowPublicKeyRetrieval", "Allow Public Key Retrieval" },
				defaultValue: false));

			AddOption(AllowUserVariables = new MySqlConnectionStringOption<bool>(
				keys: new[] { "AllowUserVariables", "Allow User Variables" },
				defaultValue: false));

			AddOption(AllowZeroDateTime = new MySqlConnectionStringOption<bool>(
				keys: new[] { "AllowZeroDateTime", "Allow Zero DateTime" },
				defaultValue: false));

			AddOption(ApplicationName = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "ApplicationName", "Application Name" },
				defaultValue: null));

			AddOption(AutoEnlist = new MySqlConnectionStringOption<bool>(
				keys: new[] { "AutoEnlist", "Auto Enlist" },
				defaultValue: true));

			AddOption(CharacterSet = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "CharSet", "Character Set", "CharacterSet" },
				defaultValue: ""));

			AddOption(ConnectionTimeout = new MySqlConnectionStringOption<uint>(
				keys: new[] { "Connection Timeout", "ConnectionTimeout", "Connect Timeout" },
				defaultValue: 15u));

			AddOption(ConvertZeroDateTime = new MySqlConnectionStringOption<bool>(
				keys: new[] { "Convert Zero Datetime", "ConvertZeroDateTime" },
				defaultValue: false));

			AddOption(DateTimeKind = new MySqlConnectionStringOption<MySqlDateTimeKind>(
				keys: new[] { "DateTimeKind" },
				defaultValue: MySqlDateTimeKind.Unspecified));

			AddOption(DefaultCommandTimeout = new MySqlConnectionStringOption<uint>(
				keys: new[] { "Default Command Timeout", "DefaultCommandTimeout", "Command Timeout" },
				defaultValue: 30u));

			AddOption(ForceSynchronous = new MySqlConnectionStringOption<bool>(
				keys: new[] { "ForceSynchronous" },
				defaultValue: false));

			AddOption(GuidFormat = new MySqlConnectionStringOption<MySqlGuidFormat>(
				keys: new[] { "GuidFormat" },
				defaultValue: MySqlGuidFormat.Default));

			AddOption(IgnoreCommandTransaction = new MySqlConnectionStringOption<bool>(
				keys: new[] { "IgnoreCommandTransaction", "Ignore Command Transaction" },
				defaultValue: false));

			AddOption(IgnorePrepare = new MySqlConnectionStringOption<bool>(
				keys: new[] { "IgnorePrepare", "Ignore Prepare" },
				defaultValue: true));

			AddOption(InteractiveSession = new MySqlConnectionStringOption<bool>(
				keys: new[] { "InteractiveSession", "Interactive", "Interactive Session" },
				defaultValue: false));

			AddOption(Keepalive = new MySqlConnectionStringOption<uint>(
				keys: new[] { "Keep Alive", "Keepalive" },
				defaultValue: 0u));

			AddOption(OldGuids = new MySqlConnectionStringOption<bool>(
				keys: new[] { "Old Guids", "OldGuids" },
				defaultValue: false));

			AddOption(PersistSecurityInfo = new MySqlConnectionStringOption<bool>(
				keys: new[] { "Persist Security Info", "PersistSecurityInfo" },
				defaultValue: false));

			AddOption(ServerRsaPublicKeyFile = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "ServerRSAPublicKeyFile", "Server RSA Public Key File" },
				defaultValue: null));

			AddOption(ServerSPN = new MySqlConnectionStringNullableOption<string>(
				keys: new[] { "Server SPN", "ServerSPN" },
				defaultValue: null));

			AddOption(TreatTinyAsBoolean = new MySqlConnectionStringOption<bool>(
				keys: new[] { "Treat Tiny As Boolean", "TreatTinyAsBoolean" },
				defaultValue: true));

			AddOption(UseAffectedRows = new MySqlConnectionStringOption<bool>(
				keys: new[] { "Use Affected Rows", "UseAffectedRows" },
				defaultValue: false));

			AddOption(UseCompression = new MySqlConnectionStringOption<bool>(
				keys: new[] { "Compress", "Use Compression", "UseCompression" },
				defaultValue: false));

			AddOption(UseXaTransactions = new MySqlConnectionStringOption<bool>(
				keys: new[] { "Use XA Transactions", "UseXaTransactions" },
				defaultValue: true));
		}

		static readonly Dictionary<string, MySqlConnectionStringOption> s_options;

		readonly IReadOnlyList<string> m_keys;
	}

	internal sealed class MySqlConnectionStringOption<T> : MySqlConnectionStringOption
		where T : struct
	{
		public MySqlConnectionStringOption(IReadOnlyList<string> keys, T defaultValue, Func<T, T>? coerce = null)
			: base(keys)
		{
			m_defaultValue = defaultValue;
			m_coerce = coerce;
		}

		public T GetValue(MySqlConnectionStringBuilder builder) =>
			builder.TryGetValue(Key, out var objectValue) ? ChangeType(objectValue) : m_defaultValue;

		public void SetValue(MySqlConnectionStringBuilder builder, T value)
		{
			builder[Key] = m_coerce != null ? m_coerce(value) : value;
		}

		public override object? GetObject(MySqlConnectionStringBuilder builder)
		{
			return GetValue(builder);
		}

		private static T ChangeType(object objectValue)
		{
			if (typeof(T) == typeof(bool) && objectValue is string booleanString)
			{
				if (string.Equals(booleanString, "yes", StringComparison.OrdinalIgnoreCase))
					return (T) (object) true;
				if (string.Equals(booleanString, "no", StringComparison.OrdinalIgnoreCase))
					return (T) (object) false;
			}

			if ((typeof(T) == typeof(MySqlLoadBalance) || typeof(T) == typeof(MySqlSslMode) || typeof(T) == typeof(MySqlDateTimeKind) || typeof(T) == typeof(MySqlGuidFormat) || typeof(T) == typeof(MySqlConnectionProtocol) || typeof(T) == typeof(MySqlCertificateStoreLocation)) && objectValue is string enumString)
			{
				try
				{
					return (T) Enum.Parse(typeof(T), enumString, ignoreCase: true);
				}
				catch (Exception ex)
				{
					throw new InvalidOperationException("Value '{0}' not supported for option '{1}'.".FormatInvariant(objectValue, typeof(T).Name), ex);
				}
			}

			return (T) Convert.ChangeType(objectValue, typeof(T), CultureInfo.InvariantCulture);
		}

		readonly T m_defaultValue;
		readonly Func<T, T>? m_coerce;
	}

	internal sealed class MySqlConnectionStringNullableOption<T> : MySqlConnectionStringOption
		where T : class
	{
		public MySqlConnectionStringNullableOption(IReadOnlyList<string> keys, T? defaultValue)
			: base(keys)
		{
			m_defaultValue = defaultValue;
		}

		public T? GetValue(MySqlConnectionStringBuilder builder) => builder.TryGetValue(Key, out var objectValue) ? ChangeType(objectValue) : m_defaultValue;
		public void SetValue(MySqlConnectionStringBuilder builder, T? value) => builder[Key] = value;
		public override object? GetObject(MySqlConnectionStringBuilder builder) => GetValue(builder);
		private static T? ChangeType(object objectValue) => (T?) Convert.ChangeType(objectValue, typeof(T), CultureInfo.InvariantCulture);

		readonly T? m_defaultValue;
	}
}
