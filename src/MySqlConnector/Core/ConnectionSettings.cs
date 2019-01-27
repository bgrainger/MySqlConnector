using System;
using System.Collections.Generic;
using System.IO;
using MySql.Data.MySqlClient;
using MySqlConnector.Utilities;

#nullable disable

namespace MySqlConnector.Core
{
	internal sealed class ConnectionSettings
	{
		public ConnectionSettings(MySqlConnectionStringBuilder csb)
		{
			ConnectionStringBuilder = csb;
			ConnectionString = csb.ConnectionString;

			if (csb.ConnectionProtocol == MySqlConnectionProtocol.UnixSocket || (!Utility.IsWindows() && (csb.Server.StartsWith("/", StringComparison.Ordinal) || csb.Server.StartsWith("./", StringComparison.Ordinal))))
			{
				if (!File.Exists(csb.Server))
					throw new MySqlException("Cannot find Unix Socket at " + csb.Server);
				ConnectionProtocol = MySqlConnectionProtocol.UnixSocket;
				UnixSocket = Path.GetFullPath(csb.Server);
			}
			else if (csb.ConnectionProtocol == MySqlConnectionProtocol.NamedPipe)
			{
				ConnectionProtocol = MySqlConnectionProtocol.NamedPipe;
				HostNames = (csb.Server == "." || string.Equals(csb.Server, "localhost", StringComparison.OrdinalIgnoreCase)) ? s_localhostPipeServer : new[] { csb.Server };
				PipeName = csb.PipeName;
			}
			else if (csb.ConnectionProtocol == MySqlConnectionProtocol.SharedMemory)
			{
				throw new NotSupportedException("Shared Memory connections are not supported");
			}
			else
			{
				ConnectionProtocol = MySqlConnectionProtocol.Sockets;
				HostNames = csb.Server.Split(',');
				LoadBalance = csb.LoadBalance;
				Port = (int) csb.Port;
			}

			UserID = csb.UserID;
			Password = csb.Password;
			Database = csb.Database;

			// SSL/TLS Options
			SslMode = csb.SslMode;
			CertificateFile = csb.CertificateFile;
			CertificatePassword = csb.CertificatePassword;
			CACertificateFile = csb.CACertificateFile;
			CertificateStoreLocation = csb.CertificateStoreLocation;
			CertificateThumbprint = csb.CertificateThumbprint;

			// Connection Pooling Options
			Pooling = csb.Pooling;
			ConnectionLifeTime = Math.Min(csb.ConnectionLifeTime, uint.MaxValue / 1000) * 1000;
			ConnectionReset = csb.ConnectionReset;
			ConnectionIdlePingTime = Math.Min(csb.ConnectionIdlePingTime, uint.MaxValue / 1000) * 1000;
			ConnectionIdleTimeout = (int) csb.ConnectionIdleTimeout;
			if (csb.MinimumPoolSize > csb.MaximumPoolSize)
				throw new MySqlException("MaximumPoolSize must be greater than or equal to MinimumPoolSize");
			MinimumPoolSize = (int) csb.MinimumPoolSize;
			MaximumPoolSize = (int) csb.MaximumPoolSize;

			// Other Options
			AllowPublicKeyRetrieval = csb.AllowPublicKeyRetrieval;
			AllowUserVariables = csb.AllowUserVariables;
			AllowZeroDateTime = csb.AllowZeroDateTime;
			ApplicationName = csb.ApplicationName;
			AutoEnlist = csb.AutoEnlist;
			ConnectionTimeout = (int) csb.ConnectionTimeout;
			ConvertZeroDateTime = csb.ConvertZeroDateTime;
			DateTimeKind = (DateTimeKind) csb.DateTimeKind;
			DefaultCommandTimeout = (int) csb.DefaultCommandTimeout;
			ForceSynchronous = csb.ForceSynchronous;
			IgnoreCommandTransaction = csb.IgnoreCommandTransaction;
			IgnorePrepare = csb.IgnorePrepare;
			InteractiveSession = csb.InteractiveSession;
			GuidFormat = GetEffectiveGuidFormat(csb.GuidFormat, csb.OldGuids);
			Keepalive = csb.Keepalive;
			PersistSecurityInfo = csb.PersistSecurityInfo;
			ServerRsaPublicKeyFile = csb.ServerRsaPublicKeyFile;
			ServerSPN = csb.ServerSPN;
			TreatTinyAsBoolean = csb.TreatTinyAsBoolean;
			UseAffectedRows = csb.UseAffectedRows;
			UseCompression = csb.UseCompression;
			UseXaTransactions = csb.UseXaTransactions;
		}

		private static MySqlGuidFormat GetEffectiveGuidFormat(MySqlGuidFormat guidFormat, bool oldGuids)
		{
			switch (guidFormat)
			{
			case MySqlGuidFormat.Default:
				return oldGuids ? MySqlGuidFormat.LittleEndianBinary16 : MySqlGuidFormat.Char36;
			case MySqlGuidFormat.None:
			case MySqlGuidFormat.Char36:
			case MySqlGuidFormat.Char32:
			case MySqlGuidFormat.Binary16:
			case MySqlGuidFormat.TimeSwapBinary16:
			case MySqlGuidFormat.LittleEndianBinary16:
				if (oldGuids)
					throw new MySqlException("OldGuids cannot be used with GuidFormat");
				return guidFormat;
			default:
				throw new MySqlException("Unknown GuidFormat");
			}
		}

		/// <summary>
		/// The <see cref="MySqlConnectionStringBuilder" /> that was used to create this <see cref="ConnectionSettings" />.!--
		/// This object must not be mutated.
		/// </summary>
		public MySqlConnectionStringBuilder ConnectionStringBuilder { get; }

		// Base Options
		public string ConnectionString { get; }
		public MySqlConnectionProtocol ConnectionProtocol { get; }
		public IReadOnlyList<string> HostNames { get; }
		public MySqlLoadBalance LoadBalance { get; }
		public int Port { get; }
		public string PipeName { get; }
		public string UnixSocket { get; }
		public string UserID { get; }
		public string Password { get; }
		public string Database { get; }

		// SSL/TLS Options
		public MySqlSslMode SslMode { get; }
		public string CertificateFile { get; }
		public string CertificatePassword { get; }
		public string CACertificateFile { get; }
		public MySqlCertificateStoreLocation CertificateStoreLocation { get; }
		public string CertificateThumbprint { get; }

		// Connection Pooling Options
		public bool Pooling { get; }
		public uint ConnectionLifeTime { get; }
		public bool ConnectionReset { get; }
		public uint ConnectionIdlePingTime { get; }
		public int ConnectionIdleTimeout { get; }
		public int MinimumPoolSize { get; }
		public int MaximumPoolSize { get; }

		// Other Options
		public bool AllowPublicKeyRetrieval { get; }
		public bool AllowUserVariables { get; }
		public bool AllowZeroDateTime { get; }
		public string ApplicationName { get; }
		public bool AutoEnlist { get; }
		public int ConnectionTimeout { get; }
		public bool ConvertZeroDateTime { get; }
		public DateTimeKind DateTimeKind { get; }
		public int DefaultCommandTimeout { get; }
		public bool ForceSynchronous { get; }
		public MySqlGuidFormat GuidFormat { get; }
		public bool IgnoreCommandTransaction { get; }
		public bool IgnorePrepare { get; }
		public bool InteractiveSession { get; }
		public uint Keepalive { get; }
		public bool PersistSecurityInfo { get; }
		public string ServerRsaPublicKeyFile { get; }
		public string ServerSPN { get; }
		public bool TreatTinyAsBoolean { get; }
		public bool UseAffectedRows { get; }
		public bool UseCompression { get; }
		public bool UseXaTransactions { get; }

		public byte[] ConnectionAttributes { get; set; }

		// Helper Functions
		int? m_connectionTimeoutMilliseconds;
		public int ConnectionTimeoutMilliseconds
		{
			get
			{
				if (!m_connectionTimeoutMilliseconds.HasValue)
				{
					try
					{
						checked
						{
							m_connectionTimeoutMilliseconds = ConnectionTimeout * 1000;
						}
					}
					catch (OverflowException)
					{
						m_connectionTimeoutMilliseconds = Int32.MaxValue;
					}
				}
				return m_connectionTimeoutMilliseconds.Value;
			}
		}

		static readonly string[] s_localhostPipeServer = { "." };
	}
}
