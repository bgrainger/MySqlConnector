using System;
using MySql.Data.MySqlClient;
using Xunit;

namespace MySqlConnector.Tests
{
	public class MySqlConnectionStringBuilderTests
	{
		[Fact]
		public void Defaults()
		{
			var csb = new MySqlConnectionStringBuilder();
			Assert.False(csb.AllowPublicKeyRetrieval);
			Assert.False(csb.AllowUserVariables);
			Assert.False(csb.AllowZeroDateTime);
			Assert.True(csb.AutoEnlist);
			Assert.Null(csb.CertificateFile);
			Assert.Null(csb.CertificatePassword);
			Assert.Equal(MySqlCertificateStoreLocation.None, csb.CertificateStoreLocation);
			Assert.Null(csb.CertificateThumbprint);
			Assert.Equal("", csb.CharacterSet);
			Assert.Equal(0u, csb.ConnectionLifeTime);
			Assert.Equal(MySqlConnectionProtocol.Sockets, csb.ConnectionProtocol);
#if BASELINE
			Assert.False(csb.ConnectionReset);
#else
			Assert.True(csb.ConnectionReset);
#endif
			Assert.Equal(15u, csb.ConnectionTimeout);
			Assert.False(csb.ConvertZeroDateTime);
#if !BASELINE
			Assert.Equal(MySqlDateTimeKind.Unspecified, csb.DateTimeKind);
#endif
			Assert.Equal("", csb.Database);
			Assert.Equal(30u, csb.DefaultCommandTimeout);
#if !BASELINE
			Assert.Null(csb.ApplicationName);
			Assert.Equal(0u, csb.ConnectionIdlePingTime);
			Assert.Equal(180u, csb.ConnectionIdleTimeout);
			Assert.False(csb.ForceSynchronous);
			Assert.Equal(MySqlGuidFormat.Default, csb.GuidFormat);
			Assert.False(csb.IgnoreCommandTransaction);
			Assert.Null(csb.CACertificateFile);
			Assert.Equal(MySqlLoadBalance.RoundRobin, csb.LoadBalance);
#endif
			Assert.True(csb.IgnorePrepare);
			Assert.False(csb.InteractiveSession);
			Assert.Equal(0u, csb.Keepalive);
			Assert.Equal(100u, csb.MaximumPoolSize);
			Assert.Equal(0u, csb.MinimumPoolSize);
			Assert.Equal("", csb.Password);
			Assert.Equal("MYSQL", csb.PipeName);
			Assert.False(csb.OldGuids);
			Assert.False(csb.PersistSecurityInfo);
			Assert.True(csb.Pooling);
			Assert.Equal(3306u, csb.Port);
			Assert.Equal("", csb.Server);
#if !BASELINE
			Assert.Null(csb.ServerRsaPublicKeyFile);
			Assert.Null(csb.ServerSPN);
#endif
			Assert.Equal(MySqlSslMode.Preferred, csb.SslMode);
			Assert.True(csb.TreatTinyAsBoolean);
			Assert.False(csb.UseCompression);
			Assert.Equal("", csb.UserID);
			Assert.False(csb.UseAffectedRows);
#if !BASELINE
			Assert.True(csb.UseXaTransactions);
#endif
		}

		[Fact]
		public void ParseConnectionString()
		{
			var csb = new MySqlConnectionStringBuilder
			{
				ConnectionString = "Data Source=db-server;" +
					"Initial Catalog=schema_name;" +
					"allowpublickeyretrieval = true;" +
					"Allow User Variables=true;" +
					"allow zero datetime=true;" +
					"auto enlist=False;" +
					"certificate file=file.pfx;" +
					"certificate password=Pass1234;" +
					"certificate store location=CurrentUser;" +
					"certificate thumb print=thumbprint123;" +
					"Character Set=latin1;" +
					"Compress=true;" +
					"connect timeout=30;" +
					"connection lifetime=15;" +
					"ConnectionReset=false;" +
					"Convert Zero Datetime=true;" +
#if !BASELINE
					"datetimekind=utc;" +
#endif
					"default command timeout=123;" +
#if !BASELINE
					"application name=My Test Application;" +
					"connection idle ping time=60;" +
					"connectionidletimeout=30;" +
					"forcesynchronous=true;" +
					"ignore command transaction=true;" +
					"ca certificate file=ca.pem;" +
					"server rsa public key file=rsa.pem;" +
					"load balance=random;" +
					"guidformat=timeswapbinary16;" +
					"server spn=mariadb/host.example.com@EXAMPLE.COM;" +
					"use xa transactions=false;" +
#endif
					"ignore prepare=false;" +
					"interactive=true;" +
					"Keep Alive=90;" +
					"minpoolsize=5;" +
					"maxpoolsize=15;" +
					"OldGuids=true;" +
					"persistsecurityinfo=yes;" +
					"Pipe=MyPipe;" +
					"Pooling=no;" +
					"Port=1234;" +
					"protocol=pipe;" +
					"pwd=Pass1234;" +
					"Treat Tiny As Boolean=false;" +
					"ssl mode=verifyca;" +
					"Uid=username;" +
					"useaffectedrows=true"
			};
			Assert.True(csb.AllowPublicKeyRetrieval);
			Assert.True(csb.AllowUserVariables);
			Assert.True(csb.AllowZeroDateTime);
			Assert.False(csb.AutoEnlist);
			Assert.Equal("file.pfx", csb.CertificateFile);
			Assert.Equal("Pass1234", csb.CertificatePassword);
			Assert.Equal(MySqlCertificateStoreLocation.CurrentUser, csb.CertificateStoreLocation);
			Assert.Equal("thumbprint123", csb.CertificateThumbprint);
			Assert.Equal("latin1", csb.CharacterSet);
			Assert.Equal(15u, csb.ConnectionLifeTime);
			Assert.Equal(MySqlConnectionProtocol.NamedPipe, csb.ConnectionProtocol);
			Assert.False(csb.ConnectionReset);
			Assert.Equal(30u, csb.ConnectionTimeout);
			Assert.True(csb.ConvertZeroDateTime);
#if !BASELINE
			Assert.Equal(MySqlDateTimeKind.Utc, csb.DateTimeKind);
#endif
			Assert.Equal("schema_name", csb.Database);
			Assert.Equal(123u, csb.DefaultCommandTimeout);
#if !BASELINE
			Assert.Equal("My Test Application", csb.ApplicationName);
			Assert.Equal(60u, csb.ConnectionIdlePingTime);
			Assert.Equal(30u, csb.ConnectionIdleTimeout);
			Assert.True(csb.ForceSynchronous);
			Assert.True(csb.IgnoreCommandTransaction);
			Assert.Equal("ca.pem", csb.CACertificateFile);
			Assert.Equal("rsa.pem", csb.ServerRsaPublicKeyFile);
			Assert.Equal(MySqlLoadBalance.Random, csb.LoadBalance);
			Assert.Equal(MySqlGuidFormat.TimeSwapBinary16, csb.GuidFormat);
			Assert.Equal("mariadb/host.example.com@EXAMPLE.COM", csb.ServerSPN);
			Assert.False(csb.UseXaTransactions);
#endif
			Assert.False(csb.IgnorePrepare);
			Assert.True(csb.InteractiveSession);
			Assert.Equal(90u, csb.Keepalive);
			Assert.Equal(15u, csb.MaximumPoolSize);
			Assert.Equal(5u, csb.MinimumPoolSize);
			Assert.Equal("Pass1234", csb.Password);
			Assert.Equal("MyPipe", csb.PipeName);
			Assert.True(csb.OldGuids);
			Assert.True(csb.PersistSecurityInfo);
			Assert.False(csb.Pooling);
			Assert.Equal(1234u, csb.Port);
			Assert.Equal("db-server", csb.Server);
			Assert.False(csb.TreatTinyAsBoolean);
			Assert.Equal(MySqlSslMode.VerifyCA, csb.SslMode);
			Assert.True(csb.UseAffectedRows);
			Assert.True(csb.UseCompression);
			Assert.Equal("username", csb.UserID);
		}

#if !BASELINE
		[Fact]
		public void EnumInvalidOperation()
		{
			var csb = new MySqlConnectionStringBuilder("ssl mode=invalid;");
			Assert.Throws<InvalidOperationException>(() => csb.SslMode);
		}

		[Fact]
		public void ConstructWithNull()
		{
			var csb = new MySqlConnectionStringBuilder(default(string));
			Assert.Equal("", csb.ConnectionString);
		}
#endif

		[Fact]
		public void ConstructWithEmptyString()
		{
			var csb = new MySqlConnectionStringBuilder("");
			Assert.Equal("", csb.ConnectionString);
		}

		[Fact]
		public void SetConnectionStringToNull()
		{
			var csb = new MySqlConnectionStringBuilder
			{
				ConnectionString = null,
			};
			Assert.Equal("", csb.ConnectionString);
		}

		[Fact]
		public void SetConnectionStringToEmptyString()
		{
			var csb = new MySqlConnectionStringBuilder
			{
				ConnectionString = "",
			};
			Assert.Equal("", csb.ConnectionString);
		}
	}
}
