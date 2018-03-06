using System;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Logging;
using MySqlConnector.Protocol;
using MySqlConnector.Protocol.Payloads;
using MySqlConnector.Protocol.Serialization;
using MySqlConnector.Utilities;

namespace MySqlConnector.Core
{
	internal sealed class ServerSession
	{
		public ServerSession()
			: this(null, 0, Interlocked.Increment(ref s_lastId))
		{
		}

		public ServerSession(ConnectionPool pool, int poolGeneration, int id)
		{
			m_lock = new object();
			m_payloadCache = new ArraySegmentHolder<byte>();
			Id = (pool?.Id ?? 0) + "." + id;
			CreatedUtc = DateTime.UtcNow;
			Pool = pool;
			PoolGeneration = poolGeneration;
			HostName = "";
			m_logArguments = new object[] { "Session{0}".FormatInvariant(Id), null };
			Log.Debug("{0} created new session", m_logArguments);
		}

		public string Id { get; }
		public ServerVersion ServerVersion { get; set; }
		public int ConnectionId { get; set; }
		public byte[] AuthPluginData { get; set; }
		public DateTime CreatedUtc { get; }
		public ConnectionPool Pool { get; }
		public int PoolGeneration { get; }
		public DateTime LastReturnedUtc { get; private set; }
		public string DatabaseOverride { get; set; }
		public string HostName { get; private set; }
		public IPAddress IPAddress => (m_tcpClient?.Client.RemoteEndPoint as IPEndPoint)?.Address;
		public WeakReference<MySqlConnection> OwningConnection { get; set; }
		public bool SupportsDeprecateEof => m_supportsDeprecateEof;
		public bool SupportsOptionalResultSetMetadata => m_supportsOptionalResultSetMetadata;

		public void ReturnToPool()
		{
			if (Log.IsDebugEnabled())
			{
				m_logArguments[1] = Pool?.Id;
				Log.Debug("{0} returning to Pool{1}", m_logArguments);
			}
			LastReturnedUtc = DateTime.UtcNow;
			Pool?.Return(this);
		}

		public bool IsConnected
		{
			get
			{
				lock (m_lock)
					return m_state == State.Connected;
			}
		}

		public bool TryStartCancel(MySqlCommand command)
		{
			lock (m_lock)
			{
				if (m_activeCommandId != command.CommandId)
					return false;
				VerifyState(State.Querying, State.CancelingQuery, State.Failed);
				if (m_state != State.Querying)
					return false;
				if (command.CancelAttemptCount++ >= 10)
					return false;
				m_state = State.CancelingQuery;
			}

			Log.Info("{0} will cancel command {1} (attempt #{2}): {3}", m_logArguments[0], command.CommandId, command.CancelAttemptCount, command.CommandText);
			return true;
		}

		public void DoCancel(MySqlCommand commandToCancel, MySqlCommand killCommand)
		{
			Log.Info("{0} canceling command {1}: {2}", m_logArguments[0], commandToCancel.CommandId, commandToCancel.CommandText);
			lock (m_lock)
			{
				if (m_activeCommandId != commandToCancel.CommandId)
					return;

				// NOTE: This command is executed while holding the lock to prevent race conditions during asynchronous cancellation.
				// For example, if the lock weren't held, the current command could finish and the other thread could set m_activeCommandId
				// to zero, then start executing a new command. By the time this "KILL QUERY" command reached the server, the wrong
				// command would be killed (because "KILL QUERY" specifies the connection whose command should be killed, not
				// a unique identifier of the command itself). As a mitigation, we set the CommandTimeout to a low value to avoid
				// blocking the other thread for an extended duration.
				killCommand.CommandTimeout = 3;
				killCommand.ExecuteNonQuery();
			}
		}

		public void AbortCancel(MySqlCommand command)
		{
			lock (m_lock)
			{
				if (m_activeCommandId == command.CommandId && m_state == State.CancelingQuery)
					m_state = State.Querying;
			}
		}

		public void StartQuerying(MySqlCommand command)
		{
			lock (m_lock)
			{
				if (m_state == State.Querying || m_state == State.CancelingQuery)
				{
					m_logArguments[1] = m_state;
					Log.Error("{0} can't execute new command when state is {1}: {2}", m_logArguments[0], m_state, command.CommandText);
					throw new InvalidOperationException("There is already an open DataReader associated with this Connection which must be closed first.");
				}

				VerifyState(State.Connected);
				m_state = State.Querying;
				command.CancelAttemptCount = 0;
				m_activeCommandId = command.CommandId;
			}
		}

		public void FinishQuerying()
		{
			m_logArguments[1] = m_state;
			Log.Debug("{0} entering FinishQuerying; state = {1}", m_logArguments);
			bool clearConnection = false;
			lock (m_lock)
			{
				if (m_state == State.CancelingQuery)
				{
					m_state = State.ClearingPendingCancellation;
					clearConnection = true;
				}
			}

			if (clearConnection)
			{
				// KILL QUERY will kill a subsequent query if the command it was intended to cancel has already completed.
				// In order to handle this case, we issue a dummy query that will consume the pending cancellation.
				// See https://bugs.mysql.com/bug.php?id=45679
				Log.Info("{0} sending 'DO SLEEP(0)' command to clear pending cancellation", m_logArguments);
				var payload = QueryPayload.Create("DO SLEEP(0);");
				SendAsync(payload, IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
				payload = ReceiveReplyAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
				OkPayload.Create(payload);
			}

			lock (m_lock)
			{
				if (m_state == State.Querying || m_state == State.ClearingPendingCancellation)
					m_state = State.Connected;
				else
					VerifyState(State.Failed);
				m_activeCommandId = 0;
			}
		}

		public void SetTimeout(int timeoutMilliseconds) => m_payloadHandler.ByteHandler.RemainingTimeout = timeoutMilliseconds;

		public async Task DisposeAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (m_payloadHandler != null)
			{
				// attempt to gracefully close the connection, ignoring any errors (it may have been closed already by the server, etc.)
				State state;
				lock (m_lock)
				{
					if (m_state == State.Connected || m_state == State.Failed)
						m_state = State.Closing;
					state = m_state;
				}

				if (state == State.Closing)
				{
					try
					{
						Log.Info("{0} sending QUIT command", m_logArguments);
						m_payloadHandler.StartNewConversation();
						await m_payloadHandler.WritePayloadAsync(QuitPayload.Instance.ArraySegment, ioBehavior).ConfigureAwait(false);
					}
					catch (IOException)
					{
					}
					catch (ObjectDisposedException)
					{
					}
					catch (SocketException)
					{
					}
				}
			}

			ShutdownSocket();
			lock (m_lock)
				m_state = State.Closed;
		}

		public async Task ConnectAsync(ConnectionSettings cs, ILoadBalancer loadBalancer, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			try
			{
				lock (m_lock)
				{
					VerifyState(State.Created);
					m_state = State.Connecting;
				}
				var connected = false;
				if (cs.ConnectionType == ConnectionType.Tcp)
					connected = await OpenTcpSocketAsync(cs, loadBalancer, ioBehavior, cancellationToken).ConfigureAwait(false);
				else if (cs.ConnectionType == ConnectionType.Unix)
					connected = await OpenUnixSocketAsync(cs, ioBehavior, cancellationToken).ConfigureAwait(false);
				if (!connected)
				{
					lock (m_lock)
						m_state = State.Failed;
					Log.Error("{0} connecting failed", m_logArguments);
					throw new MySqlException("Unable to connect to any of the specified MySQL hosts.");
				}

				var byteHandler = new SocketByteHandler(m_socket);
				m_payloadHandler = new StandardPayloadHandler(byteHandler);

				var payload = await ReceiveAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				var initialHandshake = InitialHandshakePayload.Create(payload);

				// if PluginAuth is supported, then use the specified auth plugin; else, fall back to protocol capabilities to determine the auth type to use
				string authPluginName;
				if ((initialHandshake.ProtocolCapabilities & ProtocolCapabilities.PluginAuth) != 0)
					authPluginName = initialHandshake.AuthPluginName;
				else
					authPluginName = (initialHandshake.ProtocolCapabilities & ProtocolCapabilities.SecureConnection) == 0 ? "mysql_old_password" : "mysql_native_password";
				m_logArguments[1] = authPluginName;
				Log.Debug("{0} server sent auth_plugin_name '{1}'", m_logArguments);
				if (authPluginName != "mysql_native_password" && authPluginName != "sha256_password" && authPluginName != "caching_sha2_password")
				{
					Log.Error("{0} unsupported authentication method '{1}'", m_logArguments);
					throw new NotSupportedException("Authentication method '{0}' is not supported.".FormatInvariant(initialHandshake.AuthPluginName));
				}

				ServerVersion = new ServerVersion(Encoding.ASCII.GetString(initialHandshake.ServerVersion));
				ConnectionId = initialHandshake.ConnectionId;
				AuthPluginData = initialHandshake.AuthPluginData;
				m_useCompression = cs.UseCompression && (initialHandshake.ProtocolCapabilities & ProtocolCapabilities.Compress) != 0;

				m_supportsConnectionAttributes = (initialHandshake.ProtocolCapabilities & ProtocolCapabilities.ConnectionAttributes) != 0;
				m_supportsDeprecateEof = (initialHandshake.ProtocolCapabilities & ProtocolCapabilities.DeprecateEof) != 0;
				m_supportsOptionalResultSetMetadata = (initialHandshake.ProtocolCapabilities & ProtocolCapabilities.OptionalResultSetMetadata) != 0;
				var serverSupportsSsl = (initialHandshake.ProtocolCapabilities & ProtocolCapabilities.Ssl) != 0;

				Log.Info("{0} made connection; ServerVersion={1}; ConnectionId={2}; Flags: {3}{4}{5}{6}", m_logArguments[0], ServerVersion.OriginalString, ConnectionId,
					m_useCompression ? "Cmp " :"", m_supportsConnectionAttributes ? "Attr " : "", m_supportsDeprecateEof ? "" : "Eof ", serverSupportsSsl ? "Ssl " : "");

				if (cs.SslMode != MySqlSslMode.None && (cs.SslMode != MySqlSslMode.Preferred || serverSupportsSsl))
				{
					if (!serverSupportsSsl)
					{
						Log.Error("{0} requires SSL but server doesn't support it", m_logArguments);
						throw new MySqlException("Server does not support SSL");
					}
					await InitSslAsync(initialHandshake.ProtocolCapabilities, cs, ioBehavior, cancellationToken).ConfigureAwait(false);
				}

				if (m_supportsConnectionAttributes && s_connectionAttributes == null)
					s_connectionAttributes = CreateConnectionAttributes();

				payload = HandshakeResponse41Payload.Create(initialHandshake, cs, m_useCompression, m_supportsConnectionAttributes ? s_connectionAttributes : null);
				await SendReplyAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
				payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

				// if server doesn't support the authentication fast path, it will send a new challenge
				if (payload.HeaderByte == AuthenticationMethodSwitchRequestPayload.Signature)
				{
					payload = await SwitchAuthenticationAsync(cs, payload, ioBehavior, cancellationToken).ConfigureAwait(false);
				}

				OkPayload.Create(payload);

				if (m_useCompression)
					m_payloadHandler = new CompressedPayloadHandler(m_payloadHandler.ByteHandler);

				if (ShouldGetRealServerDetails())
					await GetRealServerDetailsAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);

				// HACK: turn off all result set metadata on this connection
				if (SupportsOptionalResultSetMetadata)
				{ 
					await SendAsync(QueryPayload.Create("SET @@resultset_metadata=NONE;"), ioBehavior, CancellationToken.None).ConfigureAwait(false);
					payload = await ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
					OkPayload.Create(payload);
				}
			}
			catch (IOException ex)
			{
				m_logArguments[1] = ex.Message;
				Log.Error("{0} couldn't connect to server: {1}", m_logArguments);
				throw new MySqlException("Couldn't connect to server", ex);
			}
		}

		public async Task<bool> TryResetConnectionAsync(ConnectionSettings cs, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			VerifyState(State.Connected);

			try
			{
				if (ServerVersion.Version.CompareTo(ServerVersions.SupportsResetConnection) >= 0)
				{
					m_logArguments[1] = ServerVersion.OriginalString;
					Log.Debug("{0} ServerVersion {1} supports reset connection; sending reset connection request", m_logArguments);
					await SendAsync(ResetConnectionPayload.Instance, ioBehavior, cancellationToken).ConfigureAwait(false);
					var payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
					OkPayload.Create(payload);

					// the "reset connection" packet also resets the connection charset, so we need to change that back to our default
					await SendAsync(s_setNamesUtf8mb4Payload, ioBehavior, cancellationToken).ConfigureAwait(false);
					payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
					OkPayload.Create(payload);
				}
				else
				{
					// optimistically hash the password with the challenge from the initial handshake (supported by MariaDB; doesn't appear to be supported by MySQL)
					m_logArguments[1] = ServerVersion.OriginalString;
					Log.Debug("{0} ServerVersion {1} doesn't support reset connection; sending change user request", m_logArguments);
					var hashedPassword = AuthenticationUtility.CreateAuthenticationResponse(AuthPluginData, 0, cs.Password);
					var payload = ChangeUserPayload.Create(cs.UserID, hashedPassword, cs.Database, m_supportsConnectionAttributes ? s_connectionAttributes : null);
					await SendAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
					payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
					if (payload.HeaderByte == AuthenticationMethodSwitchRequestPayload.Signature)
					{
						Log.Debug("{0} optimistic reauthentication failed; logging in again", m_logArguments);
						payload = await SwitchAuthenticationAsync(cs, payload, ioBehavior, cancellationToken).ConfigureAwait(false);
					}
					OkPayload.Create(payload);
				}

				return true;
			}
			catch (IOException ex)
			{
				Log.Debug(ex, "{0} ignoring IOException in TryResetConnectionAsync", m_logArguments);
			}
			catch (SocketException ex)
			{
				Log.Debug(ex, "{0} ignoring SocketException in TryResetConnectionAsync", m_logArguments);
			}

			return false;
		}

		private async Task<PayloadData> SwitchAuthenticationAsync(ConnectionSettings cs, PayloadData payload, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			// if the server didn't support the hashed password; rehash with the new challenge
			var switchRequest = AuthenticationMethodSwitchRequestPayload.Create(payload);
			m_logArguments[1] = switchRequest.Name;
			Log.Debug("{0} switching authentication method to '{1}'", m_logArguments);
			switch (switchRequest.Name)
			{
			case "mysql_native_password":
				AuthPluginData = switchRequest.Data;
				var hashedPassword = AuthenticationUtility.CreateAuthenticationResponse(AuthPluginData, 0, cs.Password);
				payload = new PayloadData(hashedPassword);
				await SendReplyAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
				return await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

			case "mysql_clear_password":
				if (!m_isSecureConnection)
				{
					Log.Error("{0} needs a secure connection to use '{1}'", m_logArguments);
					throw new MySqlException("Authentication method '{0}' requires a secure connection.".FormatInvariant(switchRequest.Name));
				}
				payload = new PayloadData(Encoding.UTF8.GetBytes(cs.Password));
				await SendReplyAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
				return await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

			case "caching_sha2_password":
				var scrambleBytes = AuthenticationUtility.CreateScrambleResponse(Utility.TrimZeroByte(switchRequest.Data), cs.Password);
				payload = new PayloadData(scrambleBytes);
				await SendReplyAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
				payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

				var cachingSha2ServerResponsePayload = CachingSha2ServerResponsePayload.Create(payload);
				if (cachingSha2ServerResponsePayload.Succeeded)
					return await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

				goto case "sha256_password";

			case "sha256_password":
				if (!m_isSecureConnection && cs.Password.Length > 1)
				{
#if NET45
					Log.Error("{0} can't use {1} without secure connection on .NET 4.5", m_logArguments);
					throw new MySqlException("Authentication method '{0}' requires a secure connection (prior to .NET 4.6).".FormatInvariant(switchRequest.Name));
#else
					var publicKey = await GetRsaPublicKeyAsync(switchRequest.Name, cs, ioBehavior, cancellationToken).ConfigureAwait(false);
					return await SendEncryptedPasswordAsync(switchRequest, publicKey, cs, ioBehavior, cancellationToken).ConfigureAwait(false);
#endif
				}
				else
				{
					return await SendClearPasswordAsync(cs, ioBehavior, cancellationToken).ConfigureAwait(false);
				}

			case "mysql_old_password":
				Log.Error("{0} is requesting '{1}' which is not supported", m_logArguments);
				throw new NotSupportedException("'MySQL Server is requesting the insecure pre-4.1 auth mechanism (mysql_old_password). The user password must be upgraded; see https://dev.mysql.com/doc/refman/5.7/en/account-upgrades.html.");

			default:
				Log.Error("{0} is requesting '{1}' which is not supported", m_logArguments);
				throw new NotSupportedException("Authentication method '{0}' is not supported.".FormatInvariant(switchRequest.Name));
			}
		}

		private async Task<PayloadData> SendClearPasswordAsync(ConnectionSettings cs, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			// add NUL terminator to password
			var passwordBytes = Encoding.UTF8.GetBytes(cs.Password);
			Array.Resize(ref passwordBytes, passwordBytes.Length + 1);

			// send plaintext password
			var payload = new PayloadData(passwordBytes);
			await SendReplyAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
			return await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
		}

#if !NET45
		private async Task<PayloadData> SendEncryptedPasswordAsync(
			AuthenticationMethodSwitchRequestPayload switchRequest,
			string rsaPublicKey,
			ConnectionSettings cs,
			IOBehavior ioBehavior,
			CancellationToken cancellationToken)
		{
			// load the RSA public key
			RSA rsa;
			try
			{
				rsa = Utility.DecodeX509PublicKey(rsaPublicKey);
			}
			catch (Exception ex)
			{
				Log.Error(ex, "{0} couldn't load server's RSA public key", m_logArguments);
				throw new MySqlException("Couldn't load server's RSA public key; try using a secure connection instead.", ex);
			}

			// add NUL terminator to password
			var passwordBytes = Encoding.UTF8.GetBytes(cs.Password);
			Array.Resize(ref passwordBytes, passwordBytes.Length + 1);

			using (rsa)
			{
				// XOR the password bytes with the challenge
				AuthPluginData = Utility.TrimZeroByte(switchRequest.Data);
				for (var i = 0; i < passwordBytes.Length; i++)
					passwordBytes[i] ^= AuthPluginData[i % AuthPluginData.Length];

				// encrypt with RSA public key
				var padding = switchRequest.Name == "caching_sha2_password" ? RSAEncryptionPadding.Pkcs1 : RSAEncryptionPadding.OaepSHA1;
				var encryptedPassword = rsa.Encrypt(passwordBytes, padding);
				var payload = new PayloadData(encryptedPassword);
				await SendReplyAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
				return await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
			}
		}
#endif

		private async Task<string> GetRsaPublicKeyAsync(string switchRequestName, ConnectionSettings cs, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (!string.IsNullOrEmpty(cs.ServerRsaPublicKeyFile))
			{
				try
				{
					return File.ReadAllText(cs.ServerRsaPublicKeyFile);
				}
				catch (IOException ex)
				{
					m_logArguments[1] = cs.ServerRsaPublicKeyFile;
					Log.Error(ex, "{0} couldn't load server's RSA public key from '{1}'", m_logArguments);
					throw new MySqlException("Couldn't load server's RSA public key from '{0}'".FormatInvariant(cs.ServerRsaPublicKeyFile), ex);
				}
			}

			if (cs.AllowPublicKeyRetrieval)
			{
				// request the RSA public key
				var payloadContent = switchRequestName == "caching_sha2_password" ? (byte) 0x02 : (byte) 0x01;
				await SendReplyAsync(new PayloadData(new[] { payloadContent }), ioBehavior, cancellationToken).ConfigureAwait(false);
				var payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				var publicKeyPayload = AuthenticationMoreDataPayload.Create(payload);
				return Encoding.ASCII.GetString(publicKeyPayload.Data);
			}

			m_logArguments[1] = switchRequestName;
			Log.Error("{0} couldn't use '{1}' because RSA key wasn't specified or couldn't be retrieved", m_logArguments);
			throw new MySqlException("Authentication method '{0}' failed. Either use a secure connection, specify the server's RSA public key with ServerRSAPublicKeyFile, or set AllowPublicKeyRetrieval=True.".FormatInvariant(switchRequestName));
		}

		public async ValueTask<bool> TryPingAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			VerifyState(State.Connected);

			// send ping payload to verify client and server socket are still connected
			try
			{
				Log.Debug("{0} pinging server", m_logArguments);
				await SendAsync(PingPayload.Instance, ioBehavior, cancellationToken).ConfigureAwait(false);
				var payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				OkPayload.Create(payload);
				Log.Info("{0} successfully pinged server", m_logArguments);
				return true;
			}
			catch (IOException)
			{
				Log.Debug("{0} ping failed due to IOException", m_logArguments);
			}
			catch (SocketException)
			{
				Log.Debug("{0} ping failed due to SocketException", m_logArguments);
			}

			VerifyState(State.Failed);
			return false;
		}

		// Starts a new conversation with the server by sending the first packet.
		public ValueTask<int> SendAsync(PayloadData payload, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			m_payloadHandler.StartNewConversation();
			return SendReplyAsync(payload, ioBehavior, cancellationToken);
		}

		// Starts a new conversation with the server by receiving the first packet.
		public ValueTask<PayloadData> ReceiveAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			m_payloadHandler.StartNewConversation();
			return ReceiveReplyAsync(ioBehavior, cancellationToken);
		}

		// Continues a conversation with the server by receiving a response to a packet sent with 'Send' or 'SendReply'.
		public ValueTask<PayloadData> ReceiveReplyAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			ValueTask<ArraySegment<byte>> task;
			try
			{
				VerifyConnected();
				task = m_payloadHandler.ReadPayloadAsync(m_payloadCache, ProtocolErrorBehavior.Throw, ioBehavior);
			}
			catch (Exception ex)
			{
				m_logArguments[1] = ex.Message;
				Log.Info(ex, "{0} failed in ReceiveReplyAsync: {1}", m_logArguments);
				if ((ex as MySqlException)?.Number == (int) MySqlErrorCode.CommandTimeoutExpired)
					HandleTimeout();
				task = ValueTaskExtensions.FromException<ArraySegment<byte>>(ex);
			}

			if (task.IsCompletedSuccessfully)
			{
				var payload = new PayloadData(task.Result);
				if (payload.HeaderByte != ErrorPayload.Signature)
					return new ValueTask<PayloadData>(payload);

				var exception = CreateExceptionForErrorPayload(payload);
				return ValueTaskExtensions.FromException<PayloadData>(exception);
			}

			return new ValueTask<PayloadData>(task.AsTask().ContinueWith(TryAsyncContinuation, cancellationToken, TaskContinuationOptions.LazyCancellation | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default));
		}

		// Continues a conversation with the server by sending a reply to a packet received with 'Receive' or 'ReceiveReply'.
		public ValueTask<int> SendReplyAsync(PayloadData payload, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			ValueTask<int> task;
			try
			{
				VerifyConnected();
				task = m_payloadHandler.WritePayloadAsync(payload.ArraySegment, ioBehavior);
			}
			catch (Exception ex)
			{
				m_logArguments[1] = ex.Message;
				Log.Info(ex, "{0} failed in SendReplyAsync: {1}", m_logArguments);
				task = ValueTaskExtensions.FromException<int>(ex);
			}

			if (task.IsCompletedSuccessfully)
				return task;

			return new ValueTask<int>(task.AsTask().ContinueWith(TryAsyncContinuation, cancellationToken, TaskContinuationOptions.LazyCancellation | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default));
		}

		internal void HandleTimeout()
		{
			if (OwningConnection != null && OwningConnection.TryGetTarget(out var connection))
				connection.SetState(ConnectionState.Closed);
		}

		private void VerifyConnected()
		{
			lock (m_lock)
			{
				if (m_state == State.Closed)
					throw new ObjectDisposedException(nameof(ServerSession));
				if (m_state != State.Connected && m_state != State.Querying && m_state != State.CancelingQuery && m_state != State.ClearingPendingCancellation && m_state != State.Closing)
					throw new InvalidOperationException("ServerSession is not connected.");
			}
		}

		private async Task<bool> OpenTcpSocketAsync(ConnectionSettings cs, ILoadBalancer loadBalancer, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			var hostNames = loadBalancer.LoadBalance(cs.HostNames);
			foreach (var hostName in hostNames)
			{
				IPAddress[] ipAddresses;
				try
				{
					if (ioBehavior == IOBehavior.Asynchronous)
					{
						ipAddresses = await Dns.GetHostAddressesAsync(hostName).ConfigureAwait(false);
					}
					else
					{
#if NETSTANDARD1_3
						// Dns.GetHostAddresses isn't available until netstandard 2.0: https://github.com/dotnet/corefx/pull/11950
						ipAddresses = await Dns.GetHostAddressesAsync(hostName).ConfigureAwait(false);
#else
						ipAddresses = Dns.GetHostAddresses(hostName);
#endif
					}
				}
				catch (SocketException)
				{
					// name couldn't be resolved
					continue;
				}

				// need to try IP Addresses one at a time: https://github.com/dotnet/corefx/issues/5829
				foreach (var ipAddress in ipAddresses)
				{
					Log.Info("{0} connecting to IP address {1} for host '{2}'", m_logArguments[0], ipAddress, hostName);
					TcpClient tcpClient = null;
					try
					{
						tcpClient = new TcpClient(ipAddress.AddressFamily);

						using (cancellationToken.Register(() => tcpClient?.Client?.Dispose()))
						{
							try
							{
								if (ioBehavior == IOBehavior.Asynchronous)
								{
									await tcpClient.ConnectAsync(ipAddress, cs.Port).ConfigureAwait(false);
								}
								else
								{
#if NETSTANDARD1_3
									await tcpClient.ConnectAsync(ipAddress, cs.Port).ConfigureAwait(false);
#else
									if (Utility.IsWindows())
									{
										tcpClient.Connect(ipAddress, cs.Port);
									}
									else
									{
										// non-windows platforms block on synchronous connect, use send/receive timeouts: https://github.com/dotnet/corefx/issues/20954
										var originalSendTimeout = tcpClient.Client.SendTimeout;
										var originalReceiveTimeout = tcpClient.Client.ReceiveTimeout;
										tcpClient.Client.SendTimeout = cs.ConnectionTimeoutMilliseconds;
										tcpClient.Client.ReceiveTimeout = cs.ConnectionTimeoutMilliseconds;
										tcpClient.Connect(ipAddress, cs.Port);
										tcpClient.Client.SendTimeout = originalSendTimeout;
										tcpClient.Client.ReceiveTimeout = originalReceiveTimeout;
									}
#endif
								}
							}
							catch (ObjectDisposedException ex) when (cancellationToken.IsCancellationRequested)
							{
								Log.Info("{0} connect timeout expired connecting to IP address {1} for host '{2}'", m_logArguments[0], ipAddress, hostName);
								throw new MySqlException("Connect Timeout expired.", ex);
							}
						}
					}
					catch (SocketException)
					{
						tcpClient?.Client?.Dispose();
						continue;
					}

					HostName = hostName;
					m_tcpClient = tcpClient;
					m_socket = m_tcpClient.Client;
					m_networkStream = m_tcpClient.GetStream();
					m_socket.SetKeepAlive(cs.Keepalive);
					lock (m_lock)
						m_state = State.Connected;
					Log.Debug("{0} connected to IP address {1} for host '{2}' with local port {3}", m_logArguments[0], ipAddress, hostName, (m_socket.LocalEndPoint as IPEndPoint)?.Port);
					return true;
				}
			}
			return false;
		}

		private async Task<bool> OpenUnixSocketAsync(ConnectionSettings cs, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			m_logArguments[1] = cs.UnixSocket;
			Log.Info("{0} connecting to UNIX socket '{1}'", m_logArguments);
			var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.IP);
			var unixEp = new UnixEndPoint(cs.UnixSocket);
			try
			{
				using (cancellationToken.Register(() => socket.Dispose()))
				{
					try
					{
						if (ioBehavior == IOBehavior.Asynchronous)
						{
#if NETSTANDARD1_3
							await socket.ConnectAsync(unixEp).ConfigureAwait(false);
#else
							await Task.Factory.FromAsync(socket.BeginConnect, socket.EndConnect, unixEp, null).ConfigureAwait(false);
#endif
						}
						else
						{
							socket.Connect(unixEp);
						}
					}
					catch (ObjectDisposedException ex) when (cancellationToken.IsCancellationRequested)
					{
						Log.Info("{0} connect timeout expired connecting to UNIX socket '{1}'", m_logArguments);
						throw new MySqlException("Connect Timeout expired.", ex);
					}
				}
			}
			catch (SocketException)
			{
				socket.Dispose();
			}

			if (socket.Connected)
			{
				m_socket = socket;
				m_networkStream = new NetworkStream(socket);

				lock (m_lock)
					m_state = State.Connected;
				return true;
			}

			return false;
		}

		private async Task InitSslAsync(ProtocolCapabilities serverCapabilities, ConnectionSettings cs, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			Log.Info("{0} initializing TLS connection", m_logArguments);
			X509CertificateCollection clientCertificates = null;
			if (cs.CertificateFile != null)
			{
				try
				{
					var certificate = new X509Certificate2(cs.CertificateFile, cs.CertificatePassword);
					if (!certificate.HasPrivateKey)
					{
						m_logArguments[1] = cs.CertificateFile;
						Log.Error("{0} no private key included with certificate '{1}'", m_logArguments);
						throw new MySqlException("CertificateFile does not contain a private key. "+
						                         "CertificateFile should be in PKCS #12 (.pfx) format and contain both a Certificate and Private Key");
					}
#if !NET45
					m_clientCertificate = certificate;
#endif
					clientCertificates = new X509CertificateCollection {certificate};
				}
				catch (CryptographicException ex)
				{
					m_logArguments[1] = cs.CertificateFile;
					Log.Error(ex, "{0} couldn't load certificate from '{1}'", m_logArguments);
					if (!File.Exists(cs.CertificateFile))
						throw new MySqlException("Cannot find Certificate File", ex);
					throw new MySqlException("Either the Certificate Password is incorrect or the Certificate File is invalid", ex);
				}
			}

			X509Chain caCertificateChain = null;
			if (cs.CACertificateFile != null)
			{
				try
				{
					var caCertificate = new X509Certificate2(cs.CACertificateFile);
#if !NET45
					m_serverCertificate = caCertificate;
#endif
					caCertificateChain = new X509Chain
					{
						ChainPolicy =
						{
							RevocationMode = X509RevocationMode.NoCheck,
							VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority
						}
					};
					caCertificateChain.ChainPolicy.ExtraStore.Add(caCertificate);
				}
				catch (CryptographicException ex)
				{
					m_logArguments[1] = cs.CACertificateFile;
					Log.Error(ex, "{0} couldn't load CA certificate from '{1}'", m_logArguments);
					if (!File.Exists(cs.CACertificateFile))
						throw new MySqlException("Cannot find CA Certificate File", ex);
					throw new MySqlException("The CA Certificate File is invalid", ex);
				}
			}

			X509Certificate ValidateLocalCertificate(object lcbSender, string lcbTargetHost, X509CertificateCollection lcbLocalCertificates, X509Certificate lcbRemoteCertificate, string[] lcbAcceptableIssuers) => lcbLocalCertificates[0];

			bool ValidateRemoteCertificate(object rcbSender, X509Certificate rcbCertificate, X509Chain rcbChain, SslPolicyErrors rcbPolicyErrors)
			{
				if (cs.SslMode == MySqlSslMode.Preferred || cs.SslMode == MySqlSslMode.Required)
					return true;

				if ((rcbPolicyErrors & SslPolicyErrors.RemoteCertificateChainErrors) != 0 && caCertificateChain != null)
				{
					if (caCertificateChain.Build((X509Certificate2) rcbCertificate))
					{
						var chainStatus = caCertificateChain.ChainStatus[0].Status & ~X509ChainStatusFlags.UntrustedRoot;
						if (chainStatus == X509ChainStatusFlags.NoError)
							rcbPolicyErrors &= ~SslPolicyErrors.RemoteCertificateChainErrors;
					}
				}

				if (cs.SslMode == MySqlSslMode.VerifyCA)
					rcbPolicyErrors &= ~SslPolicyErrors.RemoteCertificateNameMismatch;

				return rcbPolicyErrors == SslPolicyErrors.None;
			}

			SslStream sslStream;
			if (clientCertificates == null)
				sslStream = new SslStream(m_networkStream, false, ValidateRemoteCertificate);
			else
				sslStream = new SslStream(m_networkStream, false, ValidateRemoteCertificate, ValidateLocalCertificate);

			// SslProtocols.Tls1.2 throws an exception in Windows, see https://github.com/mysql-net/MySqlConnector/pull/101
			var sslProtocols = SslProtocols.Tls | SslProtocols.Tls11;
			if (!Utility.IsWindows())
				sslProtocols |= SslProtocols.Tls12;

			var checkCertificateRevocation = cs.SslMode == MySqlSslMode.VerifyFull;

			var initSsl = HandshakeResponse41Payload.CreateWithSsl(serverCapabilities, cs, m_useCompression);
			await SendReplyAsync(initSsl, ioBehavior, cancellationToken).ConfigureAwait(false);

			try
			{
				if (ioBehavior == IOBehavior.Asynchronous)
				{
					await sslStream.AuthenticateAsClientAsync(HostName, clientCertificates, sslProtocols, checkCertificateRevocation).ConfigureAwait(false);
				}
				else
				{
#if NETSTANDARD1_3
					await sslStream.AuthenticateAsClientAsync(HostName, clientCertificates, sslProtocols, checkCertificateRevocation).ConfigureAwait(false);
#else
					sslStream.AuthenticateAsClient(HostName, clientCertificates, sslProtocols, checkCertificateRevocation);
#endif
				}
				var sslByteHandler = new StreamByteHandler(sslStream);
				m_payloadHandler.ByteHandler = sslByteHandler;
				m_isSecureConnection = true;
				m_sslStream = sslStream;
			}
			catch (Exception ex)
			{
				Log.Error(ex, "{0} couldn't initialize TLS connection", m_logArguments);
				sslStream.Dispose();
				ShutdownSocket();
				HostName = "";
				lock (m_lock)
					m_state = State.Failed;
				if (ex is AuthenticationException)
					throw new MySqlException("SSL Authentication Error", ex);
				if (ex is IOException && clientCertificates != null)
					throw new MySqlException("MySQL Server rejected client certificate", ex);
				throw;
			}
		}

		// Some servers are exposed through a proxy, which handles the initial handshake and gives the proxy's
		// server version and thread ID. Detect this situation and return `true` if the real server's details should
		// be requested after connecting (which takes an extra roundtrip).
		private bool ShouldGetRealServerDetails()
		{
			// currently hardcoded to the version returned by the Azure Database for MySQL proxy
			return ServerVersion.OriginalString == "5.6.26.0";
		}

		private async Task GetRealServerDetailsAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			Log.Info("{0} detected proxy; getting CONNECTION_ID(), VERSION() from server", m_logArguments);
			try
			{
				await SendAsync(QueryPayload.Create("SELECT CONNECTION_ID(), VERSION();"), ioBehavior, cancellationToken).ConfigureAwait(false);

				// column count: 2
				await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

				// CONNECTION_ID() and VERSION() columns
				await ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
				await ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);

				PayloadData payload;
				if (!SupportsDeprecateEof)
				{
					payload = await ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
					EofPayload.Create(payload);
				}

				// first (and only) row
				int? connectionId = default;
				string serverVersion = null;
				payload = await ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
				var reader = new ByteArrayReader(payload.ArraySegment);
				var length = reader.ReadLengthEncodedIntegerOrNull();
				if (length != -1)
					connectionId = int.Parse(Encoding.UTF8.GetString(reader.ReadByteArraySegment(length)), CultureInfo.InvariantCulture);
				length = reader.ReadLengthEncodedIntegerOrNull();
				if (length != -1)
					serverVersion = Encoding.UTF8.GetString(reader.ReadByteArraySegment(length));

				// OK/EOF payload
				payload = await ReceiveReplyAsync(ioBehavior, CancellationToken.None).ConfigureAwait(false);
				if (OkPayload.IsOk(payload, SupportsDeprecateEof))
					OkPayload.Create(payload, SupportsDeprecateEof);
				else
					EofPayload.Create(payload);

				if (connectionId.HasValue && serverVersion != null)
				{
					Log.Info("{0} changing ConnectionId from {1} to {2} and ServerVersion from {3} to {4}", m_logArguments[0], ConnectionId, connectionId.Value, ServerVersion.OriginalString, serverVersion);
					ConnectionId = connectionId.Value;
					ServerVersion = new ServerVersion(serverVersion);
				}
			}
			catch (MySqlException ex)
			{
				m_logArguments[1] = ex.Message;
				Log.Error(ex, "{0} failed to get CONNECTION_ID(), VERSION(): {1}", m_logArguments);
			}
		}

		private void ShutdownSocket()
		{
			Log.Info("{0} closing stream/socket", m_logArguments);
			Utility.Dispose(ref m_payloadHandler);
			Utility.Dispose(ref m_networkStream);
			SafeDispose(ref m_tcpClient);
			SafeDispose(ref m_socket);
#if !NET45
			Utility.Dispose(ref m_clientCertificate);
			Utility.Dispose(ref m_serverCertificate);
#endif
		}

		/// <summary>
		/// Disposes and sets <paramref name="disposable"/> to <c>null</c>, ignoring any
		/// <see cref="IOException"/> or <see cref="SocketException"/> that is thrown.
		/// </summary>
		/// <typeparam name="T">An <see cref="IDisposable"/> type.</typeparam>
		/// <param name="disposable">The object to dispose.</param>
		private static void SafeDispose<T>(ref T disposable)
			where T : class, IDisposable
		{
			if (disposable != null)
			{
				try
				{
					disposable.Dispose();
				}
				catch (IOException)
				{
				}
				catch (SocketException)
				{
				}
				disposable = null;
			}
		}

		private int TryAsyncContinuation(Task<int> task)
		{
			if (task.IsFaulted)
			{
				SetFailed(task.Exception.InnerException);
				task.GetAwaiter().GetResult();
			}
			return 0;
		}

		private PayloadData TryAsyncContinuation(Task<ArraySegment<byte>> task)
		{
			if (task.IsFaulted)
				SetFailed(task.Exception.InnerException);
			ArraySegment<byte> bytes;
			try
			{
				bytes = task.GetAwaiter().GetResult();
			}
			catch (MySqlException ex) when (ex.Number == (int) MySqlErrorCode.CommandTimeoutExpired)
			{
				HandleTimeout();
				throw;
			}
			var payload = new PayloadData(bytes);
			if (payload.HeaderByte == ErrorPayload.Signature)
				throw CreateExceptionForErrorPayload(payload);
			return payload;
		}

		internal void SetFailed(Exception exception)
		{
			m_logArguments[1] = exception.Message;
			Log.Info(exception, "{0} setting state to Failed: {1}", m_logArguments);
			lock (m_lock)
				m_state = State.Failed;
			if (OwningConnection != null && OwningConnection.TryGetTarget(out var connection))
				connection.SetState(ConnectionState.Closed);
		}

		private void VerifyState(State state)
		{
			if (m_state != state)
			{
				Log.Error("{0} expected state to be {1} but was {2}", m_logArguments[0], state, m_state);
				throw new InvalidOperationException("Expected state to be {0} but was {1}.".FormatInvariant(state, m_state));
			}
		}

		private void VerifyState(State state1, State state2)
		{
			if (m_state != state1 && m_state != state2)
			{
				Log.Error("{0} expected state to be {1}|{2} but was {3}", m_logArguments[0], state1, state2, m_state);
				throw new InvalidOperationException("Expected state to be ({0}|{1}) but was {2}.".FormatInvariant(state1, state2, m_state));
			}
		}

		private void VerifyState(State state1, State state2, State state3)
		{
			if (m_state != state1 && m_state != state2 && m_state != state3)
			{
				Log.Error("{0} expected state to be {1}|{2}|{3} but was {4}", m_logArguments[0], state1, state2, state3, m_state);
				throw new InvalidOperationException("Expected state to be ({0}|{1}|{2}) but was {3}.".FormatInvariant(state1, state2, state3, m_state));
			}
		}

		internal bool SslIsEncrypted => m_sslStream?.IsEncrypted ?? false;

		internal bool SslIsSigned => m_sslStream?.IsSigned ?? false;

		internal bool SslIsAuthenticated => m_sslStream?.IsAuthenticated ?? false;

		internal bool SslIsMutuallyAuthenticated => m_sslStream?.IsMutuallyAuthenticated ?? false;

		private byte[] CreateConnectionAttributes()
		{
			Log.Debug("{0} creating connection attributes", m_logArguments);
			var attributesWriter = new PayloadWriter();
			attributesWriter.WriteLengthEncodedString("_client_name");
			attributesWriter.WriteLengthEncodedString("MySqlConnector");
			attributesWriter.WriteLengthEncodedString("_client_version");
			attributesWriter.WriteLengthEncodedString(typeof(ServerSession).GetTypeInfo().Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion);
			try
			{
				Utility.GetOSDetails(out var os, out var osDescription, out var architecture);
				if (os != null)
				{
					attributesWriter.WriteLengthEncodedString("_os");
					attributesWriter.WriteLengthEncodedString(os);
				}
				attributesWriter.WriteLengthEncodedString("_os_details");
				attributesWriter.WriteLengthEncodedString(osDescription);
				attributesWriter.WriteLengthEncodedString("_platform");
				attributesWriter.WriteLengthEncodedString(architecture);
			}
			catch (PlatformNotSupportedException)
			{
			}
			using (var process = Process.GetCurrentProcess())
			{
				attributesWriter.WriteLengthEncodedString("_pid");
				attributesWriter.WriteLengthEncodedString(process.Id.ToString(CultureInfo.InvariantCulture));
			}
			var connectionAttributes = attributesWriter.ToBytes();

			var writer = new PayloadWriter();
			writer.WriteLengthEncodedInteger((ulong) connectionAttributes.Length);
			writer.Write(connectionAttributes);
			return writer.ToBytes();
		}

		private Exception CreateExceptionForErrorPayload(PayloadData payload)
		{
			var errorPayload = ErrorPayload.Create(payload);
			var exception = errorPayload.ToException();
			Log.Error(exception, "{0} got error payload: Code={1}, State={2}, Message={3}", m_logArguments[0], errorPayload.ErrorCode, errorPayload.State, errorPayload.Message);
			return exception;
		}

		private enum State
		{
			// The session has been created; no connection has been made.
			Created,

			// The session is attempting to connect to a server.
			Connecting,

			// The session is connected to a server; there is no active query.
			Connected,

			// The session is connected to a server and a query is being made.
			Querying,

			// The session is connected to a server and the active query is being cancelled.
			CancelingQuery,

			// A cancellation is pending on the server and needs to be cleared.
			ClearingPendingCancellation,

			// The session is closing.
			Closing,

			// The session is closed.
			Closed,

			// An unexpected error occurred; the session is in an unusable state.
			Failed,
		}

		static int s_lastId;
		static byte[] s_connectionAttributes;
		static readonly IMySqlConnectorLogger Log = MySqlConnectorLogManager.CreateLogger(nameof(ServerSession));
		static readonly PayloadData s_setNamesUtf8mb4Payload = QueryPayload.Create("SET NAMES utf8mb4 COLLATE utf8mb4_bin;");

		readonly object m_lock;
		readonly object[] m_logArguments;
		readonly ArraySegmentHolder<byte> m_payloadCache;
		State m_state;
		TcpClient m_tcpClient;
		Socket m_socket;
		NetworkStream m_networkStream;
		SslStream m_sslStream;
#if !NET45
		IDisposable m_clientCertificate;
		IDisposable m_serverCertificate;
#endif
		IPayloadHandler m_payloadHandler;
		int m_activeCommandId;
		bool m_useCompression;
		bool m_isSecureConnection;
		bool m_supportsConnectionAttributes;
		bool m_supportsDeprecateEof;
		bool m_supportsOptionalResultSetMetadata;
	}
}
