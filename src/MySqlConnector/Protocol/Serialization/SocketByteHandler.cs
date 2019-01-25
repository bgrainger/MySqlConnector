using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Utilities;

namespace MySqlConnector.Protocol.Serialization
{
	internal sealed class SocketByteHandler : IByteHandler
	{
		public SocketByteHandler(Socket socket)
		{
			m_socket = socket;
			var socketEventArgs = new SocketAsyncEventArgs();
			m_socketAwaitable = new SocketAwaitable(socketEventArgs);
			m_closeSocket = socket.Dispose;
			RemainingTimeout = Constants.InfiniteTimeout;
		}

		public void Dispose() => m_socketAwaitable.EventArgs.Dispose();

		public int RemainingTimeout { get; set; }

		public ValueTask<int> ReadBytesAsync(ArraySegment<byte> buffer, IOBehavior ioBehavior)
		{
			return ioBehavior == IOBehavior.Asynchronous ?
				new ValueTask<int>(DoReadBytesAsync(buffer)) : DoReadBytesSync(buffer);
		}

		private ValueTask<int> DoReadBytesSync(ArraySegment<byte> buffer)
		{
			try
			{
				if (RemainingTimeout == Constants.InfiniteTimeout)
					return new ValueTask<int>(m_socket.Receive(buffer.Array, buffer.Offset, buffer.Count, SocketFlags.None));

				while (RemainingTimeout > 0)
				{
					var startTime = Environment.TickCount;
					if (m_socket.Poll(Math.Min(int.MaxValue / 1000, RemainingTimeout) * 1000, SelectMode.SelectRead))
					{
						var bytesRead = m_socket.Receive(buffer.Array, buffer.Offset, buffer.Count, SocketFlags.None);
						RemainingTimeout -= unchecked(Environment.TickCount - startTime);
						return new ValueTask<int>(bytesRead);
					}
					RemainingTimeout -= unchecked(Environment.TickCount - startTime);
				}
				return ValueTaskExtensions.FromException<int>(MySqlException.CreateForTimeout());
			}
			catch (Exception ex)
			{
				return ValueTaskExtensions.FromException<int>(ex);
			}
		}

		private async Task<int> DoReadBytesAsync(ArraySegment<byte> buffer)
		{
			var startTime = RemainingTimeout == Constants.InfiniteTimeout ? 0 : Environment.TickCount;
			var timerId = RemainingTimeout == Constants.InfiniteTimeout ? 0 :
				RemainingTimeout <= 0 ? throw MySqlException.CreateForTimeout() :
				TimerQueue.Instance.Add(RemainingTimeout, m_closeSocket);
			m_socketAwaitable.EventArgs.SetBuffer(buffer.Array, buffer.Offset, buffer.Count);
			int bytesRead;
			try
			{
				await m_socket.ReceiveAsync(m_socketAwaitable);
				bytesRead =  m_socketAwaitable.EventArgs.BytesTransferred;
			}
			catch (SocketException ex)
			{
				if (RemainingTimeout != Constants.InfiniteTimeout)
				{
					RemainingTimeout -= unchecked(Environment.TickCount - startTime);
					if (!TimerQueue.Instance.Remove(timerId))
						throw MySqlException.CreateForTimeout(ex);
				}
				throw;
			}
			if (RemainingTimeout != Constants.InfiniteTimeout)
			{
				RemainingTimeout -= unchecked(Environment.TickCount - startTime);
				if (!TimerQueue.Instance.Remove(timerId))
					throw MySqlException.CreateForTimeout();
			}
			return bytesRead;
		}

		public ValueTask<int> WriteBytesAsync(ArraySegment<byte> data, IOBehavior ioBehavior)
		{
			if (ioBehavior == IOBehavior.Asynchronous)
				return new ValueTask<int>(DoWriteBytesAsync(data));

			try
			{
				m_socket.Send(data.Array, data.Offset, data.Count, SocketFlags.None);
				return default;
			}
			catch (Exception ex)
			{
				return ValueTaskExtensions.FromException<int>(ex);
			}
		}

		private async Task<int> DoWriteBytesAsync(ArraySegment<byte> data)
		{
			m_socketAwaitable.EventArgs.SetBuffer(data.Array, data.Offset, data.Count);
			await m_socket.SendAsync(m_socketAwaitable);
			return 0;
		}

		readonly Socket m_socket;
		readonly SocketAwaitable m_socketAwaitable;
		readonly Action m_closeSocket;
	}
}
