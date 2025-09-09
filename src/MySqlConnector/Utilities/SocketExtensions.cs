using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace MySqlConnector.Utilities;

internal static class SocketExtensions
{
	extension(Socket socket)
	{
#if NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER
		public int Send(ReadOnlyMemory<byte> data, SocketFlags flags) => socket.Send(data.Span, flags);
#else
		public SocketAwaitable ReceiveAsync(SocketAwaitable awaitable)
		{
			awaitable.Reset();
			if (!socket.ReceiveAsync(awaitable.EventArgs))
				awaitable.WasCompleted = true;
			return awaitable;
		}

		public SocketAwaitable SendAsync(SocketAwaitable awaitable)
		{
			awaitable.Reset();
			if (!socket.SendAsync(awaitable.EventArgs))
				awaitable.WasCompleted = true;
			return awaitable;
		}

		public int Send(ReadOnlyMemory<byte> data, SocketFlags flags)
		{
			MemoryMarshal.TryGetArray(data, out var arraySegment);
			return socket.Send(arraySegment.Array, arraySegment.Offset, arraySegment.Count, flags);
		}
#endif

		public void SetKeepAlive(uint keepAliveTimeSeconds)
		{
			// Always use the OS Default Keepalive settings
			socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
			if (keepAliveTimeSeconds == 0)
				return;

			// If keepAliveTimeSeconds > 0, override keepalive options on the socket
#if NETCOREAPP3_0_OR_GREATER
			// cross-platform TCP KeepAlive settings were added in netcoreapp3.0: https://github.com/dotnet/runtime/issues/24041
			socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
			socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, (int) keepAliveTimeSeconds);
#else
			if (Utility.IsWindows())
			{
				// http://stackoverflow.com/a/11834055/1419658
				// Windows takes time in milliseconds
				var keepAliveTimeMillis = keepAliveTimeSeconds > uint.MaxValue / 1000 ? uint.MaxValue : keepAliveTimeSeconds * 1000;
				var inOptionValues = new byte[12];
				inOptionValues[0] = 1;
				inOptionValues[4] = (byte) (keepAliveTimeMillis & 0xFF);
				inOptionValues[5] = (byte) ((keepAliveTimeMillis >> 8) & 0xFF);
				inOptionValues[6] = (byte) ((keepAliveTimeMillis >> 16) & 0xFF);
				inOptionValues[7] = (byte) ((keepAliveTimeMillis >> 24) & 0xFF);
				inOptionValues[8] = 0xE8;
				inOptionValues[9] = 0x03;
				socket.IOControl(IOControlCode.KeepAliveValues, inOptionValues, null);
			}

			// Unix not supported: The appropriate socket options to set Keepalive options are not exposed: https://github.com/dotnet/runtime/issues/19568
#endif
		}
	}

#if !NETCOREAPP2_1_OR_GREATER && !NETSTANDARD2_1_OR_GREATER
	extension(SocketAsyncEventArgs args)
	{
		public void SetBuffer(Memory<byte> buffer)
		{
			MemoryMarshal.TryGetArray<byte>(buffer, out var arraySegment);
			args.SetBuffer(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
		}
	}
#endif
}
