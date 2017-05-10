using System;
using System.Net.Sockets;

namespace MySql.Data.Protocol.Serialization
{
	internal sealed class SocketByteHandler : IByteHandler
	{
		public SocketByteHandler(Socket socket)
		{
			m_socket = socket;
			m_socketAsyncEventArgs = new SocketAsyncEventArgs();
			m_socketAsyncEventArgs.Completed += s_eventHandler;
		}

		public ValueOrCallback<int> ReadBytesAsync(ArraySegment<byte> buffer, IOBehavior ioBehavior)
		{
			return ioBehavior == IOBehavior.Asynchronous ? DoReadBytesAsync(buffer) :
				new ValueOrCallback<int>(m_socket.Receive(buffer.Array, buffer.Offset, buffer.Count, SocketFlags.None));
		}

		ValueOrCallback<int> DoReadBytesAsync(ArraySegment<byte> buffer_)
		{
			var source = new ValueOrCallbackSource<int>();
			m_socketAsyncEventArgs.UserToken = source;
			m_socketAsyncEventArgs.SetBuffer(buffer_.Array, buffer_.Offset, buffer_.Count);
			if (!m_socket.ReceiveAsync(m_socketAsyncEventArgs))
				TransferStatus(m_socketAsyncEventArgs, source);
			return source.ValueOrCallback;
		}

		public ValueOrCallback<int> WriteBytesAsync(ArraySegment<byte> data, IOBehavior ioBehavior)
		{
			if (ioBehavior == IOBehavior.Asynchronous)
				return DoWriteBytesAsync(data);

			m_socket.Send(data.Array, data.Offset, data.Count, SocketFlags.None);
			return ValueOrCallback<int>.Empty;
		}

		ValueOrCallback<int> DoWriteBytesAsync(ArraySegment<byte> data_)
		{
			var source = new ValueOrCallbackSource<int>();
			m_socketAsyncEventArgs.UserToken = source;
			m_socketAsyncEventArgs.SetBuffer(data_.Array, data_.Offset, data_.Count);
			if (!m_socket.SendAsync(m_socketAsyncEventArgs))
				TransferStatus(m_socketAsyncEventArgs, source);
			return source.ValueOrCallback;
		}

		private static readonly EventHandler<SocketAsyncEventArgs> s_eventHandler = (sender, args) =>
			TransferStatus(args, (ValueOrCallbackSource<int>) args.UserToken);

		private static void TransferStatus(SocketAsyncEventArgs args, ValueOrCallbackSource<int> source)
		{
			if (args.SocketError != SocketError.Success)
				source.SetException(new SocketException((int) args.SocketError));
			else
				source.SetResult(args.BytesTransferred);
		}

		readonly Socket m_socket;
		readonly SocketAsyncEventArgs m_socketAsyncEventArgs;
	}
}
