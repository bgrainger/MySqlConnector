using System;
using System.IO;
using System.Threading.Tasks;

namespace MySql.Data.Protocol.Serialization
{
	internal sealed class StreamByteHandler : IByteHandler
	{
		public StreamByteHandler(Stream stream)
		{
			m_stream = stream;
		}

#if OLD
		public ValueTask<int> ReadBytesAsync(ArraySegment<byte> buffer, IOBehavior ioBehavior)
		{
			return (ioBehavior == IOBehavior.Asynchronous) ?
				new ValueTask<int>(DoReadBytesAsync(buffer)) :
				new ValueTask<int>(m_stream.Read(buffer.Array, buffer.Offset, buffer.Count));

			async Task<int> DoReadBytesAsync(ArraySegment<byte> buffer_) =>
				await m_stream.ReadAsync(buffer_.Array, buffer_.Offset, buffer_.Count).ConfigureAwait(false);
		}

		public ValueTask<int> WriteBytesAsync(ArraySegment<byte> data, IOBehavior ioBehavior)
		{
			if (ioBehavior == IOBehavior.Asynchronous)
			{
				return new ValueTask<int>(DoWriteBytesAsync(data));
			}
			else
			{
				m_stream.Write(data.Array, data.Offset, data.Count);
				return default(ValueTask<int>);
			}

			async Task<int> DoWriteBytesAsync(ArraySegment<byte> data_)
			{
				await m_stream.WriteAsync(data_.Array, data_.Offset, data_.Count).ConfigureAwait(false);
				return 0;
			}
		}
#endif

		readonly Stream m_stream;
		public ValueOrCallback<int> ReadBytesAsync(ArraySegment<byte> buffer, IOBehavior ioBehavior)
		{
			throw new NotImplementedException();
		}

		public ValueOrCallback<int> WriteBytesAsync(ArraySegment<byte> data, IOBehavior ioBehavior)
		{
			throw new NotImplementedException();
		}
	}
}
