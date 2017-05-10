using System;

namespace MySql.Data.Protocol.Serialization
{
	internal sealed class BufferedByteReader
	{
		public BufferedByteReader()
		{
			m_buffer = new byte[16384];
		}

		public ValueOrCallback<ArraySegment<byte>> ReadBytesAsync(IByteHandler byteHandler, int count, IOBehavior ioBehavior)
		{
			// check if read can be satisfied from the buffer
			if (m_remainingData.Count >= count)
			{
				var readBytes = m_remainingData.Slice(0, count);
				m_remainingData = m_remainingData.Slice(count);
				return new ValueOrCallback<ArraySegment<byte>>(readBytes);
			}

			// get a buffer big enough to hold all the data, and move any buffered data to the beginning
			var buffer = count > m_buffer.Length ? new byte[count] : m_buffer;
			if (m_remainingData.Count > 0)
			{
				Buffer.BlockCopy(m_remainingData.Array, m_remainingData.Offset, buffer, 0, m_remainingData.Count);
				m_remainingData = new ArraySegment<byte>(buffer, 0, m_remainingData.Count);
			}

			return ReadBytesAsync(byteHandler, new ArraySegment<byte>(buffer, m_remainingData.Count, buffer.Length - m_remainingData.Count), count, ioBehavior);
		}

		private ValueOrCallback<ArraySegment<byte>> ReadBytesAsync(IByteHandler byteHandler, ArraySegment<byte> buffer, int totalBytesToRead, IOBehavior ioBehavior)
		{
			// keep reading data synchronously while it is available
			var readBytesTask = byteHandler.ReadBytesAsync(buffer, ioBehavior);
			while (readBytesTask.IsCompleted)
			{
				ValueOrCallback<ArraySegment<byte>> result;
				if (HasReadAllData(readBytesTask.Result, ref buffer, totalBytesToRead, out result))
					return result;

				readBytesTask = byteHandler.ReadBytesAsync(buffer, ioBehavior);
			}

			// call .Then (as a separate method, so that the temporary class for the lambda is only allocated if necessary)
			return AddContinuation(readBytesTask, byteHandler, buffer, totalBytesToRead, ioBehavior);

			ValueOrCallback<ArraySegment<byte>> AddContinuation(ValueOrCallback<int> readBytesTask_, IByteHandler byteHandler_, ArraySegment<byte> buffer_, int totalBytesToRead_, IOBehavior ioBehavior_) =>
				readBytesTask_.Then(x => HasReadAllData(x, ref buffer_, totalBytesToRead_, out var result_) ? result_ : ReadBytesAsync(byteHandler_, buffer_, totalBytesToRead_, ioBehavior_));
		}

		/// <summary>
		/// Returns <c>true</c> if all the required data has been read, then sets <paramref name="result"/> to a <see cref="ValueTask{ArraySegment{byte}}"/> representing that data.
		/// Otherwise, returns <c>false</c> and updates <paramref name="buffer"/> to where more data should be placed when it's read.
		/// </summary>
		/// <param name="readBytesCount">The number of bytes that have just been read into <paramref name="buffer"/>.</param>
		/// <param name="buffer">The <see cref="ArraySegment{byte}"/> that contains all the data read so far, and that will receive more data read in the future. It is assumed that data is stored
		/// at the beginning of the array owned by <paramref name="buffer"/> and that <code>Offset</code> indicates where to place future data.</param>
		/// <param name="totalBytesToRead">The total number of bytes that need to be read.</param>
		/// <param name="result">On success, a <see cref="ValueTask{ArraySegment{byte}}"/> representing all the data that was read.</param>
		/// <returns><c>true</c> if all data has been read; otherwise, <c>false</c>.</returns>
		private bool HasReadAllData(int readBytesCount, ref ArraySegment<byte> buffer, int totalBytesToRead, out ValueOrCallback<ArraySegment<byte>> result)
		{
			if (readBytesCount == 0)
			{
				var data = m_remainingData;
				m_remainingData = default(ArraySegment<byte>);
				result = new ValueOrCallback<ArraySegment<byte>>(data);
				return true;
			}

			var bufferSize = buffer.Offset + readBytesCount;
			if (bufferSize >= totalBytesToRead)
			{
				var bufferBytes = new ArraySegment<byte>(buffer.Array, 0, bufferSize);
				var requestedBytes = bufferBytes.Slice(0, totalBytesToRead);
				m_remainingData = bufferBytes.Slice(totalBytesToRead);
				result = new ValueOrCallback<ArraySegment<byte>>(requestedBytes);
				return true;
			}

			buffer = new ArraySegment<byte>(buffer.Array, bufferSize, buffer.Array.Length - bufferSize);
			result = ValueOrCallback<ArraySegment<byte>>.Empty;
			return false;
		}

		ArraySegment<byte> m_remainingData;
		readonly byte[] m_buffer;
	}
}
