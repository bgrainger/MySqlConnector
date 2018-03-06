using System;
using System.Buffers;
using System.Buffers.Text;
using System.Text;

namespace MySqlConnector.Core
{
	internal sealed class StatementWriter : IDisposable
	{
		public StatementWriter()
		{
			m_array = ArrayPool<byte>.Shared.Rent(8192);
		}

		public void Dispose()
		{
			if (m_array != null)
				ArrayPool<byte>.Shared.Return(m_array);
			m_array = null;
		}

		public ArraySegment<byte> RawBuffer => new ArraySegment<byte>(m_array, 0, m_length);

		public Span<byte> GetWriteable(int desiredSize)
		{
			EnsureCapacity(desiredSize);
			return new Span<byte>(m_array, m_length, desiredSize);
		}

		public void FinishWriting(int size)
		{
			m_length += size;
		}

		public void Write(int value)
		{
			Utf8Formatter.TryFormat(value, GetWriteable(11), out var bytesWritten);
			m_length += bytesWritten;
		}

		public void WriteUtf8(string value) => WriteUtf8(value, 0, value.Length);

		public void WriteUtf8(string value, int offset, int length)
		{
			unsafe
			{
				int byteLength;
				fixed (char* chars = value)
				{
					byteLength = Encoding.UTF8.GetByteCount(chars + offset, length);
					EnsureCapacity(byteLength);
					fixed (byte* bytes = m_array)
					{
						m_length += Encoding.UTF8.GetBytes(chars + offset, length, bytes + m_length, m_array.Length - m_length);
					}
				}
			}
		}

		public void Write(byte value)
		{
			EnsureCapacity(1);
			m_array[m_length++] = value;
		}

		public void Write(byte[] value)
		{
			EnsureCapacity(value.Length);
			Buffer.BlockCopy(value, 0, m_array, m_length, value.Length);
			m_length += value.Length;
		}

		private void EnsureCapacity(int additional)
		{
			if (m_length + additional > m_array.Length)
			{
				var newArray = ArrayPool<byte>.Shared.Rent(Math.Max(m_array.Length * 2, m_array.Length + additional));
				Buffer.BlockCopy(m_array, 0, newArray, 0, m_length);
				ArrayPool<byte>.Shared.Return(m_array);
				m_array = newArray;
			}
		}

		byte[] m_array;
		int m_length;
	}
}
