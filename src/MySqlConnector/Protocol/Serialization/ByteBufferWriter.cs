using System;
using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Text;
using MySqlConnector.Utilities;

namespace MySqlConnector.Protocol.Serialization
{
	internal sealed class ByteBufferWriter : IBufferWriter<byte>
	{
		public ByteBufferWriter(int capacity = 0)
		{
			m_buffer = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 128));
			m_output = m_buffer;
		}

		public ArraySegment<byte> ArraySegment => new ArraySegment<byte>(m_buffer, 0, m_buffer.Length - m_output.Length);

		public PayloadData ToPayloadData() => new PayloadData(ArraySegment, isPooled: true);

		public Memory<byte> GetMemory(int sizeHint = 0)
		{
			Debug.Assert(sizeHint >= 0, "sizeHint >= 0");
			if (sizeHint > m_output.Length)
				Reallocate(sizeHint);
			return m_output;
		}

		public Span<byte> GetSpan(int sizeHint = 0)
		{
			Debug.Assert(sizeHint >= 0, "sizeHint >= 0");
			if (sizeHint > m_output.Length)
				Reallocate(sizeHint);
			return m_output.Span;
		}

		public void Advance(int count)
		{
			Debug.Assert(count <= m_output.Length, "length <= m_output.Length");
			m_output = m_output.Slice(count);
		}

		public void Write(byte value)
		{
			if (m_output.Length < 1)
				Reallocate();
			m_output.Span[0] = value;
			m_output = m_output.Slice(1);
		}

		public void Write(ushort value)
		{
			if (m_output.Length < 2)
				Reallocate(2);
			var span = m_output.Span;
			span[0] = (byte) value;
			span[1] = (byte) (value >> 8);
			m_output = m_output.Slice(2);
		}

		public void Write(int value)
		{
			if (m_output.Length < 4)
				Reallocate(4);
			var span = m_output.Span;
			span[0] = (byte) value;
			span[1] = (byte) (value >> 8);
			span[2] = (byte) (value >> 16);
			span[3] = (byte) (value >> 24);
			m_output = m_output.Slice(4);
		}

		public void Write(uint value)
		{
			if (m_output.Length < 4)
				Reallocate(4);
			var span = m_output.Span;
			span[0] = (byte) value;
			span[1] = (byte) (value >> 8);
			span[2] = (byte) (value >> 16);
			span[3] = (byte) (value >> 24);
			m_output = m_output.Slice(4);
		}

		public void Write(ulong value)
		{
			if (m_output.Length < 8)
				Reallocate(8);
			var span = m_output.Span;
			span[0] = (byte) value;
			span[1] = (byte) (value >> 8);
			span[2] = (byte) (value >> 16);
			span[3] = (byte) (value >> 24);
			span[4] = (byte) (value >> 32);
			span[5] = (byte) (value >> 40);
			span[6] = (byte) (value >> 48);
			span[7] = (byte) (value >> 56);
			m_output = m_output.Slice(8);
		}

		public void Write(ArraySegment<byte> arraySegment) => Write(arraySegment.AsSpan());

		public void Write(ReadOnlySpan<byte> span)
		{
			if (m_output.Length < span.Length)
				Reallocate(span.Length);
			span.CopyTo(m_output.Span);
			m_output = m_output.Slice(span.Length);
		}

#if NET45 || NETSTANDARD1_3
		public void Write(string value)
		{
			Debug.Assert(value != null, "value != null");
			if (value.Length == 0)
				return;

			var byteCount = Encoding.UTF8.GetByteCount(value);
			if (byteCount > m_output.Length)
				Reallocate(byteCount);
			Encoding.UTF8.GetBytes(value.AsSpan(), m_output.Span);
			m_output = m_output.Slice(byteCount);
		}

		public unsafe void Write(string value, int offset, int length)
		{
			if (length == 0)
				return;

			Debug.Assert(value != null, "value != null");
			fixed (char* charsPtr = value)
			{
				var byteCount = Encoding.UTF8.GetByteCount(charsPtr + offset, length);
				if (byteCount > m_output.Length)
					Reallocate(byteCount);
				Encoding.UTF8.GetBytes(value.AsSpan(offset, length), m_output.Span);
				m_output = m_output.Slice(byteCount);
			}
		}
#else
		public void Write(string value) => Write(value.AsSpan());
		public void Write(string value, int offset, int length) => Write(value.AsSpan(offset, length));

		public void Write(ReadOnlySpan<char> chars)
		{
			if (m_encoder == null)
				m_encoder = Encoding.UTF8.GetEncoder();
			while (chars.Length > 0)
			{
				if (m_output.Length < 4)
					Reallocate();
				m_encoder.Convert(chars, m_output.Span, true, out var charsUsed, out var bytesUsed, out var completed);
				chars = chars.Slice(charsUsed);
				m_output = m_output.Slice(bytesUsed);
				if (!completed)
					Reallocate();
				Debug.Assert(completed == (chars.Length == 0));
			}
		}
#endif

		public void WriteString(short value)
		{
			int bytesWritten;
			while (!Utf8Formatter.TryFormat(value, m_output.Span, out bytesWritten))
				Reallocate();
			m_output = m_output.Slice(bytesWritten);
		}

		public void WriteString(ushort value)
		{
			int bytesWritten;
			while (!Utf8Formatter.TryFormat(value, m_output.Span, out bytesWritten))
				Reallocate();
			m_output = m_output.Slice(bytesWritten);
		}

		public void WriteString(int value)
		{
			int bytesWritten;
			while (!Utf8Formatter.TryFormat(value, m_output.Span, out bytesWritten))
				Reallocate();
			m_output = m_output.Slice(bytesWritten);
		}

		public void WriteString(uint value)
		{
			int bytesWritten;
			while (!Utf8Formatter.TryFormat(value, m_output.Span, out bytesWritten))
				Reallocate();
			m_output = m_output.Slice(bytesWritten);
		}

		public void WriteString(long value)
		{
			int bytesWritten;
			while (!Utf8Formatter.TryFormat(value, m_output.Span, out bytesWritten))
				Reallocate();
			m_output = m_output.Slice(bytesWritten);
		}

		public void WriteString(ulong value)
		{
			int bytesWritten;
			while (!Utf8Formatter.TryFormat(value, m_output.Span, out bytesWritten))
				Reallocate();
			m_output = m_output.Slice(bytesWritten);
		}

		private void Reallocate(int additional = 0)
		{
			var usedLength = m_buffer.Length - m_output.Length;
			var newBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(usedLength + additional, m_buffer.Length * 2));
			Buffer.BlockCopy(m_buffer, 0, newBuffer, 0, usedLength);
			ArrayPool<byte>.Shared.Return(m_buffer);
			m_buffer = newBuffer;
			m_output = new Memory<byte>(m_buffer, usedLength, m_buffer.Length - usedLength);
		}

#if !NET45 && !NETSTANDARD1_3
		Encoder m_encoder;
#endif
		byte[] m_buffer;
		Memory<byte> m_output;
	}

	internal static class Utf8WriterExtensions
	{
		public static void WriteLengthEncodedInteger(this ByteBufferWriter writer, ulong value)
		{
			if (value < 251)
			{
				writer.Write((byte) value);
			}
			else if (value < 65536)
			{
				writer.Write((byte) 0xfc);
				writer.Write((ushort) value);
			}
			else if (value < 16777216)
			{
				writer.Write((byte) 0xfd);
				writer.Write((byte) (value & 0xFF));
				writer.Write((byte) ((value >> 8) & 0xFF));
				writer.Write((byte) ((value >> 16) & 0xFF));
			}
			else
			{
				writer.Write((byte) 0xfe);
				writer.Write(value);
			}
		}

		public static void WriteLengthEncodedString(this ByteBufferWriter writer, string value)
		{
			var byteCount = Encoding.UTF8.GetByteCount(value);
			writer.WriteLengthEncodedInteger((ulong) byteCount);
			writer.Write(value);
		}

		public static void WriteNullTerminatedString(this ByteBufferWriter writer, string value)
		{
			writer.Write(value);
			writer.Write((byte) 0);
		}
	}
}
