using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Diagnostics;
using System.Text;
using MySqlConnector.Utilities;

namespace MySqlConnector.Protocol.Serialization;

internal sealed class ByteBufferWriter : IBufferWriter<byte>
{
	public ByteBufferWriter(int capacity = 0)
	{
		m_buffer = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 128));
		m_output = m_buffer;
	}

	public int Position => m_buffer.Length - m_output.Length;

	public ArraySegment<byte> ArraySegment => new(m_buffer, 0, Position);

	public PayloadData ToPayloadData() => new(ArraySegment, isPooled: true);

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
		Debug.Assert(count <= m_output.Length, "count <= m_output.Length");
		m_output = m_output[count..];
	}

	public void TrimEnd(int byteCount)
	{
		Debug.Assert(byteCount <= m_output.Length, "byteCount <= m_output.Length");
		m_output = m_buffer.AsMemory(Position - byteCount);
	}

	public void Write(byte value)
	{
		if (m_output.Length < 1)
			Reallocate();
		m_output.Span[0] = value;
		m_output = m_output[1..];
	}

	public void Write(ushort value)
	{
		if (m_output.Length < 2)
			Reallocate(2);
		BinaryPrimitives.WriteUInt16LittleEndian(m_output.Span, value);
		m_output = m_output[2..];
	}

	public void Write(int value)
	{
		if (m_output.Length < 4)
			Reallocate(4);
		BinaryPrimitives.WriteInt32LittleEndian(m_output.Span, value);
		m_output = m_output[4..];
	}

	public void Write(uint value)
	{
		if (m_output.Length < 4)
			Reallocate(4);
		BinaryPrimitives.WriteUInt32LittleEndian(m_output.Span, value);
		m_output = m_output[4..];
	}

	public void Write(ulong value)
	{
		if (m_output.Length < 8)
			Reallocate(8);
		BinaryPrimitives.WriteUInt64LittleEndian(m_output.Span, value);
		m_output = m_output[8..];
	}

	public void Write(ArraySegment<byte> arraySegment) => Write(arraySegment.AsSpan());

	public void Write(ReadOnlySpan<byte> span)
	{
		if (m_output.Length < span.Length)
			Reallocate(span.Length);
		span.CopyTo(m_output.Span);
		m_output = m_output[span.Length..];
	}

	public void Write(string value) => Write(value.AsSpan());

	public void WriteAscii(string value) => WriteAscii(value.AsSpan());

	public void Write(string value, int offset, int length) => Write(value.AsSpan(offset, length));

	public void Write(ReadOnlySpan<char> chars)
	{
		if (m_output.Length < chars.Length * 3)
		{
			var neededBytes = Encoding.UTF8.GetByteCount(chars);
			if (m_output.Length < neededBytes)
				Reallocate(neededBytes);
		}
		m_output = m_output[Encoding.UTF8.GetBytes(chars, m_output.Span)..];
	}

	public void Write(ReadOnlySpan<char> chars, bool flush)
	{
		m_encoder ??= Encoding.UTF8.GetEncoder();
		while (chars.Length > 0)
		{
			if (m_output.Length < 4)
				Reallocate();
			m_encoder.Convert(chars, m_output.Span, flush: false, out var charsUsed, out var bytesUsed, out var completed);
			chars = chars[charsUsed..];
			m_output = m_output[bytesUsed..];
			if (!completed)
				Reallocate();
			Debug.Assert(completed == (chars.Length == 0), "completed == (chars.Length == 0)");
		}

		if (flush && m_encoder is not null)
		{
			if (m_output.Length < 4)
				Reallocate();
			m_encoder.Convert("".AsSpan(), m_output.Span, flush: true, out _, out var bytesUsed, out _);
			m_output = m_output[bytesUsed..];
		}
	}

	public void WriteAscii(ReadOnlySpan<char> chars)
	{
		if (m_output.Length < chars.Length)
			Reallocate(chars.Length);
#if NET8_0_OR_GREATER
		Ascii.FromUtf16(chars, m_output.Span, out var bytesWritten);
		m_output = m_output[bytesWritten..];
#else
		m_output = m_output[Encoding.ASCII.GetBytes(chars, m_output.Span)..];
#endif
	}

	public void WriteLengthEncodedString(StringBuilder stringBuilder)
	{
#if NETCOREAPP3_0_OR_GREATER
		// save where the length will be written
		var lengthPosition = Position;
		if (m_output.Length < 9)
			Reallocate(9);
		Advance(9);

		// write all the text as UTF-8
		m_encoder ??= Encoding.UTF8.GetEncoder();
		foreach (var chunk in stringBuilder.GetChunks())
		{
			var currentSpan = chunk.Span;
			while (currentSpan.Length > 0)
			{
				if (m_output.Length < 4)
					Reallocate();
				m_encoder.Convert(currentSpan, m_output.Span, false, out var charsUsed, out var bytesUsed, out var completed);
				currentSpan = currentSpan[charsUsed..];
				m_output = m_output[bytesUsed..];
				if (!completed)
					Reallocate();
				Debug.Assert(completed == (currentSpan.Length == 0), "completed == (currentSpan.Length == 0)");
			}
		}

		// flush the output
		if (m_output.Length < 4)
			Reallocate();
		m_encoder.Convert("".AsSpan(), m_output.Span, true, out _, out var finalBytesUsed, out _);
		m_output = m_output[finalBytesUsed..];

		// write the length (as a 64-bit integer) in the reserved space
		var textLength = Position - (lengthPosition + 9);
		m_buffer[lengthPosition] = 0xFE;
		BinaryPrimitives.WriteUInt64LittleEndian(m_buffer.AsSpan(lengthPosition + 1), (ulong) textLength);
#else
		this.WriteLengthEncodedString(stringBuilder.ToString());
#endif
	}

	public void WriteString(short value)
	{
		Utf8Formatter.TryFormat(value, GetSpan(6), out var bytesWritten);
		m_output = m_output[bytesWritten..];
	}

	public void WriteString(ushort value)
	{
		Utf8Formatter.TryFormat(value, GetSpan(5), out var bytesWritten);
		m_output = m_output[bytesWritten..];
	}

	public void WriteString(int value)
	{
		Utf8Formatter.TryFormat(value, GetSpan(11), out var bytesWritten);
		m_output = m_output[bytesWritten..];
	}

	public void WriteString(uint value)
	{
		Utf8Formatter.TryFormat(value, GetSpan(10), out var bytesWritten);
		m_output = m_output[bytesWritten..];
	}

	public void WriteString(long value)
	{
		Utf8Formatter.TryFormat(value, GetSpan(20), out var bytesWritten);
		m_output = m_output[bytesWritten..];
	}

	public void WriteString(ulong value)
	{
		Utf8Formatter.TryFormat(value, GetSpan(20), out var bytesWritten);
		m_output = m_output[bytesWritten..];
	}

	private void Reallocate(int additional = 0)
	{
		var usedLength = Position;
		var newBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(usedLength + additional, m_buffer.Length * 2));
		m_buffer.AsSpan(0, usedLength).CopyTo(newBuffer);
		ArrayPool<byte>.Shared.Return(m_buffer);
		m_buffer = newBuffer;
		m_output = new(m_buffer, usedLength, m_buffer.Length - usedLength);
	}

	private Encoder? m_encoder;
	private byte[] m_buffer;
	private Memory<byte> m_output;
}

internal static class ByteBufferWriterExtensions
{
	extension(ByteBufferWriter writer)
	{
		public void WriteLengthEncodedInteger(ulong value)
		{
			switch (value)
			{
				case < 251:
					writer.Write((byte) value);
					break;

				case < 65536:
					writer.Write((byte) 0xfc);
					writer.Write((ushort) value);
					break;

				case < 16777216:
					writer.Write((uint) ((value << 8) | 0xfd));
					break;

				default:
					writer.Write((byte) 0xfe);
					writer.Write(value);
					break;
			}
		}

		public void WriteLengthEncodedString(string value) => writer.WriteLengthEncodedString(value.AsSpan());

		public void WriteLengthEncodedString(ReadOnlySpan<char> value)
		{
			var byteCount = Encoding.UTF8.GetByteCount(value);
			writer.WriteLengthEncodedInteger((ulong) byteCount);
			writer.Write(value);
		}

		public void WriteLengthEncodedAsciiString(string value)
		{
			writer.WriteLengthEncodedInteger((ulong) value.Length);
			writer.WriteAscii(value.AsSpan());
		}

		public void WriteNullTerminatedString(string value)
		{
			writer.Write(value);
			writer.Write((byte) 0);
		}
	}
}
