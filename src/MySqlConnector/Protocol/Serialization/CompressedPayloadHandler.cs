using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Utilities;

namespace MySqlConnector.Protocol.Serialization
{
	internal sealed class CompressedPayloadHandler : IPayloadHandler
	{
		public CompressedPayloadHandler(IByteHandler byteHandler)
		{
			m_uncompressedStream = new MemoryStream();
			m_uncompressedStreamByteHandler = new StreamByteHandler(m_uncompressedStream);
			m_byteHandler = byteHandler;
			m_bufferedByteReader = new BufferedByteReader();
			m_compressedBufferedByteReader = new BufferedByteReader();
		}

		public void Dispose()
		{
			m_byteHandler.Dispose();
			m_uncompressedStreamByteHandler.Dispose();
			m_uncompressedStream.Dispose();
		}

		public void StartNewConversation()
		{
			m_compressedSequenceNumber = 0;
			m_uncompressedSequenceNumber = 0;
		}

		public IByteHandler ByteHandler
		{
			get => m_byteHandler;
			set => throw new NotSupportedException();
		}

		public ValueTask<ArraySegment<byte>> ReadPayloadAsync(ArraySegmentHolder<byte> cache, ProtocolErrorBehavior protocolErrorBehavior, IOBehavior ioBehavior)
		{
			using var compressedByteHandler = new CompressedByteHandler(this, protocolErrorBehavior);
			return ProtocolUtility.ReadPayloadAsync(m_bufferedByteReader, compressedByteHandler, () => -1, cache, protocolErrorBehavior, ioBehavior);
		}

		public ValueTask<int> WritePayloadAsync(ArraySegment<byte> payload, IOBehavior ioBehavior)
		{
			// break the payload up into (possibly more than one) uncompressed packets
			return ProtocolUtility.WritePayloadAsync(m_uncompressedStreamByteHandler, GetNextUncompressedSequenceNumber, payload, ioBehavior).ContinueWith(_ =>
			{
				if (m_uncompressedStream.Length == 0)
					return default(ValueTask<int>);

				if (!m_uncompressedStream.TryGetBuffer(out var uncompressedData))
					throw new InvalidOperationException("Couldn't get uncompressed stream buffer.");

				return CompressAndWrite(uncompressedData, ioBehavior)
					.ContinueWith(__ =>
					{
						// reset the uncompressed stream to accept more data
						m_uncompressedStream.SetLength(0);
						return default(ValueTask<int>);
					});
			});
		}

		private ValueTask<int> ReadBytesAsync(ArraySegment<byte> buffer, ProtocolErrorBehavior protocolErrorBehavior, IOBehavior ioBehavior)
		{
			// satisfy the read from cache if possible
			if (m_remainingData.Count > 0)
			{
				var bytesToRead = Math.Min(m_remainingData.Count, buffer.Count);
				Buffer.BlockCopy(m_remainingData.Array, m_remainingData.Offset, buffer.Array, buffer.Offset, bytesToRead);
				m_remainingData = m_remainingData.Slice(bytesToRead);
				return new ValueTask<int>(bytesToRead);
			}

			// read the compressed header (seven bytes)
			return m_compressedBufferedByteReader.ReadBytesAsync(m_byteHandler, 7, ioBehavior)
				.ContinueWith(headerReadBytes =>
				{
					if (headerReadBytes.Count < 7)
					{
						return protocolErrorBehavior == ProtocolErrorBehavior.Ignore ?
							default(ValueTask<int>) :
							ValueTaskExtensions.FromException<int>(new EndOfStreamException("Wanted to read 7 bytes but only read {0} when reading compressed packet header".FormatInvariant(headerReadBytes.Count)));
					}

					var payloadLength = (int) SerializationUtility.ReadUInt32(headerReadBytes.Array, headerReadBytes.Offset, 3);
					int packetSequenceNumber = headerReadBytes.Array[headerReadBytes.Offset + 3];
					var uncompressedLength = (int) SerializationUtility.ReadUInt32(headerReadBytes.Array, headerReadBytes.Offset + 4, 3);

					// verify the compressed packet sequence number
					var expectedSequenceNumber = GetNextCompressedSequenceNumber() % 256;
					if (packetSequenceNumber != expectedSequenceNumber)
					{
						if (protocolErrorBehavior == ProtocolErrorBehavior.Ignore)
							return default(ValueTask<int>);

						var exception = MySqlProtocolException.CreateForPacketOutOfOrder(expectedSequenceNumber, packetSequenceNumber);
						return ValueTaskExtensions.FromException<int>(exception);
					}

					// MySQL protocol resets the uncompressed sequence number back to the sequence number of this compressed packet.
					// This isn't in the documentation, but the code explicitly notes that uncompressed packets are modified by compression:
					//  - https://github.com/mysql/mysql-server/blob/c28e258157f39f25e044bb72e8bae1ff00989a3d/sql/net_serv.cc#L276
					//  - https://github.com/mysql/mysql-server/blob/c28e258157f39f25e044bb72e8bae1ff00989a3d/sql/net_serv.cc#L225-L227
					if (!m_isContinuationPacket)
						m_uncompressedSequenceNumber = packetSequenceNumber;

					// except this doesn't happen when uncompressed packets need to be broken up across multiple compressed packets
					m_isContinuationPacket = payloadLength == ProtocolUtility.MaxPacketSize || uncompressedLength == ProtocolUtility.MaxPacketSize;

					return m_compressedBufferedByteReader.ReadBytesAsync(m_byteHandler, payloadLength, ioBehavior)
						.ContinueWith(payloadReadBytes =>
						{
							if (payloadReadBytes.Count < payloadLength)
							{
								return protocolErrorBehavior == ProtocolErrorBehavior.Ignore ?
									default(ValueTask<int>) :
									ValueTaskExtensions.FromException<int>(new EndOfStreamException("Wanted to read {0} bytes but only read {1} when reading compressed payload".FormatInvariant(payloadLength, payloadReadBytes.Count)));
							}

							if (uncompressedLength == 0)
							{
								// data is uncompressed
								m_remainingData = payloadReadBytes;
							}
							else
							{
								// check CMF (Compression Method and Flags) and FLG (Flags) bytes for expected values
								var cmf = payloadReadBytes.Array[payloadReadBytes.Offset];
								var flg = payloadReadBytes.Array[payloadReadBytes.Offset + 1];
								if (cmf != 0x78 || ((flg & 0x40) == 0x40) || ((cmf * 256 + flg) % 31 != 0))
								{
									// CMF = 0x78: 32K Window Size + deflate compression
									// FLG & 0x40: has preset dictionary (not supported)
									// CMF*256+FLG is a multiple of 31: header checksum
									return protocolErrorBehavior == ProtocolErrorBehavior.Ignore ?
										default(ValueTask<int>) :
										ValueTaskExtensions.FromException<int>(new NotSupportedException("Unsupported zlib header: {0:X2}{1:X2}".FormatInvariant(cmf, flg)));
								}

								// zlib format (https://www.ietf.org/rfc/rfc1950.txt) is: [two header bytes] [deflate-compressed data] [four-byte checksum]
								// .NET implements the middle part with DeflateStream; need to handle header and checksum explicitly
								const int headerSize = 2;
								const int checksumSize = 4;
								var uncompressedData = new byte[uncompressedLength];
								using var compressedStream = new MemoryStream(payloadReadBytes.Array, payloadReadBytes.Offset + headerSize, payloadReadBytes.Count - headerSize - checksumSize);
								using var decompressingStream = new DeflateStream(compressedStream, CompressionMode.Decompress);
								var bytesRead = decompressingStream.Read(uncompressedData, 0, uncompressedLength);
								m_remainingData = new ArraySegment<byte>(uncompressedData, 0, bytesRead);

								var checksum = ComputeAdler32Checksum(uncompressedData, 0, bytesRead);
								int adlerStartOffset = payloadReadBytes.Offset + payloadReadBytes.Count - 4;
								if (payloadReadBytes.Array[adlerStartOffset + 0] != ((checksum >> 24) & 0xFF) ||
									payloadReadBytes.Array[adlerStartOffset + 1] != ((checksum >> 16) & 0xFF) ||
									payloadReadBytes.Array[adlerStartOffset + 2] != ((checksum >> 8) & 0xFF) ||
									payloadReadBytes.Array[adlerStartOffset + 3] != (checksum & 0xFF))
								{
									return protocolErrorBehavior == ProtocolErrorBehavior.Ignore ?
										default(ValueTask<int>) :
										ValueTaskExtensions.FromException<int>(new NotSupportedException("Invalid Adler-32 checksum of uncompressed data."));
								}
							}

							var bytesToRead = Math.Min(m_remainingData.Count, buffer.Count);
							Buffer.BlockCopy(m_remainingData.Array, m_remainingData.Offset, buffer.Array, buffer.Offset, bytesToRead);
							m_remainingData = m_remainingData.Slice(bytesToRead);
							return new ValueTask<int>(bytesToRead);

						});
				});
		}

		private int GetNextCompressedSequenceNumber() => m_compressedSequenceNumber++;

		private int GetNextUncompressedSequenceNumber() => m_uncompressedSequenceNumber++;

		private ValueTask<int> CompressAndWrite(ArraySegment<byte> remainingUncompressedData, IOBehavior ioBehavior)
		{
			var remainingUncompressedBytes = Math.Min(remainingUncompressedData.Count, ProtocolUtility.MaxPacketSize);

			// don't compress small packets; 80 bytes is typically a good cutoff
			var compressedData = default(ArraySegment<byte>);
			if (remainingUncompressedBytes > 80)
			{
				using var compressedStream = new MemoryStream();

				// write CMF: 32K window + deflate algorithm
				compressedStream.WriteByte(0x78);

				// write FLG: maximum compression + checksum
				compressedStream.WriteByte(0xDA);

				using (var deflateStream = new DeflateStream(compressedStream, CompressionLevel.Optimal, leaveOpen: true))
					deflateStream.Write(remainingUncompressedData.Array, remainingUncompressedData.Offset, remainingUncompressedBytes);

				// write Adler-32 checksum to stream
				var checksum = ComputeAdler32Checksum(remainingUncompressedData.Array, remainingUncompressedData.Offset, remainingUncompressedBytes);
				compressedStream.WriteByte((byte) ((checksum >> 24) & 0xFF));
				compressedStream.WriteByte((byte) ((checksum >> 16) & 0xFF));
				compressedStream.WriteByte((byte) ((checksum >> 8) & 0xFF));
				compressedStream.WriteByte((byte) (checksum & 0xFF));

				if (!compressedStream.TryGetBuffer(out compressedData))
					throw new InvalidOperationException("Couldn't get compressed stream buffer.");
			}

			uint uncompressedLength = (uint) remainingUncompressedBytes;
			if (compressedData.Array == null || compressedData.Count >= remainingUncompressedBytes)
			{
				// setting the length to 0 indicates sending uncompressed data
				uncompressedLength = 0;
				compressedData = remainingUncompressedData.Slice(0, remainingUncompressedBytes);
			}

			var buffer = new byte[compressedData.Count + 7];
			SerializationUtility.WriteUInt32((uint) compressedData.Count, buffer, 0, 3);
			buffer[3] = (byte) GetNextCompressedSequenceNumber();
			SerializationUtility.WriteUInt32(uncompressedLength, buffer, 4, 3);
			Buffer.BlockCopy(compressedData.Array, compressedData.Offset, buffer, 7, compressedData.Count);

			remainingUncompressedData = remainingUncompressedData.Slice(remainingUncompressedBytes);
			return m_byteHandler.WriteBytesAsync(new ArraySegment<byte>(buffer, 0, buffer.Length), ioBehavior)
				.ContinueWith(_ => remainingUncompressedData.Count == 0 ? default(ValueTask<int>) :
					CompressAndWrite(remainingUncompressedData, ioBehavior));
		}

		private uint ComputeAdler32Checksum(byte[] data, int offset, int length)
		{
			int s1 = 1, s2 = 0;
			for (int i = 0; i < length; i++)
			{
				s1 = (s1 + data[offset + i]) % 65521;
				s2 = (s2 + s1) % 65521;
			}
			return (((uint) s2) << 16) | (uint) s1;
		}

		// CompressedByteHandler implements IByteHandler and delegates reading bytes back to the CompressedPayloadHandler class.
		private sealed class CompressedByteHandler : IByteHandler
		{
			public CompressedByteHandler(CompressedPayloadHandler compressedPayloadHandler, ProtocolErrorBehavior protocolErrorBehavior)
			{
				m_compressedPayloadHandler = compressedPayloadHandler;
				m_protocolErrorBehavior = protocolErrorBehavior;
			}

			public void Dispose()
			{
			}

			public int RemainingTimeout
			{
				get => m_compressedPayloadHandler.ByteHandler.RemainingTimeout;
				set => m_compressedPayloadHandler.ByteHandler.RemainingTimeout = value;
			}

			public ValueTask<int> ReadBytesAsync(ArraySegment<byte> buffer, IOBehavior ioBehavior) =>
				m_compressedPayloadHandler.ReadBytesAsync(buffer, m_protocolErrorBehavior, ioBehavior);

			public ValueTask<int> WriteBytesAsync(ArraySegment<byte> data, IOBehavior ioBehavior) => throw new NotSupportedException();

			readonly CompressedPayloadHandler m_compressedPayloadHandler;
			readonly ProtocolErrorBehavior m_protocolErrorBehavior;
		}

		MemoryStream m_uncompressedStream;
		IByteHandler m_uncompressedStreamByteHandler;
		IByteHandler m_byteHandler;
		readonly BufferedByteReader m_bufferedByteReader;
		readonly BufferedByteReader m_compressedBufferedByteReader;
		int m_compressedSequenceNumber;
		int m_uncompressedSequenceNumber;
		ArraySegment<byte> m_remainingData;
		bool m_isContinuationPacket;
	}
}
