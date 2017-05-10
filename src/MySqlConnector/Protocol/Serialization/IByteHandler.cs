using System;
using System.Threading.Tasks;

namespace MySql.Data.Protocol.Serialization
{
	internal interface IByteHandler
	{
		/// <summary>
		/// Reads data from this byte handler.
		/// </summary>
		/// <param name="buffer">The buffer to read into.</param>
		/// <param name="ioBehavior">The <see cref="IOBehavior"/> to use when reading data.</param>
		/// <returns>A <see cref="ValueTask{Int32}"/>. Number of bytes read.</returns>
		/// If reading failed, zero bytes will be returned. This
		/// <see cref="ArraySegment{Byte}"/> will be valid to read from until the next time <see cref="ReadBytesAsync"/> or
		/// <see cref="WriteBytesAsync"/> is called.</returns>
		ValueOrCallback<int> ReadBytesAsync(ArraySegment<byte> buffer, IOBehavior ioBehavior);

		/// <summary>
		/// Writes data to this byte handler.
		/// </summary>
		/// <param name="data">The data to write.</param>
		/// <param name="ioBehavior">The <see cref="IOBehavior"/> to use when writing.</param>
		/// <returns>A <see cref="ValueTask{Int32}"/>. The value of this object is not defined.</returns>
		ValueOrCallback<int> WriteBytesAsync(ArraySegment<byte> data, IOBehavior ioBehavior);
	}
}
