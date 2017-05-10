using System;
using System.Threading.Tasks;

namespace MySql.Data.Protocol.Serialization
{
	internal interface IPayloadHandler
	{
		/// <summary>
		/// Starts a new "conversation" with the MySQL Server. This resets the "<a href="https://dev.mysql.com/doc/internals/en/sequence-id.html">sequence id</a>"
		/// and should be called when a new command begins.
		/// </summary>
		void StartNewConversation();

		/// <summary>
		/// Gets or sets the underlying <see cref="IByteHandler"/> that data is read from and written to.
		/// </summary>
		IByteHandler ByteHandler { get; set; }

		/// <summary>
		/// Reads the next payload.
		/// </summary>
		/// <param name="cache">An <see cref="ArraySegmentHolder{Byte}"/> that will cache any buffers allocated during this
		/// read. (To disable caching, pass <code>new ArraySegmentHolder&lt;byte&gt;</code> so the cache will be garbage-collected
		/// when this method returns.)</param>
		/// <param name="protocolErrorBehavior">The <see cref="ProtocolErrorBehavior"/> to use if there is a protocol error.</param>
		/// <param name="ioBehavior">The <see cref="IOBehavior"/> to use when reading data.</param>
		/// <returns>An <see cref="ArraySegment{Byte}"/> containing the data that was read. This
		/// <see cref="ArraySegment{Byte}"/> will be valid to read from until the next time <see cref="ReadPayloadAsync"/> or
		/// <see cref="WritePayloadAsync"/> is called.</returns>
		ValueOrCallback<ArraySegment<byte>> ReadPayloadAsync(ArraySegmentHolder<byte> cache, ProtocolErrorBehavior protocolErrorBehavior, IOBehavior ioBehavior);

		/// <summary>
		/// Writes a payload.
		/// </summary>
		/// <param name="payload">The data to write.</param>
		/// <param name="ioBehavior">The <see cref="IOBehavior"/> to use when writing.</param>
		/// <returns>A <see cref="ValueTask{Int32}"/>. The value of this object is not defined.</returns>
		ValueOrCallback<int> WritePayloadAsync(ArraySegment<byte> payload, IOBehavior ioBehavior);
	}
}
