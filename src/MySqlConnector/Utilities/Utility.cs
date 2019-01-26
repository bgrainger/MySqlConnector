using System;
using System.Buffers.Text;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Reflection;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MySqlConnector.Utilities
{
	internal static class Utility
	{
		public static void Dispose<T>(ref T disposable)
			where T : class, IDisposable
		{
			if (disposable != null)
			{
				disposable.Dispose();
				disposable = null;
			}
		}

		public static string FormatInvariant(this string format, params object[] args) =>
			string.Format(CultureInfo.InvariantCulture, format, args);

#if NET45 || NET461 || NET471 || NETSTANDARD1_3 || NETSTANDARD2_0
		public static string GetString(this Encoding encoding, ReadOnlySpan<byte> span)
		{
			if (span.Length == 0)
				return "";
#if NET45
			return encoding.GetString(span.ToArray());
#else
			unsafe
			{
				fixed (byte* ptr = span)
					return encoding.GetString(ptr, span.Length);
			}
#endif
		}

		public static unsafe void GetBytes(this Encoding encoding, ReadOnlySpan<char> chars, Span<byte> bytes)
		{
			fixed (char* charsPtr = chars)
			fixed (byte* bytesPtr = bytes)
			{
				encoding.GetBytes(charsPtr, chars.Length, bytesPtr, bytes.Length);
			}
		}
#endif

#if NET461 || NET471 || NETSTANDARD2_0
		public static unsafe void Convert(this Encoder encoder, ReadOnlySpan<char> chars, Span<byte> bytes, bool flush, out int charsUsed, out int bytesUsed, out bool completed)
		{
			fixed (char* charsPtr = chars)
			fixed (byte* bytesPtr = bytes)
			{
				// MemoryMarshal.GetNonNullPinnableReference is internal, so fake it by using an invalid but non-null pointer; this
				// prevents Convert from throwing an exception when the output buffer is empty
				encoder.Convert(charsPtr, chars.Length, bytesPtr == null ? (byte*) 1 : bytesPtr, bytes.Length, flush, out charsUsed, out bytesUsed, out completed);
			}
		}
#endif

		/// <summary>
		/// Loads a RSA public key from a PEM string. Taken from <a href="https://stackoverflow.com/a/32243171/23633">Stack Overflow</a>.
		/// </summary>
		/// <param name="publicKey">The public key, in PEM format.</param>
		/// <returns>An RSA public key, or <c>null</c> on failure.</returns>
		public static RSA DecodeX509PublicKey(string publicKey)
		{
			var x509Key = System.Convert.FromBase64String(publicKey.Replace("-----BEGIN PUBLIC KEY-----", "").Replace("-----END PUBLIC KEY-----", ""));

			// encoded OID sequence for  PKCS #1 rsaEncryption szOID_RSA_RSA = "1.2.840.113549.1.1.1"
			byte[] seqOid = { 0x30, 0x0D, 0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01, 0x05, 0x00 };

			// ---------  Set up stream to read the asn.1 encoded SubjectPublicKeyInfo blob  ------
			using var stream = new MemoryStream(x509Key);
			using var reader = new BinaryReader(stream); //wrap Memory Stream with BinaryReader for easy reading
			var temp = reader.ReadUInt16();
			switch (temp)
			{
			case 0x8130:
				reader.ReadByte(); //advance 1 byte
				break;
			case 0x8230:
				reader.ReadInt16(); //advance 2 bytes
				break;
			default:
				throw new FormatException("Expected 0x8130 or 0x8230 but read {0:X4}".FormatInvariant(temp));
			}

			var seq = reader.ReadBytes(15);
			if (!seq.SequenceEqual(seqOid)) //make sure Sequence for OID is correct
				throw new FormatException("Expected RSA OID but read {0}".FormatInvariant(BitConverter.ToString(seq)));

			temp = reader.ReadUInt16();
			if (temp == 0x8103) //data read as little endian order (actual data order for Bit String is 03 81)
				reader.ReadByte(); //advance 1 byte
			else if (temp == 0x8203)
				reader.ReadInt16(); //advance 2 bytes
			else
				throw new FormatException("Expected 0x8130 or 0x8230 but read {0:X4}".FormatInvariant(temp));

			var bt = reader.ReadByte();
			if (bt != 0x00) //expect null byte next
				throw new FormatException("Expected 0x00 but read {0:X2}".FormatInvariant(bt));

			temp = reader.ReadUInt16();
			if (temp == 0x8130) //data read as little endian order (actual data order for Sequence is 30 81)
				reader.ReadByte(); //advance 1 byte
			else if (temp == 0x8230)
				reader.ReadInt16(); //advance 2 bytes
			else
				throw new FormatException("Expected 0x8130 or 0x8230 but read {0:X4}".FormatInvariant(temp));

			temp = reader.ReadUInt16();
			byte lowbyte;
			byte highbyte = 0x00;

			if (temp == 0x8102)
			{
				//data read as little endian order (actual data order for Integer is 02 81)
				lowbyte = reader.ReadByte(); // read next bytes which is bytes in modulus
			}
			else if (temp == 0x8202)
			{
				highbyte = reader.ReadByte(); //advance 2 bytes
				lowbyte = reader.ReadByte();
			}
			else
			{
				throw new FormatException("Expected 0x8102 or 0x8202 but read {0:X4}".FormatInvariant(temp));
			}

			var modulusSize = highbyte * 256 + lowbyte;

			var firstbyte = reader.ReadByte();
			reader.BaseStream.Seek(-1, SeekOrigin.Current);

			if (firstbyte == 0x00)
			{
				//if first byte (highest order) of modulus is zero, don't include it
				reader.ReadByte(); //skip this null byte
				modulusSize -= 1; //reduce modulus buffer size by 1
			}

			var modulus = reader.ReadBytes(modulusSize); //read the modulus bytes

			if (reader.ReadByte() != 0x02) //expect an Integer for the exponent data
				throw new FormatException("Expected 0x02");
			int exponentSize = reader.ReadByte(); // should only need one byte for actual exponent data (for all useful values)
			var exponent = reader.ReadBytes(exponentSize);

			// ------- create RSACryptoServiceProvider instance and initialize with public key -----
			var rsa = RSA.Create();
			var rsaKeyInfo = new RSAParameters
			{
				Modulus = modulus,
				Exponent = exponent
			};
			rsa.ImportParameters(rsaKeyInfo);
			return rsa;
		}

		/// <summary>
		/// Returns a new <see cref="ArraySegment{T}"/> that starts at index <paramref name="index"/> into <paramref name="arraySegment"/>.
		/// </summary>
		/// <param name="arraySegment">The <see cref="ArraySegment{T}"/> from which to create a slice.</param>
		/// <param name="index">The non-negative, zero-based starting index of the new slice (relative to <see cref="ArraySegment{T}.Offset"/> of <paramref name="arraySegment"/>.</param>
		/// <returns>A new <see cref="ArraySegment{T}"/> starting at the <paramref name="index"/>th element of <paramref name="arraySegment"/> and continuing to the end of <paramref name="arraySegment"/>.</returns>
		public static ArraySegment<T> Slice<T>(this ArraySegment<T> arraySegment, int index) =>
			new ArraySegment<T>(arraySegment.Array, arraySegment.Offset + index, arraySegment.Count - index);

		/// <summary>
		/// Returns a new <see cref="ArraySegment{T}"/> that starts at index <paramref name="index"/> into <paramref name="arraySegment"/> and has a length of <paramref name="length"/>.
		/// </summary>
		/// <param name="arraySegment">The <see cref="ArraySegment{T}"/> from which to create a slice.</param>
		/// <param name="index">The non-negative, zero-based starting index of the new slice (relative to <see cref="ArraySegment{T}.Offset"/> of <paramref name="arraySegment"/>.</param>
		/// <param name="length">The non-negative length of the new slice.</param>
		/// <returns>A new <see cref="ArraySegment{T}"/> of length <paramref name="length"/>, starting at the <paramref name="index"/>th element of <paramref name="arraySegment"/>.</returns>
		public static ArraySegment<T> Slice<T>(this ArraySegment<T> arraySegment, int index, int length) =>
			new ArraySegment<T>(arraySegment.Array, arraySegment.Offset + index, length);

		/// <summary>
		/// Returns a new <see cref="byte[]"/> that is a slice of <paramref name="input"/> starting at <paramref name="offset"/>.
		/// </summary>
		/// <param name="input">The array to slice.</param>
		/// <param name="offset">The offset at which to slice.</param>
		/// <returns>A new <see cref="byte[]"/> that is a slice of <paramref name="input"/> from <paramref name="offset"/> to the end.</returns>
		public static byte[] ArraySlice(byte[] input, int offset, int length)
		{
			if (offset == 0 && length == input.Length)
				return input;
			var slice = new byte[length];
			Array.Copy(input, offset, slice, 0, slice.Length);
			return slice;
		}

		/// <summary>
		/// Finds the next index of <paramref name="pattern"/> in <paramref name="data"/>, starting at index <paramref name="offset"/>.
		/// </summary>
		/// <param name="data">The array to search.</param>
		/// <param name="offset">The offset at which to start searching.</param>
		/// <param name="pattern">The pattern to find in <paramref name="data"/>.</param>
		/// <returns>The offset of <paramref name="pattern"/> within <paramref name="data"/>, or <c>-1</c> if <paramref name="pattern"/> was not found.</returns>
		public static int FindNextIndex(ReadOnlySpan<byte> data, int offset, ReadOnlySpan<byte> pattern)
		{
			var limit = data.Length - pattern.Length;
			for (var start = offset; start <= limit; start++)
			{
				var i = 0;
				for (; i < pattern.Length; i++)
				{
					if (data[start + i] != pattern[i])
						break;
				}
				if (i == pattern.Length)
					return start;
			}
			return -1;
		}

		/// <summary>
		/// Resizes <paramref name="resizableArray"/> to hold at least <paramref name="newLength"/> items.
		/// </summary>
		/// <remarks><paramref name="resizableArray"/> may be <c>null</c>, in which case a new <see cref="ResizableArray{T}"/> will be allocated.</remarks>
		public static void Resize<T>(ref ResizableArray<T> resizableArray, int newLength)
		{
			if (resizableArray == null)
				resizableArray = new ResizableArray<T>();
			resizableArray.DoResize(newLength);
		}

		public static TimeSpan ParseTimeSpan(ReadOnlySpan<byte> value)
		{
			var originalValue = value;

			// parse (optional) leading minus sign
			var isNegative = false;
			if (value.Length > 0 && value[0] == 0x2D)
			{
				isNegative = true;
				value = value.Slice(1);
			}

			// parse hours (0-838)
			if (!Utf8Parser.TryParse(value, out int hours, out var bytesConsumed) || hours < 0 || hours > 838)
				goto InvalidTimeSpan;
			if (value.Length == bytesConsumed || value[bytesConsumed] != 58)
				goto InvalidTimeSpan;
			value = value.Slice(bytesConsumed + 1);

			// parse minutes (0-59)
			if (!Utf8Parser.TryParse(value, out int minutes, out bytesConsumed) || bytesConsumed != 2 || minutes < 0 || minutes > 59)
				goto InvalidTimeSpan;
			if (value.Length < 3 || value[2] != 58)
				goto InvalidTimeSpan;
			value = value.Slice(3);

			// parse seconds (0-59)
			if (!Utf8Parser.TryParse(value, out int seconds, out bytesConsumed) || bytesConsumed != 2 || seconds < 0 || seconds > 59)
				goto InvalidTimeSpan;

			int microseconds;
			if (value.Length == 2)
			{
				microseconds = 0;
			}
			else
			{
				if (value[2] != 46)
					goto InvalidTimeSpan;
				value = value.Slice(3);
				if (!Utf8Parser.TryParse(value, out microseconds, out bytesConsumed) || bytesConsumed != value.Length || microseconds < 0 || microseconds > 999_999)
					goto InvalidTimeSpan;
				for (; bytesConsumed < 6; bytesConsumed++)
					microseconds *= 10;
			}

			if (isNegative)
			{
				hours = -hours;
				minutes = -minutes;
				seconds = -seconds;
				microseconds = -microseconds;
			}
			return new TimeSpan(0, hours, minutes, seconds, microseconds / 1000) + TimeSpan.FromTicks(microseconds % 1000 * 10);

			InvalidTimeSpan:
			throw new FormatException("Couldn't interpret '{0}' as a valid TimeSpan".FormatInvariant(Encoding.UTF8.GetString(originalValue)));
		}

#if NET45
		public static Task CompletedTask
		{
			get
			{
				if (s_completedTask == null)
				{
					var tcs = new TaskCompletionSource<object>();
					tcs.SetResult(null);
					s_completedTask = tcs.Task;
				}
				return s_completedTask;
			}
		}
		static Task s_completedTask;

		public static Task TaskFromException(Exception exception) => TaskFromException<object>(exception);
		public static Task<T> TaskFromException<T>(Exception exception)
		{
			var tcs = new TaskCompletionSource<T>();
			tcs.SetException(exception);
			return tcs.Task;
		}
#else
		public static Task CompletedTask => Task.CompletedTask;
		public static Task TaskFromException(Exception exception) => Task.FromException(exception);
		public static Task<T> TaskFromException<T>(Exception exception) => Task.FromException<T>(exception);
#endif

		public static byte[] TrimZeroByte(byte[] value)
		{
			if (value[value.Length - 1] == 0)
				Array.Resize(ref value, value.Length - 1);
			return value;
		}

#if NET45
		public static bool TryGetBuffer(this MemoryStream memoryStream, out ArraySegment<byte> buffer)
		{
			try
			{
				var rawBuffer = memoryStream.GetBuffer();
				buffer = new ArraySegment<byte>(rawBuffer, 0, checked((int) memoryStream.Length));
				return true;
			}
			catch (UnauthorizedAccessException)
			{
				buffer = default(ArraySegment<byte>);
				return false;
			}
		}
#endif

		public static void SwapBytes(byte[] bytes, int offset1, int offset2)
		{
			byte swap = bytes[offset1];
			bytes[offset1] = bytes[offset2];
			bytes[offset2] = swap;
		}

#if NET45 || NET461
		public static bool IsWindows() => Environment.OSVersion.Platform == PlatformID.Win32NT;

		public static void GetOSDetails(out string os, out string osDescription, out string architecture)
		{
			os = Environment.OSVersion.Platform == PlatformID.Win32NT ? "Windows" :
				Environment.OSVersion.Platform == PlatformID.Unix ? "Linux" :
				Environment.OSVersion.Platform == PlatformID.MacOSX ? "macOS" : null;
			osDescription = Environment.OSVersion.VersionString;
			architecture = IntPtr.Size == 8 ? "X64" : "X86";
		}
#else
		public static bool IsWindows()
		{
			try
			{
				// OSPlatform.Windows is not supported on AWS Lambda
				return RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
			}
			catch (PlatformNotSupportedException)
			{
				return false;
			}
		}

		public static void GetOSDetails(out string os, out string osDescription, out string architecture)
		{
			os = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Windows" :
				RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "Linux" :
				RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "macOS" : null;
			osDescription = RuntimeInformation.OSDescription;
			architecture = RuntimeInformation.ProcessArchitecture.ToString();
		}
#endif

#if NET45 || NET461
		public static SslProtocols GetDefaultSslProtocols()
		{
			if (!s_defaultSslProtocols.HasValue)
			{
				// Prior to .NET Framework 4.7, SslProtocols.None is not a valid argument to SslStream.AuthenticateAsClientAsync.
				// If the NET461 build is loaded by an application that targets. NET 4.7 (or later), or if app.config has set
				// Switch.System.Net.DontEnableSystemDefaultTlsVersions to false, then SslProtocols.None will work; otherwise,
				// if the application targets .NET 4.6.2 or earlier and hasn't changed the AppContext switch, then it will
				// fail at runtime. We attempt to determine if it will fail by accessing the internal static
				// ServicePointManager.DisableSystemDefaultTlsVersions property, which controls whether SslProtocols.None is
				// an acceptable value.
				bool disableSystemDefaultTlsVersions;
				try
				{
					var property = typeof(ServicePointManager).GetProperty("DisableSystemDefaultTlsVersions", BindingFlags.NonPublic | BindingFlags.Static);
					disableSystemDefaultTlsVersions = property == null || (property.GetValue(null) is bool b && b);
				}
				catch (Exception)
				{
					// couldn't access the property; assume the safer default of 'true'
					disableSystemDefaultTlsVersions = true;
				}

				s_defaultSslProtocols = disableSystemDefaultTlsVersions ? (SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12) : SslProtocols.None;
			}

			return s_defaultSslProtocols.Value;
		}

		static SslProtocols? s_defaultSslProtocols;
#elif NETSTANDARD1_3
		public static SslProtocols GetDefaultSslProtocols() => SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12;
#else
		public static SslProtocols GetDefaultSslProtocols() => SslProtocols.None;
#endif
	}
}
