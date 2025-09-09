#if NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER
using System.Buffers;
#endif
using System.Buffers.Text;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
#if NET462
using System.Net;
using System.Reflection;
#endif
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Text;

namespace MySqlConnector.Utilities;

internal static class Utility
{
	public static void Dispose<T>(ref T? disposable)
		where T : class, IDisposable
	{
		if (disposable is not null)
		{
			disposable.Dispose();
			disposable = null;
		}
	}

#if !NETCOREAPP2_1_OR_GREATER && !NETSTANDARD2_1_OR_GREATER
	extension(Encoding encoding)
	{
		public string GetString(ReadOnlySpan<byte> span)
		{
			if (span.Length == 0)
				return "";
			unsafe
			{
				fixed (byte* ptr = &MemoryMarshal.GetReference(span))
					return encoding.GetString(ptr, span.Length);
			}
		}

		public unsafe int GetByteCount(ReadOnlySpan<char> chars)
		{
			if (chars.Length == 0)
				return 0;

			fixed (char* charsPtr = &MemoryMarshal.GetReference(chars))
			{
				return encoding.GetByteCount(charsPtr, chars.Length);
			}
		}

		public unsafe int GetBytes(ReadOnlySpan<char> chars, Span<byte> bytes)
		{
			fixed (char* charsPtr = &MemoryMarshal.GetReference(chars))
			{
				fixed (byte* bytesPtr = &MemoryMarshal.GetReference(bytes))
				{
					return encoding.GetBytes(charsPtr, chars.Length, bytesPtr, bytes.Length);
				}
			}
		}
	}
#endif

#if !NETCOREAPP2_1_OR_GREATER && !NETSTANDARD2_1_OR_GREATER
	extension(Encoder encoder)
	{
		public unsafe void Convert(ReadOnlySpan<char> chars, Span<byte> bytes, bool flush, out int charsUsed, out int bytesUsed, out bool completed)
		{
			fixed (char* charsPtr = &MemoryMarshal.GetReference(chars))
			{
				fixed (byte* bytesPtr = &MemoryMarshal.GetReference(bytes))
				{
					// MemoryMarshal.GetNonNullPinnableReference is internal, so fake it by using an invalid but non-null pointer; this
					// prevents Convert from throwing an exception when the output buffer is empty
					encoder.Convert(charsPtr, chars.Length, bytesPtr is null ? (byte*) 1 : bytesPtr, bytes.Length, flush, out charsUsed, out bytesUsed, out completed);
				}
			}
		}

		public unsafe int GetByteCount(ReadOnlySpan<char> chars, bool flush)
		{
			fixed (char* charsPtr = &MemoryMarshal.GetReference(chars))
			{
				// MemoryMarshal.GetNonNullPinnableReference is internal, so fake it by using an invalid but non-null pointer; this
				// prevents Convert from throwing an exception when the output buffer is empty
				return encoder.GetByteCount(charsPtr, chars.Length, flush);
			}
		}
	}
#endif

	/// <summary>
	/// Loads a RSA key from a PEM string.
	/// </summary>
#if NET5_0_OR_GREATER
	public static void LoadRsaParameters(string key, RSA rsa)
#else
	public static RSAParameters GetRsaParameters(string key)
#endif
	{
#if NET5_0_OR_GREATER
		if (!PemEncoding.TryFind(key, out var pemFields))
			throw new FormatException(string.Concat("Unrecognized PEM data: ", key.AsSpan(0, Math.Min(key.Length, 80))));
		var isPrivate = key.AsSpan()[pemFields.Label].SequenceEqual("RSA PRIVATE KEY");
#else
		const string beginRsaPrivateKey = "-----BEGIN RSA PRIVATE KEY-----";
		const string endRsaPrivateKey = "-----END RSA PRIVATE KEY-----";
		const string beginPublicKey = "-----BEGIN PUBLIC KEY-----";
		const string endPublicKey = "-----END PUBLIC KEY-----";

		int keyStartIndex;
		string pemFooter;
		bool isPrivate;

		if ((keyStartIndex = key.IndexOf(beginRsaPrivateKey, StringComparison.Ordinal)) > -1)
		{
			keyStartIndex += beginRsaPrivateKey.Length;
			pemFooter = endRsaPrivateKey;
			isPrivate = true;
		}
		else if ((keyStartIndex = key.IndexOf(beginPublicKey, StringComparison.Ordinal)) > -1)
		{
			keyStartIndex += beginPublicKey.Length;
			pemFooter = endPublicKey;
			isPrivate = false;
		}
		else
		{
#if NETCOREAPP3_0_OR_GREATER
			throw new FormatException(string.Concat("Unrecognized PEM header: ", key.AsSpan(0, Math.Min(key.Length, 80))));
#else
			throw new FormatException("Unrecognized PEM header: " + key[..Math.Min(key.Length, 80)]);
#endif
		}

		var keyEndIndex = key.IndexOf(pemFooter, keyStartIndex, StringComparison.Ordinal);

		if (keyEndIndex <= -1)
#if NETCOREAPP3_0_OR_GREATER
			throw new FormatException(string.Concat("Missing expected '", pemFooter, "' PEM footer: ", key.AsSpan(Math.Max(key.Length - 80, 0))));
#else
			throw new FormatException($"Missing expected '{pemFooter}' PEM footer: " + key[Math.Max(key.Length - 80, 0)..]);
#endif
#endif

#if NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER
#if NET5_0_OR_GREATER
		var keyChars = key.AsSpan()[pemFields.Base64Data];
#else
		var keyChars = key.AsSpan()[keyStartIndex..keyEndIndex];
#endif
		var bufferLength = keyChars.Length / 4 * 3;
		byte[]? buffer = null;
		Span<byte> bufferBytes = bufferLength > 1024 ?
			(Span<byte>) (buffer = ArrayPool<byte>.Shared.Rent(bufferLength)) :
			stackalloc byte[bufferLength];
		try
		{
			if (!System.Convert.TryFromBase64Chars(keyChars, bufferBytes, out var bytesWritten))
				throw new FormatException("The input is not a valid Base-64 string.");
#if NET5_0_OR_GREATER
			if (isPrivate)
				rsa.ImportRSAPrivateKey(bufferBytes[..bytesWritten], out var _);
			else
				rsa.ImportSubjectPublicKeyInfo(bufferBytes[..bytesWritten], out var _);
#else
			return GetRsaParameters(bufferBytes[..bytesWritten], isPrivate);
#endif
		}
		finally
		{
			if (buffer is not null)
				ArrayPool<byte>.Shared.Return(buffer);
		}
#else
		key = key[keyStartIndex..keyEndIndex];
		return GetRsaParameters(System.Convert.FromBase64String(key), isPrivate);
#endif
	}

#if !NET5_0_OR_GREATER
	// Derived from: https://stackoverflow.com/a/32243171/, https://stackoverflow.com/a/26978561/, http://luca.ntop.org/Teaching/Appunti/asn1.html
	private static RSAParameters GetRsaParameters(ReadOnlySpan<byte> data, bool isPrivate)
	{
		// read header (30 81 xx, or 30 82 xx xx)
		if (data[0] != 0x30)
			throw new FormatException($"Expected 0x30 but read 0x{data[0]:X2}");
		data = data[1..];

		if (!TryReadAsnLength(data, out var length, out var bytesConsumed))
			throw new FormatException("Couldn't read key length");
		data = data[bytesConsumed..];

		if (!isPrivate)
		{
			// encoded OID sequence for  PKCS #1 rsaEncryption szOID_RSA_RSA = "1.2.840.113549.1.1.1"
			ReadOnlySpan<byte> rsaOid = [0x30, 0x0D, 0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01, 0x05, 0x00];
			if (!data[..rsaOid.Length].SequenceEqual(rsaOid))
				throw new FormatException($"Expected RSA OID but read {BitConverter.ToString(data[..15].ToArray())}");
			data = data[rsaOid.Length..];

			// BIT STRING (0x03) followed by length
			if (data[0] != 0x03)
				throw new FormatException($"Expected 0x03 but read 0x{data[0]:X2}");
			data = data[1..];

			if (!TryReadAsnLength(data, out length, out bytesConsumed))
				throw new FormatException("Couldn't read length");
			data = data[bytesConsumed..];

			// skip NULL byte
			if (data[0] != 0x00)
				throw new FormatException($"Expected 0x00 but read 0x{data[0]:X2}");
			data = data[1..];

			// skip next header (30 81 xx, or 30 82 xx xx)
			if (data[0] != 0x30)
				throw new FormatException($"Expected 0x30 but read 0x{data[0]:X2}");
			data = data[1..];

			if (!TryReadAsnLength(data, out length, out bytesConsumed))
				throw new FormatException("Couldn't read length");
			data = data[bytesConsumed..];
		}
		else
		{
			if (!TryReadAsnInteger(data, out var zero, out bytesConsumed) || zero.Length != 1 || zero[0] != 0)
				throw new FormatException("Couldn't read zero.");
			data = data[bytesConsumed..];
		}

		if (!TryReadAsnInteger(data, out var modulus, out bytesConsumed))
			throw new FormatException("Couldn't read modulus");
		data = data[bytesConsumed..];

		if (!TryReadAsnInteger(data, out var exponent, out bytesConsumed))
			throw new FormatException("Couldn't read exponent");
		data = data[bytesConsumed..];

		if (!isPrivate)
		{
			return new RSAParameters
			{
				Modulus = modulus.ToArray(),
				Exponent = exponent.ToArray(),
			};
		}

		if (!TryReadAsnInteger(data, out var d, out bytesConsumed))
			throw new FormatException("Couldn't read D");
		data = data[bytesConsumed..];

		if (!TryReadAsnInteger(data, out var p, out bytesConsumed))
			throw new FormatException("Couldn't read P");
		data = data[bytesConsumed..];

		if (!TryReadAsnInteger(data, out var q, out bytesConsumed))
			throw new FormatException("Couldn't read Q");
		data = data[bytesConsumed..];

		if (!TryReadAsnInteger(data, out var dp, out bytesConsumed))
			throw new FormatException("Couldn't read DP");
		data = data[bytesConsumed..];

		if (!TryReadAsnInteger(data, out var dq, out bytesConsumed))
			throw new FormatException("Couldn't read DQ");
		data = data[bytesConsumed..];

		if (!TryReadAsnInteger(data, out var iq, out bytesConsumed))
			throw new FormatException("Couldn't read IQ");
		data = data[bytesConsumed..];

		return new RSAParameters
		{
			Modulus = modulus.ToArray(),
			Exponent = exponent.ToArray(),
			D = d.ToArray(),
			P = p.ToArray(),
			Q = q.ToArray(),
			DP = dp.ToArray(),
			DQ = dq.ToArray(),
			InverseQ = iq.ToArray(),
		};
	}
#endif

#if !NETSTANDARD2_1_OR_GREATER && !NETCOREAPP2_0_OR_GREATER
	/// <param name="arraySegment">The <see cref="ArraySegment{T}"/> from which to create a slice.</param>
	extension<T>(ArraySegment<T> arraySegment)
	{
		/// <summary>
		/// Returns a new <see cref="ArraySegment{T}"/> that starts at index <paramref name="index"/> into <paramref name="arraySegment"/>.
		/// </summary>
		/// <param name="index">The non-negative, zero-based starting index of the new slice (relative to <see cref="ArraySegment{T}.Offset"/> of <paramref name="arraySegment"/>.</param>
		/// <returns>A new <see cref="ArraySegment{T}"/> starting at the <paramref name="index"/>th element of <paramref name="arraySegment"/> and continuing to the end of <paramref name="arraySegment"/>.</returns>
		public ArraySegment<T> Slice(int index) =>
			new(arraySegment.Array!, arraySegment.Offset + index, arraySegment.Count - index);

		/// <summary>
		/// Returns a new <see cref="ArraySegment{T}"/> that starts at index <paramref name="index"/> into <paramref name="arraySegment"/> and has a length of <paramref name="length"/>.
		/// </summary>
		/// <param name="index">The non-negative, zero-based starting index of the new slice (relative to <see cref="ArraySegment{T}.Offset"/> of <paramref name="arraySegment"/>.</param>
		/// <param name="length">The non-negative length of the new slice.</param>
		/// <returns>A new <see cref="ArraySegment{T}"/> of length <paramref name="length"/>, starting at the <paramref name="index"/>th element of <paramref name="arraySegment"/>.</returns>
		public ArraySegment<T> Slice(int index, int length) =>
			new(arraySegment.Array!, arraySegment.Offset + index, length);
	}
#endif

#if !NET5_0_OR_GREATER
	/// <summary>
	/// Returns a new <see cref="byte"/> array that is a slice of <paramref name="input"/> starting at <paramref name="offset"/>.
	/// </summary>
	/// <param name="input">The array to slice.</param>
	/// <param name="offset">The offset at which to slice.</param>
	/// <param name="length">The length of the slice.</param>
	/// <returns>A new <see cref="byte"/> array that is a slice of <paramref name="input"/> from <paramref name="offset"/> to the end.</returns>
	public static byte[] ArraySlice(byte[] input, int offset, int length)
	{
		if (offset == 0 && length == input.Length)
			return input;
		var slice = new byte[length];
		Array.Copy(input, offset, slice, 0, slice.Length);
		return slice;
	}
#endif

	/// <summary>
	/// Finds the next index of <paramref name="pattern"/> in <paramref name="data"/>, starting at index <paramref name="offset"/>.
	/// </summary>
	/// <param name="data">The array to search.</param>
	/// <param name="offset">The offset at which to start searching.</param>
	/// <param name="pattern">The pattern to find in <paramref name="data"/>.</param>
	/// <returns>The offset of <paramref name="pattern"/> within <paramref name="data"/>, or <c>-1</c> if <paramref name="pattern"/> was not found.</returns>
	public static int FindNextIndex(ReadOnlySpan<byte> data, int offset, ReadOnlySpan<byte> pattern)
	{
		var index = MemoryExtensions.IndexOf(data[offset..], pattern);
		return index == -1 ? -1 : offset + index;
	}

	/// <summary>
	/// Resizes <paramref name="resizableArray"/> to hold at least <paramref name="newLength"/> items.
	/// </summary>
	/// <remarks><paramref name="resizableArray"/> may be <c>null</c>, in which case a new <see cref="ResizableArray{T}"/> will be allocated.</remarks>
	public static void Resize<T>([NotNull] ref ResizableArray<T>? resizableArray, int newLength)
		where T : notnull
	{
		resizableArray ??= new();
		resizableArray.DoResize(newLength);
	}

	public static bool TryParseRedirectionHeader(string redirectUrl, string initialUser, out string host, out int port, out string user)
	{
		host = "";
		port = 0;
		user = "";

		// "mariadb/mysql://[{user}[:{password}]@]{host}[:{port}]/[{db}[?{opt1}={value1}[&{opt2}={value2}]]]']"
		if (!redirectUrl.StartsWith("mysql://", StringComparison.Ordinal) && !redirectUrl.StartsWith("mariadb://", StringComparison.Ordinal))
			return false;

		try
		{
			var uri = new Uri(redirectUrl);
			host = uri.Host;
			if (string.IsNullOrEmpty(host)) return false;
			if (host.StartsWith('[') && host.EndsWith("]", StringComparison.Ordinal)) host = host.Substring(1, host.Length - 2);

			port = uri.Port;
			user = Uri.UnescapeDataString(uri.UserInfo.Split(':')[0]);
			if (string.IsNullOrEmpty(user) && !string.IsNullOrEmpty(uri.Query))
			{
				// query format "?{opt1}={value1}[&{opt2}={value2}]"
				var q = uri.Query.Substring(1);
				foreach (var token in q.Split('&'))
				{
					if (token.StartsWith("user=", StringComparison.Ordinal))
					{
						user = Uri.UnescapeDataString(token.Substring(5));
					}
				}
			}

			if (string.IsNullOrEmpty(user)) user = initialUser;
			return true;
		}
		catch (UriFormatException)
		{
			return false;
		}
	}

	public static TimeSpan ParseTimeSpan(ReadOnlySpan<byte> value)
	{
		var originalValue = value;

		// parse (optional) leading minus sign
		var isNegative = false;
		if (value is [0x2D, ..])
		{
			isNegative = true;
			value = value[1..];
		}

		// parse hours (0-838)
		if (!Utf8Parser.TryParse(value, out int hours, out var bytesConsumed) || hours < 0 || hours > 838)
			goto InvalidTimeSpan;
		if (value.Length == bytesConsumed || value[bytesConsumed] != 58)
			goto InvalidTimeSpan;
		value = value[(bytesConsumed + 1)..];

		// parse minutes (0-59)
		if (!Utf8Parser.TryParse(value, out int minutes, out bytesConsumed) || bytesConsumed != 2 || minutes < 0 || minutes > 59)
			goto InvalidTimeSpan;
		if (value.Length < 3 || value[2] != 58)
			goto InvalidTimeSpan;
		value = value[3..];

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
			value = value[3..];
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
#if NET9_0_OR_GREATER
		return TimeSpan.FromHours(hours, minutes, seconds, microseconds: microseconds);
#elif NET7_0_OR_GREATER
		return new TimeSpan(0, hours, minutes, seconds, microseconds / 1000, microseconds % 1000);
#else
		return new TimeSpan(0, hours, minutes, seconds, microseconds / 1000) + TimeSpan.FromTicks(microseconds % 1000 * 10);
#endif

		InvalidTimeSpan:
		throw new FormatException($"Couldn't interpret value as a valid TimeSpan: {Encoding.UTF8.GetString(originalValue)}");
	}

#if !NETCOREAPP2_1_OR_GREATER && !NETSTANDARD2_1_OR_GREATER
	public static bool TryComputeHash(this HashAlgorithm hashAlgorithm, ReadOnlySpan<byte> source, Span<byte> destination, out int bytesWritten)
	{
		// assume caller supplies a large-enough buffer so we don't have to bounds-check it
		var output = hashAlgorithm.ComputeHash(source.ToArray());
		output.AsSpan().CopyTo(destination);
		bytesWritten = output.Length;
		return true;
	}
#endif

	public static byte[] TrimZeroByte(byte[] value)
	{
		if (value is [.., 0])
			Array.Resize(ref value, value.Length - 1);
		return value;
	}

	public static ReadOnlySpan<byte> TrimZeroByte(ReadOnlySpan<byte> value) =>
		value is [.., 0] ? value[..^1] : value;

	extension(Stream stream)
	{
#if !NETCOREAPP2_1_OR_GREATER && !NETSTANDARD2_1_OR_GREATER
		public int Read(Memory<byte> buffer)
		{
			_ = MemoryMarshal.TryGetArray<byte>(buffer, out var arraySegment);
			return stream.Read(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
		}

		public Task<int> ReadAsync(Memory<byte> buffer)
		{
			_ = MemoryMarshal.TryGetArray<byte>(buffer, out var arraySegment);
			return stream.ReadAsync(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
		}

		public void Write(ReadOnlyMemory<byte> data)
		{
			_ = MemoryMarshal.TryGetArray(data, out var arraySegment);
			stream.Write(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
		}

		public Task WriteAsync(ReadOnlyMemory<byte> data)
		{
			_ = MemoryMarshal.TryGetArray(data, out var arraySegment);
			return stream.WriteAsync(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
		}
#else
		public int Read(Memory<byte> buffer) => stream.Read(buffer.Span);

		public void Write(ReadOnlyMemory<byte> data) => stream.Write(data.Span);
#endif
	}

#if !NETCOREAPP2_0_OR_GREATER && !NETSTANDARD2_1_OR_GREATER
	public static bool StartsWith(this string str, char value) => !string.IsNullOrEmpty(str) && str[0] == value;
#endif

	public static void SwapBytes(Span<byte> bytes, int offset1, int offset2)
	{
#if NET8_0_OR_GREATER
		ref var first = ref Unsafe.AsRef(ref bytes[0]);
#else
		ref var first = ref Unsafe.AsRef(bytes[0]);
#endif
		(Unsafe.Add(ref first, offset2), Unsafe.Add(ref first, offset1)) = (Unsafe.Add(ref first, offset1), Unsafe.Add(ref first, offset2));
	}

#if NET462
	public static bool IsWindows() => Environment.OSVersion.Platform == PlatformID.Win32NT;

	public static void GetOSDetails(out string? os, out string osDescription, out string architecture)
	{
		os = Environment.OSVersion.Platform switch
		{
			PlatformID.Win32NT => "Windows",
			PlatformID.Unix => "Linux",
			PlatformID.MacOSX => "macOS",
			_ => null,
		};
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

	public static void GetOSDetails(out string? os, out string osDescription, out string architecture)
	{
		os = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Windows" :
			RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "Linux" :
			RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "macOS" : null;
		osDescription = RuntimeInformation.OSDescription;
		architecture = RuntimeInformation.ProcessArchitecture.ToString();
	}
#endif

	/// <summary>
	/// Gets the elapsed time (in milliseconds) since the specified <paramref name="startingTimestamp"/> (which must be a value returned from <see cref="Stopwatch.GetTimestamp"/>.
	/// </summary>
	public static int GetElapsedMilliseconds(long startingTimestamp) =>
#if NET7_0_OR_GREATER
		(int) Stopwatch.GetElapsedTime(startingTimestamp).TotalMilliseconds;
#else
		(int) ((Stopwatch.GetTimestamp() - startingTimestamp) * 1000L / Stopwatch.Frequency);
#endif

	/// <summary>
	/// Gets the elapsed time (in seconds) between the specified <paramref name="startingTimestamp"/> and <paramref name="endingTimestamp"/>. (These must be values returned from <see cref="Stopwatch.GetTimestamp"/>.)
	/// </summary>
	public static double GetElapsedSeconds(long startingTimestamp, long endingTimestamp) =>
#if NET7_0_OR_GREATER
		Stopwatch.GetElapsedTime(startingTimestamp, endingTimestamp).TotalSeconds;
#else
		(endingTimestamp - startingTimestamp) / (double) Stopwatch.Frequency;
#endif

#if NET462
	public static SslProtocols GetDefaultSslProtocols()
	{
		if (!s_defaultSslProtocols.HasValue)
		{
			// Prior to .NET Framework 4.7, SslProtocols.None is not a valid argument to SslStream.AuthenticateAsClientAsync.
			// If the NET462 build is loaded by an application that targets .NET 4.7 (or later), or if app.config has set
			// Switch.System.Net.DontEnableSystemDefaultTlsVersions to false, then SslProtocols.None will work; otherwise,
			// if the application targets .NET 4.6.2 or earlier and hasn't changed the AppContext switch, then it will
			// fail at runtime. We attempt to determine if it will fail by accessing the internal static
			// ServicePointManager.DisableSystemDefaultTlsVersions property, which controls whether SslProtocols.None is
			// an acceptable value.
			bool disableSystemDefaultTlsVersions;
			try
			{
				var property = typeof(ServicePointManager).GetProperty("DisableSystemDefaultTlsVersions", BindingFlags.NonPublic | BindingFlags.Static);
				disableSystemDefaultTlsVersions = property is null || property.GetValue(null) is true;
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

	private static SslProtocols? s_defaultSslProtocols;
#else
	public static SslProtocols GetDefaultSslProtocols() => SslProtocols.None;
#endif

	// Reads a length encoded according to ASN.1 BER rules.
	private static bool TryReadAsnLength(ReadOnlySpan<byte> data, out int length, out int bytesConsumed)
	{
		var leadByte = data[0];
		if (leadByte < 0x80)
		{
			// Short form. One octet. Bit 8 has value "0" and bits 7-1 give the length.
			length = leadByte;
			bytesConsumed = 1;
			return true;
		}

		// Long form. Two to 127 octets. Bit 8 of first octet has value "1" and bits 7-1 give the number of additional length octets. Second and following octets give the length, base 256, most significant digit first.
		if (leadByte == 0x81)
		{
			length = data[1];
			bytesConsumed = 2;
			return true;
		}

		if (leadByte == 0x82)
		{
			length = (data[1] * 256) + data[2];
			bytesConsumed = 3;
			return true;
		}

		// lengths over 2^16 are not currently handled
		length = 0;
		bytesConsumed = 0;
		return false;
	}

	private static bool TryReadAsnInteger(ReadOnlySpan<byte> data, out ReadOnlySpan<byte> number, out int bytesConsumed)
	{
		// integer tag is 2
		if (data is not [0x02, ..])
		{
			number = default;
			bytesConsumed = 0;
			return false;
		}
		data = data[1..];

		// tag is followed by the length of the integer
		if (!TryReadAsnLength(data, out var length, out var lengthBytesConsumed))
		{
			number = default;
			bytesConsumed = 0;
			return false;
		}

		// length is followed by the integer bytes, MSB first
		number = data.Slice(lengthBytesConsumed, length);
		bytesConsumed = lengthBytesConsumed + length + 1;

		// trim leading zero bytes
		while (number is [0, _, ..])
			number = number[1..];

		return true;
	}
}
