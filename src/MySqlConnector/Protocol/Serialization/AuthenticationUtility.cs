using System;
using System.Security.Cryptography;
using System.Text;

namespace MySqlConnector.Protocol.Serialization
{
	internal static class AuthenticationUtility
	{
		public static byte[] CreateAuthenticationResponse(byte[] challenge, int offset, string password) =>
			string.IsNullOrEmpty(password) ? s_emptyArray : HashPassword(challenge, offset, password);

		/// <summary>
		/// Hashes a password with the "Secure Password Authentication" method.
		/// </summary>
		/// <param name="challenge">The 20-byte random challenge (from the "auth-plugin-data" in the initial handshake).</param>
		/// <param name="offset">The offset of the start of the challenge within <paramref name="challenge"/>.</param>
		/// <param name="password">The password to hash.</param>
		/// <returns>A 20-byte password hash.</returns>
		/// <remarks>See <a href="https://dev.mysql.com/doc/internals/en/secure-password-authentication.html">Secure Password Authentication</a>.</remarks>
		public static byte[] HashPassword(byte[] challenge, int offset, string password)
		{
			using var sha1 = SHA1.Create();
			var combined = new byte[40];
			Buffer.BlockCopy(challenge, offset, combined, 0, 20);

			var passwordBytes = Encoding.UTF8.GetBytes(password);
			var hashedPassword = sha1.ComputeHash(passwordBytes);

			var doubleHashedPassword = sha1.ComputeHash(hashedPassword);
			Buffer.BlockCopy(doubleHashedPassword, 0, combined, 20, 20);

			var xorBytes = sha1.ComputeHash(combined);
			for (int i = 0; i < hashedPassword.Length; i++)
				hashedPassword[i] ^= xorBytes[i];

			return hashedPassword;
		}

		static readonly byte[] s_emptyArray = new byte[0];

		public static byte[] CreateScrambleResponse(byte[] nonce, string password)
		{
			var scrambleResponse = string.IsNullOrEmpty(password)
				? s_emptyArray
				: HashPasswordWithNonce(nonce, password);

			return scrambleResponse;
		}

		private static byte[] HashPasswordWithNonce(byte[] nonce, string password)
		{
			using var sha256 = SHA256.Create();
			var passwordBytes = Encoding.UTF8.GetBytes(password);
			var hashedPassword = sha256.ComputeHash(passwordBytes);

			var doubleHashedPassword = sha256.ComputeHash(hashedPassword);
			var combined = new byte[doubleHashedPassword.Length + nonce.Length];

			Buffer.BlockCopy(doubleHashedPassword, 0, combined, 0, doubleHashedPassword.Length);
			Buffer.BlockCopy(nonce, 0, combined, doubleHashedPassword.Length, nonce.Length);

			var xorBytes = sha256.ComputeHash(combined);
			for (int i = 0; i < hashedPassword.Length; i++)
				hashedPassword[i] ^= xorBytes[i];

			return hashedPassword;
		}
	}
}
