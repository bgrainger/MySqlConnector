#nullable enable

namespace MySqlConnector.Tests.Metrics;

#if !NETSTANDARD2_1 && !NETCOREAPP3_1_OR_GREATER
internal static class DictionaryExtensions
{
	public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key)
		where TValue : struct =>
		dictionary.TryGetValue(key, out var value) ? value : default;
}
#endif
