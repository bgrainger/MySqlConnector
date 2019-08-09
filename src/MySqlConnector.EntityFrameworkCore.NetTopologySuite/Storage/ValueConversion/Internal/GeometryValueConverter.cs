using System;
using System.IO;
using GeoAPI.Geometries;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using NetTopologySuite.IO;

namespace MySqlConnector.EntityFrameworkCore.MySql.Storage.ValueConversion.Internal
{
	/// <summary>
	///     This API supports the Entity Framework Core infrastructure and is not intended to be used
	///     directly from your code. This API may change or be removed in future releases.
	/// </summary>
	public class GeometryValueConverter<TGeometry> : ValueConverter<TGeometry, byte[]>
		where TGeometry : IGeometry
	{
		/// <summary>
		///     This API supports the Entity Framework Core infrastructure and is not intended to be used
		///     directly from your code. This API may change or be removed in future releases.
		/// </summary>
		public GeometryValueConverter(WKBReader reader, WKBWriter writer)
			: base(g => Write(writer, g), b => Read(reader, b))
		{
		}

		private static byte[] Write(WKBWriter writer, TGeometry geometry)
		{
			using (var memoryStream = new MemoryStream())
			{
				memoryStream.Write(BitConverter.GetBytes(geometry.SRID), 0, 4);
				writer.Write(geometry, memoryStream);
				return memoryStream.ToArray();
			}
		}

		private static TGeometry Read(WKBReader reader, byte[] value)
		{
			var srid = BitConverter.ToInt32(value, 0);
			using (var memoryStream = new MemoryStream(value, 4, value.Length - 4))
			{
				var geometry = (TGeometry) reader.Read(memoryStream);
				geometry.SRID = srid;
				return geometry;
			}
		}
	}
}
