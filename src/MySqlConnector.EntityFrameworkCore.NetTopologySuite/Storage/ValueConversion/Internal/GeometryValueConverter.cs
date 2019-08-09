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
            : base(
                g => Write(writer, g),
                b => (TGeometry)reader.Read(b))
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
    }
}
