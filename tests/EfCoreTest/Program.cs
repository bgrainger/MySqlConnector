using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using NetTopologySuite.Geometries;

[assembly: DesignTimeServicesReference("MySqlConnector.EntityFrameworkCore.MySql.Design.Internal.MySqlConnectorNetTopologySuiteDesignTimeServices, MySqlConnector.EntityFrameworkCore.NetTopologySuite")]

namespace EfCoreTest
{
	class Program
	{
		static void Main(string[] args)
		{
			User u2 = new User
			{
				Username = "user1",
				FirstName = "Maureen",
				LastName = "Miles",
				Location = new Point(16.3738, 48.2082) { SRID = 4326 },
			};

			using (var ctx = new MyContext())
			{
				Point myLocation = new Point(13.4050, 52.5200)
				{
					SRID = 4326
				};

				double radiusMeters = 700000;
				User[] usersWithinRadius = ctx.Users.Where(x => x.Location.Distance(myLocation) <= radiusMeters).ToArray();

				foreach (User u in usersWithinRadius)
					Console.WriteLine($"{u.Username}\t{u.FirstName} {u.LastName}");
			}
		}
	}

	public class MyContext : DbContext
	{
		public DbSet<User> Users { get; set; }

		protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
		{
			optionsBuilder.UseMySql(
				"server=localhost;user id=mysqltest;password='test;key=\"val';port=3306;database=efcoretest;ssl mode=None;UseCompression=false;",
				x => x.UseNetTopologySuite());
		}

		protected override void OnModelCreating(ModelBuilder modelBuilder)
		{
			modelBuilder.Entity<User>().HasKey(x => x.Username);
		}
	}

	public class User
	{
		public string Username { get; set; }
		public string FirstName { get; set; }
		public string LastName { get; set; }
		public Point Location { get; set; }
	}
}
