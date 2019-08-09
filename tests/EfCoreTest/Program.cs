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
			MySqlConnector.Logging.MySqlConnectorLogManager.Provider = new MySqlConnector.Logging.ConsoleLoggerProvider(MySqlConnector.Logging.MySqlConnectorLogLevel.Debug, true);

			using (var ctx = new MyContext())
			{
				ctx.Users.Add(new User { Username = "amsterdam", FirstName = "Elvis", LastName = "Yoxall", Location = new Point(4.899431, 52.379189) { SRID = 4326 } });
				ctx.Users.Add(new User { Username = "belgrade", FirstName = "Cory", LastName = "Howard", Location = new Point(20.457273, 44.787197) { SRID = 4326 } });
				ctx.Users.Add(new User { Username = "bern", FirstName = "Garnette", LastName = "Graves", Location = new Point(7.44744, 46.94809) { SRID = 4326 } });
				ctx.Users.Add(new User { Username = "brussels", FirstName = "Winslow", LastName = "Wescott", Location = new Point(4.34878, 50.85045) { SRID = 4326 } });
				ctx.Users.Add(new User { Username = "budapest", FirstName = "Dex", LastName = "Hamilton", Location = new Point(19.040236, 47.497913) { SRID = 4326 } });
				ctx.Users.Add(new User { Username = "london", FirstName = "Miracle", LastName = "Beck", Location = new Point(-0.118092, 51.509865) { SRID = 4326 } });
				ctx.Users.Add(new User { Username = "prague", FirstName = "Scotty", LastName = "Neil", Location = new Point(14.418540, 50.073658) { SRID = 4326 } });
				ctx.Users.Add(new User { Username = "rome", FirstName = "Esme", LastName = "Leonard", Location = new Point(12.496366, 41.902782) { SRID = 4326 } });
				ctx.Users.Add(new User { Username = "vienna", FirstName = "Maureen", LastName = "Miles", Location = new Point(16.363449, 48.210033) { SRID = 4326 } });
				ctx.Users.Add(new User { Username = "warsaw", FirstName = "Seraphina", LastName = "Rodgers", Location = new Point(21.017532, 52.237049) { SRID = 4326 } });
				ctx.SaveChanges();
				/*Point myLocation = new Point(13.4050, 52.5200)
				{
					SRID = 4326
				};

				double radiusMeters = 700000;
				User[] usersWithinRadius = ctx.Users.Where(x => x.Location.Distance(myLocation) <= radiusMeters).ToArray();

				foreach (User u in usersWithinRadius)
					Console.WriteLine($"{u.Username}\t{u.FirstName} {u.LastName}");*/
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
