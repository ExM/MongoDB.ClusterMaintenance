using System;
using System.Text;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using NUnit.Framework;

namespace MongoDB.ClusterMaintenance
{
	[TestFixture]
	public class ProgressRendererTests
	{
		[Test]
		public void Demo()
		{
		
		
			Console.WriteLine(Console.IsOutputRedirected);
			
			
			Console.Write("123");

			//Console.CursorLeft = p;
			
			Console.Write("321");

			Console.Write("\b");

		}
	}
}