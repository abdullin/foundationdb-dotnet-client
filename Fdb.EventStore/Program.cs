using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using CommandLine.Text;
using FoundationDB.Client;
using FoundationDB.Layers.Tuples;

namespace FoundationDB.EventStore
{
	class Program {


		static long c1 = 0;
		static long c2 = 0;
		static void Main(string[] args) {

			var options = new Options();
			if (Parser.Default.ParseArguments(args, options))
			{
				Console.WriteLine("Running with:");
				Console.WriteLine("  Input file {0}", options.InputFile);
				Console.WriteLine("  Workers    {0}", options.Workers);

				MainAsync(options).Wait();
				
			}

			

		}

		static async Task MainAsync(Options args) {
			

			var go = new CancellationTokenSource();

			Console.WriteLine("Server Version {0}, Client will use {1}", Fdb.GetMaxApiVersion(), Fdb.GetMaxSafeApiVersion());
			// Initialize FDB
			//note: always use the latest version available
			Fdb.UseApiVersion(Fdb.GetMaxSafeApiVersion());
			Console.WriteLine("Starting network thread...");
			Fdb.Start();


			var db = await Fdb.OpenAsync(go.Token);
			var space = FdbSubspace.Create(FdbTuple.Create("bench", "es"));

			var es = new FdbAppendOnlyStore(db, space);

			Console.WriteLine("Clearing subspace");
			await es.ResetStore(go.Token);
			Console.WriteLine("Starting the test");
			
			var queues = new List< ConcurrentQueue<Tuple<string, byte[]>>>();
			var tasks = new List<Task>();

			for (int i = 0; i < args.Workers; i++) {
				var queue = new ConcurrentQueue<Tuple<string, byte[]>>();
				queues.Add(queue);
				var id = i;
				var reader = ReadUpdater(go.Token, s => {
					var hash = s.ToLowerInvariant().GetHashCode();
					return (hash%args.Workers) == id;
				} , args.InputFile, queue);
				tasks.Add(reader);
			}
			
			await Task.Delay(1000, go.Token);
			// spam commits on a single thread
			var t1 = Stopwatch.StartNew();
			var t2 = Stopwatch.StartNew();


			//var cg = 0;
			var tg = Stopwatch.StartNew();


			

			Console.WriteLine("Reading file {0}", args.InputFile);

			for (int i = 0; i < args.Workers; i++)
			{
				tasks.Add(RunApender(go.Token, queues[i], es));
			}

			while (!go.IsCancellationRequested)
			{

				var counter = Interlocked.Read(ref c1);
				await Task.Delay(10000, go.Token);
				
				var passed = Interlocked.Read(ref c1) - counter;

				var status = await Fdb.System.GetStatusAsync(db, go.Token);


				double conflicts = double.NaN;
				double commits = double.NaN;
				long diskUsed = -1;
				if (status != null) {
					conflicts = status.Cluster.Workload.Transactions.Conflicted.Hz;
					commits = status.Cluster.Workload.Transactions.Committed.Hz;
					diskUsed = status.Cluster.Data.TotalDiskUsedBytes;
				}

				var elapsedSeconds = t1.ElapsedMilliseconds/1000;
				
				
				
				Console.WriteLine("{0:########}\t{1:#########}\t{2:######}\t{3:######}\t{4:######}", 
					tg.ElapsedMilliseconds, 
					c1, 
					passed/elapsedSeconds,
					commits,
					conflicts);
				
				t1.Restart();
			}

			Task.WaitAll(tasks.ToArray());

			// single thread test
		}



		static async Task RunApender(
			CancellationToken token, 
			ConcurrentQueue<Tuple<string, byte[]>> queue, 
			FdbAppendOnlyStore es) {
			while (!token.IsCancellationRequested) {
				Tuple<string, byte[]> tuple;
				
				if (!queue.TryDequeue(out tuple)) {
					Console.WriteLine("Queue empty!");
				} else {
					try {
						var s = tuple.Item1;
						var data = tuple.Item2;
						int ver = 0;
						await es.ReadStream(token, s, (stream, l) => { ver += 1; }, 0, int.MaxValue);
						await es.Append(token, s, data, ver);
						Interlocked.Increment(ref c1);
						Interlocked.Increment(ref c2);
					}
					catch (Exception ex) {
						Console.WriteLine(ex);
					}
				}
			}
		}

		static async Task ReadUpdater(CancellationToken token, Predicate<string> check, string filename, ConcurrentQueue<Tuple<string, byte[]>> q) {


			using (var f = File.OpenRead(filename))
			{
				using (var zip = new GZipStream(f, CompressionMode.Decompress))
				{
					using (var bin = new BinaryReader(zip, new UTF8Encoding(false)))
					{
						while (!token.IsCancellationRequested)
						{
							var s = bin.ReadString();
							var len = bin.ReadInt32();
							var data = new byte[len];

							if (!check(s)) {
								continue;
							}


							while (q.Count >= 10000) {
								await Task.Delay(500, token);
							}

							q.Enqueue(Tuple.Create(s, data));
						}
					}
				}
			}
		}
	}


	class Options
	{
		[Option('r', "read", Required = true,
		  HelpText = "Input file to be processed.")]
		public string InputFile { get; set; }

		[Option('w', "workers", DefaultValue = 1,
		  HelpText = "Number of workers to run.")]
		public int Workers { get; set; }

		[ParserState]
		public IParserState LastParserState { get; set; }

		[HelpOption]
		public string GetUsage()
		{
			return HelpText.AutoBuild(this,
			  (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
		}
	}

}
