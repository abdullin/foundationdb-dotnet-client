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
using FoundationDB.Client.Status;
using FoundationDB.Layers.Tuples;

namespace FoundationDB.EventStore
{
	class Program {


		static long AppendedEventCount = 0;

		static long ProjectedCount = 0;
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
			
			Console.WriteLine("Sleeping a little, to let the store clean and queues fill");
			await Task.Delay(5000, go.Token);
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

			//Console.WriteLine("Launching inbox processor");
			//tasks.Add(es.ProcessInboxForever(go.Token, args.InboxChunkSize));

			Console.WriteLine("Launching {0} projectors", args.Projectors);
			for (int i = 0; i < args.Projectors; i++) {
				tasks.Add(RunProjector(go.Token, es));
			}

			Table.PrintRow("Inserted","Speed", "dVer", "dProj");

		
	
			while (!go.IsCancellationRequested)
			{
				var eventCountBefore = Interlocked.Read(ref AppendedEventCount);
				await Task.Delay(10000, go.Token).ConfigureAwait(false);
				
				var events = Interlocked.Read(ref AppendedEventCount) - eventCountBefore;
				var storeVersion = await es.GetStoreVersion(go.Token).ConfigureAwait(false);


				//FdbSystemStatus status;
				//using (var tr = db.BeginReadOnlyTransaction(go.Token)) {
				//	tr.WithPrioritySystemImmediate();
				//	tr.WithReadAccessToSystemKeys();
				//	status = await Fdb.System.GetStatusAsync(tr);
				//}

				


				//double conflicts = double.NaN;
				//double commits = double.NaN;
				//long diskUsed = -1;
				//if (status != null) {
				//	conflicts = status.Cluster.Workload.Transactions.Conflicted.Hz;
				//	commits = status.Cluster.Workload.Transactions.Committed.Hz;
				//	diskUsed = status.Cluster.Data.TotalDiskUsedBytes;
				//}

				var elapsedSeconds = t1.ElapsedMilliseconds/1000;


				var deltaProj = (AppendedEventCount*args.Projectors - ProjectedCount) / args.Projectors;
				
				Table.PrintRow(
					AppendedEventCount,
					events / elapsedSeconds,
					AppendedEventCount - storeVersion,
					deltaProj
					);
				
				
				t1.Restart();
			}

			Task.WaitAll(tasks.ToArray());

			// single thread test
		}

		static async Task RunProjector(CancellationToken token, FdbAppendOnlyStore es) {
			long version = 0;

			
			while (!token.IsCancellationRequested) {
				var handled = 0;
				await es.ReadStore(token, version, 100, (stream, l) => {
					version = l;
					handled += 1;
				}).ConfigureAwait(false);

				if (handled > 0) {
					Interlocked.Add(ref ProjectedCount, handled);
				} else {
					await Task.Delay(250, token).ConfigureAwait(false);
				}
			}
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
						await es.ReadStream(token, s, (stream, l) => { ver += 1; }, 0, int.MaxValue)
							.ConfigureAwait(false);
						await es.Append(token, s, data, ver)
							.ConfigureAwait(false);
						Interlocked.Increment(ref AppendedEventCount);
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
								await Task.Delay(500, token)
									.ConfigureAwait(false);
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

		[Option('p', "projectors", DefaultValue = 1, HelpText = "Number of projectors to run")]
		public int Projectors { get; set; }

		[Option('c', "chunks", DefaultValue = 5000, HelpText = "Inbox Chunk Size")]
		public int InboxChunkSize { get; set; }

		[ParserState]
		public IParserState LastParserState { get; set; }

		[HelpOption]
		public string GetUsage()
		{
			return HelpText.AutoBuild(this,
			  (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
		}
	}


	public static class Table {
		static int tableWidth = 77;

		public static void PrintLine()
		{
			Console.WriteLine(new string('-', tableWidth));
		}

		public static void PrintRow(params object[] columns)
		{
			int width = (tableWidth - columns.Length) / columns.Length;
			string row = "|";

			foreach (var column in columns)
			{
				row += AlignCentre(column, width) + "|";
			}

			Console.WriteLine(row);
		}

		static string AlignCentre(object s, int width) {
			var text = s.ToString();
			text = text.Length > width ? text.Substring(0, width - 3) + "..." : text;

			if (string.IsNullOrEmpty(text))
			{
				return new string(' ', width);
			}
			else
			{
				return text.PadLeft(width);
			}
		}
	}

}
