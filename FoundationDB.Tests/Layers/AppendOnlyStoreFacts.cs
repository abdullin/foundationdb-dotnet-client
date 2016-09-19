using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FoundationDB.Client;
using FoundationDB.Client.Tests;
using FoundationDB.Layers.Counters;
using NUnit.Framework;

namespace FoundationDB.EventStore {

	public sealed class AppendOnlyStoreFacts : FdbTest {


		private const int DataFileCount = 10;
		private const int FileMessagesCount = 5;






		[Test]
		public async Task when_append_and_read()
		{

			using (var db = await OpenTestPartitionAsync())
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message1"));
				await appendOnly.Append(Cancellation, "stream2", Encoding.UTF8.GetBytes("test message2"));
				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message3"));

				


				var recordsSteam1 = await appendOnly.ReadStream(Cancellation,  "stream1");
				var recordsSteam2 = await appendOnly.ReadStream(Cancellation, "stream2");

				Assert.AreEqual(2, recordsSteam1.Count);
				Assert.AreEqual(1, recordsSteam2.Count);
				Assert.AreEqual("test message1", Encoding.UTF8.GetString(recordsSteam1[0].Data));
				Assert.AreEqual("test message3", Encoding.UTF8.GetString(recordsSteam1[1].Data));
				Assert.AreEqual("test message2", Encoding.UTF8.GetString(recordsSteam2[0].Data));
			}



		}

		[Test]
		public async Task when_read_after_version()
		{
			using (var db = await OpenTestPartitionAsync())
			{
				var location = GetCleanDirectory(db,
					"es",
					"simple").Result;


				var appendOnly = new FdbAppendOnlyStore(db, location);

				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message1"));
				await appendOnly.Append(Cancellation, "stream2", Encoding.UTF8.GetBytes("test message2"));
				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message3"));

				var recordsSteam1 = await appendOnly.ReadStream(Cancellation, "stream1", 1);

				Assert.AreEqual(1, recordsSteam1.Count);
				Assert.AreEqual("test message3", Encoding.UTF8.GetString(recordsSteam1[0].Data));
			}
		}

		[Test]
		public async Task when_read_than_set_max_records()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var location = GetCleanDirectory(db,
					"es",
					"simple").Result;


				var appendOnly = new FdbAppendOnlyStore(db, location);

				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message1"));
				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message2"));
				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message3"));

				var recordsSteam1 = await appendOnly.ReadStream(Cancellation, "stream1", 0, 2);

				Assert.AreEqual(2, recordsSteam1.Count);
				Assert.AreEqual("test message1", Encoding.UTF8.GetString(recordsSteam1[0].Data));
				Assert.AreEqual("test message2", Encoding.UTF8.GetString(recordsSteam1[1].Data));
			}
		}

		[Test]
		public async Task when_reads_store()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var location = GetCleanDirectory(db,
					"es",
					"simple").Result;


				var appendOnly = new FdbAppendOnlyStore(db, location);

				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message1"));
				await appendOnly.Append(Cancellation, "stream2", Encoding.UTF8.GetBytes("test message2"));
				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message3"));

				//await appendOnly.ProcessInbox(Cancellation);

				var recordsSteam = await  appendOnly.ReadStore(Cancellation,  0, Int32.MaxValue);

				Assert.AreEqual(3, recordsSteam.Count);
				Assert.AreEqual(1, recordsSteam[0].StoreVersion);
				Assert.AreEqual(2, recordsSteam[1].StoreVersion);
				Assert.AreEqual(3, recordsSteam[2].StoreVersion);

				Assert.AreEqual("test message1", Encoding.UTF8.GetString(recordsSteam[0].Data));
				Assert.AreEqual("test message2", Encoding.UTF8.GetString(recordsSteam[1].Data));
				Assert.AreEqual("test message3", Encoding.UTF8.GetString(recordsSteam[2].Data));
			}
		}

		[Test]
		[ExpectedException(typeof(AppendOnlyStoreConcurrencyException))]
		public async Task append_data_when_set_version_where_does_not_correspond_real_version()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message1"), 100);
			}
		}

		[Test]
		public async Task get_current_version()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message1"));
				await appendOnly.Append(Cancellation, "stream2", Encoding.UTF8.GetBytes("test message2"));
				await appendOnly.Append(Cancellation, "stream1", Encoding.UTF8.GetBytes("test message3"));

				Assert.AreEqual(0, await appendOnly.GetStoreVersion(Cancellation));

				await appendOnly.ProcessInbox(Cancellation);
				Assert.AreEqual(3, await appendOnly.GetStoreVersion(Cancellation));
			}
		}

		private async Task CreateCacheFiles(IFdbDatabase db, IFdbSubspace space)
		{

			var appendOnly = new FdbAppendOnlyStore(db, space);


			const string msg = "test messages";
			for (var index = 0; index < DataFileCount; index++)
			{
				for (var i = 0; i < FileMessagesCount; i++)
				{
					await appendOnly.Append(Cancellation, "test-key" + index, Encoding.UTF8.GetBytes(msg + i));
				}
			}

		}

		[Test]
		public async Task load_cache()
		{


			using (var db = OpenTestPartitionAsync().Result)
			{
				var subspace = GetCleanDirectory(db, "es", "simple").Result;
				await this.CreateCacheFiles(db, subspace);

				var appendOnly = new FdbAppendOnlyStore(db, subspace);

				for (var j = 0; j < DataFileCount; j++)
				{
					var key = "test-key" + j;
					var data = await appendOnly.ReadStream(Cancellation,  key);
					Assert.AreEqual(FileMessagesCount, data.Count);
					var i = 0;
					foreach (var dataWithKey in data)
					{
						Assert.AreEqual("test messages" + i, Encoding.UTF8.GetString(dataWithKey.Data));
						i++;
					}
				}
			}
		}

		[Test]
		public async Task when_reset_store()
		{
			using (var db = await OpenTestPartitionAsync())
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				var stream = Guid.NewGuid().ToString();

				for (var i = 0; i < 10; i++)
				{
					await appendOnly.Append(this.Cancellation, stream, Encoding.UTF8.GetBytes("test message" + i));
				}

				//await appendOnly.ProcessInbox(Cancellation);
				var version = await appendOnly.GetStoreVersion(this.Cancellation);
				Assert.GreaterOrEqual(10, version);

				await appendOnly.ResetStore(Cancellation);
				var versionAfterReset = await appendOnly.GetStoreVersion(Cancellation);
				Assert.AreEqual(0, versionAfterReset);
			}
		}

		[Test]
		public async Task when_append_after_reset_store()
		{
			using (var db = await OpenTestPartitionAsync())
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				var stream = Guid.NewGuid().ToString();

				for (var i = 0; i < 10; i++)
				{
					await appendOnly.Append(Cancellation, stream, Encoding.UTF8.GetBytes("test message" + i));
				}
				await appendOnly.ResetStore(Cancellation);
				for (var i = 0; i < 10; i++)
				{
					await appendOnly.Append(Cancellation, stream, Encoding.UTF8.GetBytes("test message" + i));
				}
				//await appendOnly.ProcessInbox(Cancellation);
				var version = await appendOnly.GetStoreVersion(Cancellation);

				Assert.GreaterOrEqual(10, version);
			}
		}

		[Test]
		public async Task When_having_version_overflow()
		{
			using (var db = await OpenTestPartitionAsync())
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				var versions = new List<long>();
				for (int i = 0; i < 11; i++)
				{
					var version = Convert.ToInt64(Math.Pow(10L, i));
					versions.Add(version);

					await appendOnly.Append(Cancellation, "all", BitConverter.GetBytes(version));

				}
				//await appendOnly.ProcessInbox(Cancellation);
				// recreate

				var stores =( await appendOnly.ReadStore(Cancellation, 0, int.MaxValue))
					.Select(d => BitConverter.ToInt64(d.Data, 0));
				CollectionAssert.AreEqual(versions, stores);
			}

		}
		[Test]
		public async Task Test_FdbCounter_Can_Increment_And_SetTotal() {
			using (var db = await OpenTestPartitionAsync()) {
				var location = await GetCleanDirectory(db, 
					"es", 
					"simple");

				

				var store = new FdbAppendOnlyStore(db, location);

				var data = Guid.NewGuid().ToByteArray();
				var result = await store.Append(Cancellation, "test", data);
				Assert.That(result, Is.EqualTo(1));
				

				await store.ProcessInbox(Cancellation);
				Assert.That(await store.GetStoreVersion(Cancellation), Is.EqualTo(1));
			}
		}



		[Test]
		public async Task Test_EventStore_Reads_Version_After_Reopen()
		{
			using (var db = await OpenTestPartitionAsync())
			{
				var location = await GetCleanDirectory(db,
					"es",
					"simple");

				var store = new FdbAppendOnlyStore(db, location);
				await  store.Append(Cancellation, "test", new byte[10]);
				await store.ProcessInbox(Cancellation);
				//await DumpSubspace(db, location);
				var store2 = new FdbAppendOnlyStore(db, location);
				
				Assert.That(await store2.GetStoreVersion(Cancellation), Is.EqualTo(1));
			}
		}


		[Test]
		public async Task Test_EventStore_Appends_Large_file()
		{
			var rnd = new Random(2403);

			using (var db = await OpenTestPartitionAsync())
			{
				var location = await GetCleanDirectory(db,
					"es",
					"simple");

				var store = new FdbAppendOnlyStore(db, location);
				var slice = Slice.Random(rnd, FdbAppendOnlyStore.MaxBlobSize + rnd.Next(200));

				var bytes = slice.GetBytes();

				await store.Append(Cancellation, "test", bytes);
				var stored = new List<Tuple<long,byte[]>>();
				
				 await store
					.ReadStream(Cancellation,  "test",(stream, l) => stored.Add(Tuple.Create(l, stream.ToArray())), 0, 10000)
					.ConfigureAwait(false);


				Assert.That(stored.Count, Is.EqualTo(1));
				Assert.That(() => stored[0].Item2, Is.EquivalentTo(bytes));
				Assert.That(() => stored[0].Item1, Is.EqualTo(1));
				
			}
		}

	}

}