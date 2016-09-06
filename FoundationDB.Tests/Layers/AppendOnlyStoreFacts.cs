using System;
using System.Collections.Generic;
using System.Linq;
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
		public void when_append_and_read()
		{

			using (var db = OpenTestPartitionAsync().Result)
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message1"));
				appendOnly.Append("stream2", Encoding.UTF8.GetBytes("test message2"));
				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message3"));

				var recordsSteam1 = appendOnly.ReadRecords("stream1", 0, Int32.MaxValue).ToArray();
				var recordsSteam2 = appendOnly.ReadRecords("stream2", 0, Int32.MaxValue).ToArray();

				Assert.AreEqual(2, recordsSteam1.Length);
				Assert.AreEqual(1, recordsSteam2.Length);
				Assert.AreEqual("test message1", Encoding.UTF8.GetString(recordsSteam1[0].Data));
				Assert.AreEqual("test message3", Encoding.UTF8.GetString(recordsSteam1[1].Data));
				Assert.AreEqual("test message2", Encoding.UTF8.GetString(recordsSteam2[0].Data));
			}



		}

		[Test]
		public void when_read_after_version()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var location = GetCleanDirectory(db,
					"es",
					"simple").Result;


				var appendOnly = new FdbAppendOnlyStore(db, location);

				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message1"));
				appendOnly.Append("stream2", Encoding.UTF8.GetBytes("test message2"));
				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message3"));

				var recordsSteam1 = appendOnly.ReadRecords("stream1", 1, Int32.MaxValue).ToArray();

				Assert.AreEqual(1, recordsSteam1.Length);
				Assert.AreEqual("test message3", Encoding.UTF8.GetString(recordsSteam1[0].Data));
			}
		}

		[Test]
		public void when_read_than_set_max_records()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var location = GetCleanDirectory(db,
					"es",
					"simple").Result;


				var appendOnly = new FdbAppendOnlyStore(db, location);

				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message1"));
				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message2"));
				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message3"));

				var recordsSteam1 = appendOnly.ReadRecords("stream1", 0, 2).ToArray();

				Assert.AreEqual(2, recordsSteam1.Length);
				Assert.AreEqual("test message1", Encoding.UTF8.GetString(recordsSteam1[0].Data));
				Assert.AreEqual("test message2", Encoding.UTF8.GetString(recordsSteam1[1].Data));
			}
		}

		[Test]
		public void when_reads_record()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var location = GetCleanDirectory(db,
					"es",
					"simple").Result;


				var appendOnly = new FdbAppendOnlyStore(db, location);

				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message1"));
				appendOnly.Append("stream2", Encoding.UTF8.GetBytes("test message2"));
				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message3"));

				var recordsSteam = appendOnly.ReadRecords(0, Int32.MaxValue).ToArray();

				Assert.AreEqual(3, recordsSteam.Length);
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
		public void append_data_when_set_version_where_does_not_correspond_real_version()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message1"), 100);
			}
		}

		[Test]
		public void get_current_version()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message1"));
				appendOnly.Append("stream2", Encoding.UTF8.GetBytes("test message2"));
				appendOnly.Append("stream1", Encoding.UTF8.GetBytes("test message3"));

				Assert.AreEqual(3, appendOnly.GetCurrentVersion());
			}
		}

		private void CreateCacheFiles(IFdbDatabase db, IFdbSubspace space)
		{

			var appendOnly = new FdbAppendOnlyStore(db, space);


			const string msg = "test messages";
			for (var index = 0; index < DataFileCount; index++)
			{
				for (var i = 0; i < FileMessagesCount; i++)
				{
					appendOnly.Append("test-key" + index, Encoding.UTF8.GetBytes(msg + i));
				}
			}

		}

		[Test]
		public void load_cache()
		{


			using (var db = OpenTestPartitionAsync().Result)
			{
				var subspace = GetCleanDirectory(db, "es", "simple").Result;
				this.CreateCacheFiles(db, subspace);

				var appendOnly = new FdbAppendOnlyStore(db, subspace);

				for (var j = 0; j < DataFileCount; j++)
				{
					var key = "test-key" + j;
					var data = appendOnly.ReadRecords(key, 0, Int32.MaxValue).ToArray();
					Assert.AreEqual(FileMessagesCount, data.Length);
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
		public void when_reset_store()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				var stream = Guid.NewGuid().ToString();

				for (var i = 0; i < 10; i++)
				{
					appendOnly.Append(stream, Encoding.UTF8.GetBytes("test message" + i));
				}

				var version = appendOnly.GetCurrentVersion();
				appendOnly.ResetStore();
				var versionAfterReset = appendOnly.GetCurrentVersion();

				Assert.GreaterOrEqual(10, version);
				Assert.AreEqual(0, versionAfterReset);
			}
		}

		[Test]
		public void when_append_after_reset_store()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				var stream = Guid.NewGuid().ToString();

				for (var i = 0; i < 10; i++)
				{
					appendOnly.Append(stream, Encoding.UTF8.GetBytes("test message" + i));
				}
				appendOnly.ResetStore();
				for (var i = 0; i < 10; i++)
				{
					appendOnly.Append(stream, Encoding.UTF8.GetBytes("test message" + i));
				}

				var version = appendOnly.GetCurrentVersion();

				Assert.GreaterOrEqual(10, version);
			}
		}

		[Test]
		public void When_having_version_overflow()
		{
			using (var db = OpenTestPartitionAsync().Result)
			{
				var appendOnly = new FdbAppendOnlyStore(db, GetCleanDirectory(db, "es", "simple").Result);

				var versions = new List<long>();
				for (int i = 0; i < 11; i++)
				{
					var version = Convert.ToInt64(Math.Pow(10L, i));
					versions.Add(version);

					appendOnly.Append("all", BitConverter.GetBytes(version));

				}
				// recreate

				var stores = appendOnly.ReadRecords(0, int.MaxValue)
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
				var result = store.Append("test", data);
				Assert.That(result, Is.EqualTo(1));
				Assert.That(store.GetVersionSnapshot(), Is.EqualTo(1));
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
				 store.Append("test", new byte[10]);
				//await DumpSubspace(db, location);
				var store2 = new FdbAppendOnlyStore(db, location);
				Assert.That(store2.GetVersionSnapshot(), Is.EqualTo(1));
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

				store.Append("test", bytes);

				
				 var stored = store.ReadRecords("test",0,10000).ToList();


				Assert.That(stored.Count, Is.EqualTo(1));
				Assert.That(() => stored[0].Data, Is.EquivalentTo(bytes));
				Assert.That(() => stored[0].StreamVersion, Is.EqualTo(1));
				
			}
		}

	}

}