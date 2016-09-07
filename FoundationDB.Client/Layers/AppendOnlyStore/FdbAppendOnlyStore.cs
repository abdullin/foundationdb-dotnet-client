﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using FoundationDB.Async;
using FoundationDB.Client;
using FoundationDB.Layers.Tuples;

namespace FoundationDB.EventStore {

	/// <summary>
	///   <para>
	///     IAppendOnly store implementation that keeps all events in FoundationDB.
	///   </para>
	///   <para>
	///     Given some database prefix, it will store things:
	///     db|1|StreamName|Version|ChunkNum -> bytes
	///     db|0|Version|ChunkNum -> bytes
	///   </para>
	/// </summary>
	public class FdbAppendOnlyStore {
		readonly IFdbDatabase _db;
		readonly IFdbSubspace _subspace;
		readonly IFdbSubspace _aggSpace;
		readonly IFdbSubspace _globalSpace;

		readonly Slice _ageKey;


		public FdbAppendOnlyStore(IFdbDatabase db, IFdbSubspace subspace) {
			_db = db;
			_subspace = subspace;

			_aggSpace = subspace[FdbTuple.Create((byte) 1)];
			_globalSpace = subspace[FdbTuple.Create((byte) 2)];
			_ageKey = subspace[FdbTuple.Create((byte) 0)].ToFoundationDbKey();
		}

		async Task<long> GetLastEventVersion(IFdbReadOnlyTransaction tr, IFdbSubspace prefix) {
			var globalRange = FdbKeyRange.PrefixedBy(prefix);

			var slice = await tr.GetKeyAsync(FdbKeySelector.LastLessThan(globalRange.End))
				.ConfigureAwait(false);

			if (!prefix.Contains(slice)) {
				return 0;
			}

			var tuple = prefix.ExtractKey(slice).ToTuple();
			// we need element before the last one
			return tuple.Get<long>(0);
		}

		async Task<long> GetStoreAge(IFdbReadOnlyTransaction tr) {
			var result = await tr.GetAsync(_ageKey)
				.ConfigureAwait(false);

			if (result.IsNullOrEmpty) {
				return 0;
			}
			return result.ToInt64();
		}

		public const int MaxBlobSize = (1000*100);

		static void WriteBlob(IFdbTransaction tr, IFdbSubspace key, byte[] data) {
			if (data.Length == 0) {
				throw new ArgumentOutOfRangeException(nameof(data), "Byte array can't be empty");
			}

			// from here http://www.cs.nott.ac.uk/~psarb2/G51MPC/slides/NumberLogic.pdf
			var chunksTotal = (ushort) ((data.Length + MaxBlobSize - 1)/MaxBlobSize);


			var left = data.Length;
			var pos = 0;
			ushort chunkId = 0;
			while (left > 0) {
				//Console.WriteLine("Writing {0} of {1}", chunkId, chunksTotal);
				var uploadBytes = Math.Min(left, MaxBlobSize);
				var chunkKey = key.ConcatKey(FdbTuple.Create(chunkId, chunksTotal));
				tr.Set(chunkKey, Slice.Create(data, pos, uploadBytes, false));

				pos += uploadBytes;
				left -= uploadBytes;
				chunkId += 1;
			}
		}

		//static async MemoryStream ReadChunk(IFdbTransaction tr, IFdbSubspace key) {

		//}

		private static readonly Slice PlusOne = Slice.FromFixed64(1);

		public async Task<long> Append(CancellationToken token, string streamName, byte[] data,
			long expectedStreamVersion = -1) {
			var aggSpace = _aggSpace[FdbTuple.Create(streamName)];


			// we need to somehow insert time


			// store starts
			// reads generation (bucket).
			// starts writing to that bucket
			// in 100ms increments bucket
			// starts writing to that bucket


			// Solution: make sure that buckets are refreshed every second via shared counter

			// another option. Every second we increment the bucket number, otherwise we use

			//long newGlobalVersion = Interlocked.Increment(ref _version);


			using (var tr = _db.BeginTransaction(token)) {
				while (true) {
					token.ThrowIfCancellationRequested();
					try {
						// launch computation
						var storeAgeFuture = GetStoreAge(tr.Snapshot);

						//var globalVersion = tr.Snapshot.GetKeyAsync()

						var version = await GetLastEventVersion(tr, aggSpace).ConfigureAwait(false);

						if (expectedStreamVersion != -1) {
							if (version != expectedStreamVersion) {
								throw new AppendOnlyStoreConcurrencyException(expectedStreamVersion, version, streamName);
							}
						}


						var storeAge = await storeAgeFuture.ConfigureAwait(false);

						var nextStoreAge = storeAge + 1;
						var sid = Sid.CreateNew(nextStoreAge);

						var newStreamVersion = version + 1;
						// ReSharper disable RedundantTypeArgumentsOfMethod


						var versionedEventKey = aggSpace[FdbTuple.Create<long>(newStreamVersion)];


						// due to snapshot version of the global store version, we might have multiple
						// workers writing at the same point of time.
						var storeKey = _globalSpace[FdbTuple.Create<long, byte[]>(nextStoreAge, sid.GetBytes())];
						// ReSharper restore RedundantTypeArgumentsOfMethod

						// just dump copy.
						WriteBlob(tr, versionedEventKey, data);
						WriteBlob(tr, storeKey, data);

						tr.AtomicAdd(_ageKey, PlusOne);

						await tr.CommitAsync().ConfigureAwait(false);
						return newStreamVersion;
					}
					catch (FdbException ex) {
						await tr.OnErrorAsync(ex.Code).ConfigureAwait(false);
					}
				}
			}
		}

		//public long Append(string streamName, byte[] data, long expectedStreamVersion = -1) {
		//	var task = AppendAsync(CancellationToken.None, streamName, data, expectedStreamVersion);
		//	try {
		//		task.Wait();
		//		return task.Result;
		//	}
		//	catch (AggregateException ex) {
		//		if (ex.InnerException != null) {
		//			throw ex.InnerException;
		//		}
		//		throw;
		//	}
		//}


		public async Task ReadStream(CancellationToken token, string streamName,
			Action<MemoryStream, long> handler, long startingFrom = 0, int maxCount = int.MaxValue) {
			var aggSpace = _aggSpace[FdbTuple.Create(streamName)];
			await ReadDataInSpace(
				token,
				handler,
				startingFrom,
				maxCount,
				aggSpace);
		}

		public async Task<IList<StreamData>> ReadStream(CancellationToken token, string streamName,
			long startingFrom = 0, int maxCount = int.MaxValue) {
			// this will allocate a lot of RAM
			var list = new List<StreamData>();
			// that fits existing interface 
			await ReadStream(
					// lost cancellation support
					CancellationToken.None,
					streamName,
					// potentially allocating LOH
					(stream, l) => list.Add(new StreamData(stream.ToArray(), l)), startingFrom, maxCount)
				// blocking wait
				;

			return list;
		}

		public void Close() {
			// nothing to do here
		}

		public Task ResetStore(CancellationToken token) {
			return _db.WriteAsync(tr => { tr.ClearRange(_subspace); }, token
			);
		}

		public Task<long> GetCurrentVersion(CancellationToken token) {
			return _db.ReadAsync(GetStoreAge, token);
		}


		public async Task ReadStore(CancellationToken token, long startingFrom, int maxCount,
			Action<MemoryStream, long> handler) {
			var aggSpace = _globalSpace;
			await ReadDataInSpace(
				token,
				handler,
				startingFrom,
				maxCount,
				aggSpace);
		}

		public async Task<IList<StoreData>> ReadStore(CancellationToken token, long skip, int limit) {
			var list = new List<StoreData>();

			// Convert async version into thread-blocking memory-consuming version
			// that fits existing interface 
			await ReadStore(
				token,
				skip,
				limit,
				// we are potentially allocating LOH
				(stream, ver) => list.Add(new StoreData(stream.ToArray(), ver)));

			return list;
		}


		async Task ReadDataInSpace(CancellationToken token, Action<MemoryStream, long> handler, long skip,
			int limit,
			IFdbSubspace readSpace) {
			// events come with versions 1,2,3 (starting with 1).
			// To get all events from start, we can read from version 1
			// to skip 2 events and read from 3, we need to read 1 + skip.
			// to read only 10 events from 1, we need to read from 1 until 10.
			// to read 10 events from 3, we need to read from 3 to 13 (exclusive)


			// explicitly specify LONG
			// ReSharper disable RedundantTypeArgumentsOfMethod
			var startKey = FdbTuple.Create<long>(1 + skip);
			var endKey = FdbTuple.Create<long>(1 + skip + limit);
			// ReSharper restore RedundantTypeArgumentsOfMethod

			var range = FdbKeyRange.Create(
				readSpace.ConcatKey(startKey),
				readSpace.ConcatKey(endKey));

			var options = new FdbRangeOptions {
				Mode = FdbStreamingMode.WantAll
			};

			using (var stream = new MemoryStream()) {
				await Fdb.Bulk.ExportAsync(_db, range, (pairs, offset, ct) => {
					foreach (var pair in pairs) {
						// tuple will have: version|chunkid|chunkcount

						var tuple = readSpace.ExtractKey(pair.Key).ToTuple();

						var chunkId = tuple.Get<ushort>(1);
						var chunkCount = tuple.Get<ushort>(2);

						var buffer = pair.Value;
						stream.Write(buffer.Array, buffer.Offset, buffer.Count);
						//Console.WriteLine("Reading chunk {0} or {1}", chunkId, chunkCount);

						if (chunkId + 1 == chunkCount) {
							var version = tuple.Get<long>(0);
							handler(stream, version);
							stream.Seek(0, SeekOrigin.Begin);
							stream.SetLength(0);
						}
					}
					return TaskHelpers.CompletedTask;
				}, token, options, transaction => { }).ConfigureAwait(false);
			}
		}

		public void Dispose() {
			// nothing to dispose
		}
	}


	public struct StreamData {
		public readonly byte[] Data;
		public readonly long StreamVersion;

		public StreamData(byte[] data, long streamVersion) {
			Data = data;
			StreamVersion = streamVersion;
		}
	}

	/// <summary>
	///   This record will never have "audit" flag.
	/// </summary>
	public struct StoreData {
		public readonly byte[] Data;

		public readonly long StoreVersion;

		public StoreData(byte[] data, long storeVersion) {
			if (null == data)
				throw new ArgumentNullException("data");
			Data = data;
			StoreVersion = storeVersion;
		}
	}

	/// <summary>
	///   Is thrown internally, when storage version does not match the condition
	///   specified in server request
	/// </summary>
	[Serializable]
	public class AppendOnlyStoreConcurrencyException : Exception {
		public long ExpectedStreamVersion { get; private set; }
		public long ActualStreamVersion { get; private set; }
		public string StreamName { get; private set; }

		protected AppendOnlyStoreConcurrencyException(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context) {}

		public AppendOnlyStoreConcurrencyException(long expectedVersion, long actualVersion, string name)
			: base(
				string.Format("Expected version {0} in stream '{1}' but got {2}", expectedVersion, name,
					actualVersion)) {
			StreamName = name;
			ExpectedStreamVersion = expectedVersion;
			ActualStreamVersion = actualVersion;
		}
	}

}