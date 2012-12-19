//-----------------------------------------------------------------------
// <copyright file="MapRduce.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IMappedResultsStorageAction
	{
		private static readonly ThreadLocal<SHA1> localSha1 = new ThreadLocal<SHA1>(() => new SHA1Managed());

		public static byte[] HashReduceKey(string reduceKey)
		{
			return localSha1.Value.ComputeHash(Encoding.UTF8.GetBytes(reduceKey));
		}

		public void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data)
		{
			Guid etag = uuidGenerator.CreateSequentialUuid();
			using (var update = new Update(session, MappedResults, JET_prep.Insert))
			{
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], view, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"], docId, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["hashed_reduce_key"], HashReduceKey(reduceKey));
				var mapBucket = IndexingUtil.MapBucket(docId);
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"], mapBucket);

				using (Stream stream = new BufferedStream(new ColumnStream(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"])))
				{
					using (var dataStream = documentCodecs.Aggregate(stream, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
					{
						data.WriteTo(dataStream);
						dataStream.Flush();
					}
				}

				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"], etag.TransformToValueForEsentSorting());
				Api.SetColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"], SystemTime.UtcNow);

				update.Save();
			}

			IncrementReduceKeyCounter(view, reduceKey);
		}

		public IEnumerable<ReduceKeyAndCount> GetKeysStats(string view, int start, int pageSize)
		{
			Api.JetSetCurrentIndex(session, ReduceKeysCounts, "by_view");
			Api.MakeKey(session, ReduceKeysCounts, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, ReduceKeysCounts, SeekGrbit.SeekEQ) == false)
				yield break;

			Api.MakeKey(session, ReduceKeysCounts, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, ReduceKeysCounts, SetIndexRangeGrbit.RangeInclusive|SetIndexRangeGrbit.RangeUpperLimit);

			while (start > 0)
			{
				start--;
				if (Api.TryMoveNext(session, ReduceKeysCounts) == false)
					yield break;
			}

			do
			{
				pageSize--;
				var count =
					Api.RetrieveColumnAsInt32(session, ReduceKeysCounts,
					                          tableColumnsCache.ReduceKeysCountsColumns["mapped_items_count"]).Value;
				var key = Api.RetrieveColumnAsString(session, ReduceKeysCounts,
				                                     tableColumnsCache.ReduceKeysCountsColumns["reduce_key"], Encoding.Unicode);

				yield return new ReduceKeyAndCount
				{
					Count = count,
					Key = key
				};
			} while (Api.TryMoveNext(session, ReduceKeysCounts) && pageSize > 0);
		}


		public void PutReducedResult(string view, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
		{
			Guid etag = uuidGenerator.CreateSequentialUuid();

			using (var update = new Update(session, ReducedResults, JET_prep.Insert))
			{
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["view"], view, Encoding.Unicode);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["level"], level);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["hashed_reduce_key"], HashReduceKey(reduceKey));
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["bucket"], bucket);
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["source_bucket"], sourceBucket);

				using (Stream stream = new BufferedStream(new ColumnStream(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["data"])))
				{
					using (var dataStream = documentCodecs.Aggregate(stream, (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
					{
						data.WriteTo(dataStream);
						dataStream.Flush();
					}
				}

				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["etag"], etag.TransformToValueForEsentSorting());
				Api.SetColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["timestamp"], SystemTime.UtcNow);

				update.Save();
			}
		}

		public void ScheduleReductions(string view, int level, IEnumerable<ReduceKeyAndBucket> reduceKeysAndBuckets)
		{
			foreach (var reduceKeysAndBukcet in reduceKeysAndBuckets)
			{
				var bucket = reduceKeysAndBukcet.Bucket;

				using (var map = new Update(session, ScheduledReductions, JET_prep.Insert))
				{
					Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["view"],
								  view, Encoding.Unicode);
					Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["reduce_key"],
								  reduceKeysAndBukcet.ReduceKey, Encoding.Unicode);
					Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["hashed_reduce_key"],
												  HashReduceKey(reduceKeysAndBukcet.ReduceKey));

					Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["etag"],
								  uuidGenerator.CreateSequentialUuid());

					Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["timestamp"],
								  SystemTime.UtcNow);


					Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["bucket"],
								  bucket);

					Api.SetColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["level"], level);
					map.Save();
				}
			}
		}

		public ScheduledReductionInfo DeleteScheduledReduction(List<object> itemsToDelete)
		{
			var hasResult = false;
			var result = new ScheduledReductionInfo();
			var currentEtagBinary = Guid.Empty.ToByteArray();
			foreach (var sortedBookmark in itemsToDelete.Cast<OptimizedIndexReader>().SelectMany(reader => reader.GetSortedBookmarks()))
			{
				Api.JetGotoBookmark(session, ScheduledReductions, sortedBookmark, sortedBookmark.Length);
				var etagBinary = Api.RetrieveColumn(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["etag"]);
				if (new ComparableByteArray(etagBinary).CompareTo(currentEtagBinary) > 0)
				{
					hasResult = true;
					var timestamp = Api.RetrieveColumnAsDateTime(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["timestamp"]).Value;
					result.Etag = etagBinary.TransfromToGuidWithProperSorting();
					result.Timestamp = timestamp;
				}

				Api.JetDelete(session, ScheduledReductions);
			}
			return hasResult ? result : null;
		}

		public IEnumerable<MappedResultInfo> GetItemsToReduce(string index, string[] reduceKeys, int level, int take, bool loadData, List<object> itemsToDelete)
		{
			Api.JetSetCurrentIndex(session, ScheduledReductions, "by_view_level_and_hashed_reduce_key");
			
			foreach (var reduceKey in reduceKeys)
			{
				Api.MakeKey(session, ScheduledReductions, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, ScheduledReductions, level, MakeKeyGrbit.None);
				Api.MakeKey(session, ScheduledReductions, HashReduceKey(reduceKey), MakeKeyGrbit.None);
				if (Api.TrySeek(session, ScheduledReductions, SeekGrbit.SeekEQ) == false)
					yield break;

				Api.MakeKey(session, ScheduledReductions, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, ScheduledReductions, level, MakeKeyGrbit.None);
				Api.MakeKey(session, ScheduledReductions, HashReduceKey(reduceKey), MakeKeyGrbit.None);

				Api.TrySetIndexRange(session, ScheduledReductions,
				                     SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

				// this isn't used for optimized reading, but to make it easier to delete records later on
				var reader = new OptimizedIndexReader(session, ScheduledReductions, take);
				itemsToDelete.Add(reader);
				var seen = new HashSet<Tuple<string, int>>();
				do
				{
					var indexFromDb = Api.RetrieveColumnAsString(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["view"], Encoding.Unicode, RetrieveColumnGrbit.RetrieveFromIndex);
					var levelFromDb =
						Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["level"], RetrieveColumnGrbit.RetrieveFromIndex).
							Value;
					var reduceKeyFromDb = Api.RetrieveColumnAsString(session, ScheduledReductions,
												   tableColumnsCache.ScheduledReductionColumns["reduce_key"]);

					if (string.Equals(index, indexFromDb, StringComparison.InvariantCultureIgnoreCase) == false)
						continue;
					if (levelFromDb != level)
						continue;
					if (string.IsNullOrEmpty(reduceKey) == false &&
					    string.Equals(reduceKeyFromDb, reduceKey, StringComparison.Ordinal) == false)
						continue;

					var bucket =
							Api.RetrieveColumnAsInt32(session, ScheduledReductions, tableColumnsCache.ScheduledReductionColumns["bucket"]).Value;

					if (seen.Add(Tuple.Create(reduceKeyFromDb, bucket)))
					{
						foreach (var mappedResultInfo in GetResultsForBucket(index, level, reduceKeyFromDb, bucket, loadData))
						{
							take--;
							yield return mappedResultInfo;
						}
					}

					reader.Add();
				} while (Api.TryMoveNext(session, ScheduledReductions) && take > 0);

				if (take <= 0)
					break;
			}
		}

		private IEnumerable<MappedResultInfo> GetResultsForBucket(string index, int level, string reduceKey, int bucket, bool loadData)
		{
			switch (level)
			{
				case 0:
					return GetMappedResultsForBucket(index, reduceKey, bucket, loadData);
				case 1:
				case 2:
					return GetReducedResultsForBucket(index, reduceKey, level, bucket, loadData);
				default:
					throw new ArgumentException("Invalid level: " + level);
			}
		}

		private IEnumerable<MappedResultInfo> GetMappedResultsForBucket(string index, string reduceKey, int bucket, bool loadData)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_bucket_and_hashed_reduce_key");
			Api.MakeKey(session, MappedResults, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, bucket, MakeKeyGrbit.None);
			Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);

			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
			{
				yield return new MappedResultInfo
				{
					ReduceKey = reduceKey,
					Bucket = bucket
				};
				yield break;
			}

			Api.MakeKey(session, MappedResults, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, bucket, MakeKeyGrbit.None);
			Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);

			Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			bool returnedResults = false;
			do
			{
				var indexFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
				var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
				var bucketFromDb = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;
				if (string.Equals(indexFromDb, index, StringComparison.InvariantCultureIgnoreCase) == false ||
					bucketFromDb != bucket ||
					string.Equals(keyFromDb, reduceKey, StringComparison.Ordinal) == false // the key is explicitly compared using case sensitive approach
					)
				{
					// we might have a hash collision, so we will just skip and try the next one
					continue;
				}
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey =
						keyFromDb,
					Etag = new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
					Timestamp =
						Api.RetrieveColumnAsDateTime(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).
							Value,
					Data = loadData ? LoadMappedResults(keyFromDb) : null,
					Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0
				};
				returnedResults = true;
			} while (Api.TryMoveNext(session, MappedResults));

			if (returnedResults == false)
			{
				{
					yield return new MappedResultInfo
					{
						ReduceKey = reduceKey,
						Bucket = bucket
					};
				}
			}
		}


		public void RemoveReduceResults(string indexName, int level, string reduceKey, int sourceBucket)
		{
			Api.JetSetCurrentIndex(session, ReducedResults, "by_view_level_source_bucket_and_hashed_reduce_key");
			Api.MakeKey(session, ReducedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, sourceBucket, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);

			if (Api.TrySeek(session, ReducedResults, SeekGrbit.SeekEQ) == false)
				return;

			do
			{
				var indexFromDb = Api.RetrieveColumnAsString(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["view"], Encoding.Unicode, RetrieveColumnGrbit.RetrieveFromIndex);
				var bucketFromDb = Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["source_bucket"], RetrieveColumnGrbit.RetrieveFromIndex).Value;
				if (string.Equals(indexFromDb, indexName, StringComparison.InvariantCultureIgnoreCase) == false ||
					bucketFromDb != sourceBucket)
				{
					break;
				}

				var keyFromDb = Api.RetrieveColumnAsString(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["reduce_key"]);
				if (string.Equals(keyFromDb, reduceKey, StringComparison.Ordinal) == false)// case sensitive check on purpose
					continue;


				Api.JetDelete(session, ReducedResults);
			} while (Api.TryMoveNext(session, ReducedResults));
		}

		private IEnumerable<MappedResultInfo> GetReducedResultsForBucket(string index, string reduceKey, int level, int bucket, bool loadData)
		{
			Api.JetSetCurrentIndex(session, ReducedResults, "by_view_level_bucket_and_hashed_reduce_key");
			Api.MakeKey(session, ReducedResults, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, bucket, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);

			if (Api.TrySeek(session, ReducedResults, SeekGrbit.SeekEQ) == false)
			{
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey = reduceKey,
				};
				yield break;
			}

			Api.MakeKey(session, ReducedResults, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, bucket, MakeKeyGrbit.None);
			Api.MakeKey(session, ReducedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);
			Api.JetSetIndexRange(session, ReducedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
			bool returnedResults = false;
			do
			{
				var key = Api.RetrieveColumnAsString(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["reduce_key"]);
				var bucketFromDb = Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["bucket"]).Value;
				if (bucketFromDb != bucket)
					break;

				// we explicitly compare the key just as we would during the group by phase, using case sensitive approach
				if (string.Equals(key, reduceKey, StringComparison.Ordinal) == false)
					continue;
				returnedResults = true;
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey =
						key,
					Etag = new Guid(Api.RetrieveColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["etag"])),
					Timestamp =
						Api.RetrieveColumnAsDateTime(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["timestamp"]).
							Value,
					Data = loadData ? LoadReducedResults(key) : null,
					Size = Api.RetrieveColumnSize(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["data"]) ?? 0
				};
			} while (Api.TryMoveNext(session, ReducedResults));

			if (returnedResults == false)
			{
				yield return new MappedResultInfo
				{
					Bucket = bucket,
					ReduceKey = reduceKey,
				};
			}

		}
		public void DeleteMappedResultsForDocumentId(string documentId, string view, HashSet<ReduceKeyAndBucket> removed)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_doc_key");
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, documentId, Encoding.Unicode, MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
				return;

			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, documentId, Encoding.Unicode, MakeKeyGrbit.None);
			Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
			do
			{
				// esent index ranges are approximate, and we need to check them ourselves as well
				var viewFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
				if (StringComparer.InvariantCultureIgnoreCase.Equals(viewFromDb, view) == false)
					continue;
				var documentIdFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"]);
				if (StringComparer.InvariantCultureIgnoreCase.Equals(documentIdFromDb, documentId) == false)
					continue;
				var reduceKey = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"],
														   Encoding.Unicode);
				var bucket = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;

				removed.Add(new ReduceKeyAndBucket(bucket, reduceKey));
				Api.JetDelete(session, MappedResults);
			} while (Api.TryMoveNext(session, MappedResults));

			foreach (var reduceKey in removed.Select(x => x.ReduceKey))
			{
				DecrementReduceKeyCounter(view, reduceKey);
			}
		}

		public void DeleteMappedResultsForView(string view)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view");
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
				return;
			Api.MakeKey(session, MappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			var deletedReduceKeys = new List<string>();

			do
			{
				// esent index ranges are approximate, and we need to check them ourselves as well
				var viewFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
				if (StringComparer.InvariantCultureIgnoreCase.Equals(viewFromDb, view) == false)
					continue;

				var reduceKey = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
				deletedReduceKeys.Add(reduceKey);

				Api.JetDelete(session, MappedResults);

			} while (Api.TryMoveNext(session, MappedResults));

			foreach (var reduceKey in deletedReduceKeys)
			{
				DecrementReduceKeyCounter(view, reduceKey);
			}
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsReduceKeysAfter(string indexName, Guid lastReducedEtag, bool loadData, int take)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_etag");
			Api.MakeKey(session, MappedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, lastReducedEtag, MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekLE) == false)
				return Enumerable.Empty<MappedResultInfo>();

			var results = new Dictionary<string, MappedResultInfo>();
			while (
				results.Count < take &&
				Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"], Encoding.Unicode, RetrieveColumnGrbit.RetrieveFromIndex) == indexName)
			{
				var key = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);
				var mappedResultInfo = new MappedResultInfo
				{
					ReduceKey =
						key,
					Etag = new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
					Timestamp =
						Api.RetrieveColumnAsDateTime(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).
						Value,
					Data = loadData
							? LoadMappedResults(key)
							: null,
					Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0
				};

				results[mappedResultInfo.ReduceKey] = mappedResultInfo;

				// the index is view ascending and etag descending
				// that means that we are going backward to go up
				if (Api.TryMovePrevious(session, MappedResults) == false)
					break;
			}

			return results.Values;
		}

		public IEnumerable<string> GetKeysForIndexForDebug(string indexName, int start, int take)
		{
			if (take <= 0)
				yield break;

			Api.JetSetCurrentIndex(session, MappedResults, "by_view_reduce_key_and_bucket");
			Api.MakeKey(session, MappedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false)
				yield break;

			try
			{
				Api.JetMove(session, MappedResults, start, MoveGrbit.MoveKeyNE);
			}
			catch (EsentErrorException e)
			{
				if (e.Error == JET_err.NoCurrentRecord)
				{
					yield break;
				}
				throw;
			}

			var results = new HashSet<string>();
			do
			{
				var indexNameFromDb = Api.RetrieveColumnAsString(session, MappedResults,
																 tableColumnsCache.MappedResultsColumns["view"], Encoding.Unicode,
																 RetrieveColumnGrbit.RetrieveFromIndex);
				var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults,
														   tableColumnsCache.MappedResultsColumns["reduce_key"]);
				var comparision = String.Compare(indexNameFromDb, indexName, StringComparison.InvariantCultureIgnoreCase);
				if (comparision < 0)
					continue; // skip to the next item
				if (comparision > 0) // after the current item
					break;

				if (results.Add(keyFromDb))
				{
					take -= 1;
					yield return keyFromDb;
				}
			} while (Api.TryMoveNext(session, MappedResults) && take > 0);
		}

		public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(string indexName, string key, int take)
		{
			if (take <= 0)
				yield break;


			// NOTE, this intentionally does a table scan for all the items in the same index.
			// the reason it is allowed is that this is only applicable for debug, and never is used in production systems
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_bucket_and_hashed_reduce_key");
			Api.MakeKey(session, MappedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekGE) == false)
				yield break;

			do
			{

				var indexNameFromDb = Api.RetrieveColumnAsString(session, MappedResults,
																 tableColumnsCache.MappedResultsColumns["view"], Encoding.Unicode,
																 RetrieveColumnGrbit.RetrieveFromIndex);
				var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults,
														   tableColumnsCache.MappedResultsColumns["reduce_key"]);

				var indexCompare = string.Compare(indexNameFromDb, indexName, StringComparison.InvariantCultureIgnoreCase);

				if (indexCompare < 0)
					continue;
				if (indexCompare > 0)
					break;
				var keyCompare = string.Compare(key, keyFromDb, StringComparison.InvariantCultureIgnoreCase);
				if (keyCompare != 0)
					continue;

				take -= 1;

				var bucket =
					Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;
				yield return new MappedResultInfo
				{
					ReduceKey = keyFromDb,
					Etag = new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
					Timestamp =
						Api.RetrieveColumnAsDateTime(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).Value,
					Data = LoadMappedResults(keyFromDb),
					Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0,
					Bucket = bucket,
					Source =
						Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["document_key"],
												   Encoding.Unicode)
				};

			} while (Api.TryMoveNext(session, MappedResults) && take > 0);
		}

		public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(string indexName, string key, int level, int take)
		{
			if (take <= 0)
				yield break;

			// NOTE, this intentionally does a table scan for all the items in the same index.
			// the reason it is allowed is that this is only applicable for debug, and never is used in production systems

			Api.JetSetCurrentIndex(session, ReducedResults, "by_view_level_source_bucket_and_hashed_reduce_key");
			Api.MakeKey(session, ReducedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, ReducedResults, level, MakeKeyGrbit.None);
			if (Api.TrySeek(session, ReducedResults, SeekGrbit.SeekGE) == false)
				yield break;

			do
			{

				var levelFromDb =
					Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["level"]).Value;
				var indexNameFromDb = Api.RetrieveColumnAsString(session, ReducedResults,
																 tableColumnsCache.ReduceResultsColumns["view"], Encoding.Unicode,
																 RetrieveColumnGrbit.RetrieveFromIndex);
				var keyFromDb = Api.RetrieveColumnAsString(session, ReducedResults,
														   tableColumnsCache.ReduceResultsColumns["reduce_key"]);
				var indexCompare = string.Compare(indexNameFromDb, indexName, StringComparison.InvariantCultureIgnoreCase);

				if (indexCompare < 0)
					continue;
				if (indexCompare > 0)
					break;
				if (levelFromDb < level)
					continue;
				if (levelFromDb > level)
					break;
				var keyCompare = string.Compare(key, keyFromDb, StringComparison.InvariantCultureIgnoreCase);
				if (keyCompare != 0)
					continue;

				take -= 1;


				yield return new MappedResultInfo
				{
					ReduceKey = keyFromDb,
					Etag = new Guid(Api.RetrieveColumn(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["etag"])),
					Timestamp =
						Api.RetrieveColumnAsDateTime(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["timestamp"]).Value,
					Data = LoadReducedResults(keyFromDb),
					Size = Api.RetrieveColumnSize(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["data"]) ?? 0,
					Bucket = Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["bucket"]).Value,
					Source =
						Api.RetrieveColumnAsInt32(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["source_bucket"]).
							ToString()
				};
			} while (Api.TryMoveNext(session, ReducedResults) && take > 0);
		}

		public IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(string indexName, int limitOfItemsToReduceInSingleStep)
		{
			var allKeysToReduce = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);

			Api.JetSetCurrentIndex(session, ScheduledReductions, "by_view");
			Api.MakeKey(session, ScheduledReductions, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, ScheduledReductions, SeekGrbit.SeekEQ) == false)
				yield break;
			Api.MakeKey(session, ScheduledReductions, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.TrySetIndexRange(session, ScheduledReductions,
			                     SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			do
			{
				var indexFromDb = Api.RetrieveColumnAsString(session, ScheduledReductions,
															 tableColumnsCache.ScheduledReductionColumns["view"], Encoding.Unicode,
															 RetrieveColumnGrbit.RetrieveFromIndex);

				if (StringComparer.InvariantCultureIgnoreCase.Equals(indexName,indexFromDb) == false)
					continue;

				var reduceKey = Api.RetrieveColumnAsString(session, ScheduledReductions,
											   tableColumnsCache.ScheduledReductionColumns["reduce_key"]);

				allKeysToReduce.Add(reduceKey);

			} while (Api.TryMoveNext(session, ScheduledReductions));

			foreach (var reduceKey in allKeysToReduce)
			{
				var count = GetNumberOfMappedItemsPerReduceKey(indexName, reduceKey);
				var reduceType = count >= limitOfItemsToReduceInSingleStep ? ReduceType.MultiStep : ReduceType.SingleStep;
				yield return new ReduceTypePerKey(reduceKey, reduceType);
			}
		}

		public void UpdatePerformedReduceType(string indexName, string reduceKey, ReduceType performedReduceType)
		{
			ExecuteOnReduceKey(indexName, reduceKey, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns, () =>
			{
				using (var update = new Update(session, ReduceKeysStatus, JET_prep.Replace))
				{
					Api.SetColumn(session, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns["reduce_type"], (int)performedReduceType);

					update.Save();
				}
			});
		}

		private void ExecuteOnReduceKey(string view, string reduceKey, 
			Table table,
			IDictionary<string, JET_COLUMNID> columnids,
			 Action action)
		{
			var hashReduceKey = HashReduceKey(reduceKey);

			Api.JetSetCurrentIndex(session, table, "by_view_and_hashed_reduce_key");
			Api.MakeKey(session, table, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, table, hashReduceKey, MakeKeyGrbit.None);
			Api.MakeKey(session, table, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);

			if (Api.TrySeek(session, table, SeekGrbit.SeekEQ) == false)
			{
				using (var update = new Update(session, table, JET_prep.Insert))
				{
					Api.SetColumn(session, table, columnids["view"], view, Encoding.Unicode);
					Api.SetColumn(session, table, columnids["reduce_key"], reduceKey, Encoding.Unicode);
					Api.SetColumn(session, table, columnids["hashed_reduce_key"], hashReduceKey);

					update.SaveAndGotoBookmark();
				}
				action();
				return;
			}

			Api.MakeKey(session, table, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, table, hashReduceKey, MakeKeyGrbit.None);
			Api.MakeKey(session, table, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);

			Api.TrySetIndexRange(session, table, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			do
			{
				var reduceKeyFromDb = Api.RetrieveColumnAsString(session, table, columnids["reduce_key"]);
				if (StringComparer.Ordinal.Equals(reduceKey, reduceKeyFromDb) == false)
					continue;

				action();
				return;
			} while (Api.TryMoveNext(session, table));
		}

		public ReduceType GetLastPerformedReduceType(string indexName, string reduceKey)
		{
			int reduceType = 0;
			ExecuteOnReduceKey(indexName, reduceKey, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns, () =>
			{
				reduceType = Api.RetrieveColumnAsInt32(session, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns["reduce_type"]).Value;
			});
			return (ReduceType)reduceType;
		}

		public IEnumerable<int> GetMappedBuckets(string indexName, string reduceKey)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_hashed_reduce_key");
			Api.MakeKey(session, MappedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);
			if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
				yield break;

			Api.MakeKey(session, MappedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);

			Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
			do
			{
				var viewFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
				if (StringComparer.InvariantCultureIgnoreCase.Equals(viewFromDb, indexName) == false)
					continue;

				var rKey = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"],
														   Encoding.Unicode);

				if (StringComparer.OrdinalIgnoreCase.Equals(rKey, reduceKey) == false)
					continue;

				yield return Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value;
			} while (Api.TryMoveNext(session, MappedResults));
		}

		public IEnumerable<MappedResultInfo> GetMappedResults(string indexName, string[] keysToReduce, bool loadData, int take)
		{
			Api.JetSetCurrentIndex(session, MappedResults, "by_view_and_hashed_reduce_key");

			foreach (var reduceKey in keysToReduce)
			{
				Api.MakeKey(session, MappedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);
				if (Api.TrySeek(session, MappedResults, SeekGrbit.SeekEQ) == false)
					continue;

				Api.MakeKey(session, MappedResults, indexName, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, MappedResults, HashReduceKey(reduceKey), MakeKeyGrbit.None);

				Api.JetSetIndexRange(session, MappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
				do
				{
					var indexFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["view"]);
					var keyFromDb = Api.RetrieveColumnAsString(session, MappedResults, tableColumnsCache.MappedResultsColumns["reduce_key"]);

					if (string.Equals(indexFromDb, indexName, StringComparison.InvariantCultureIgnoreCase) == false ||
						string.Equals(keyFromDb, reduceKey, StringComparison.Ordinal) == false)
					{
						continue;
					}
					yield return new MappedResultInfo
					{
						Bucket = Api.RetrieveColumnAsInt32(session, MappedResults, tableColumnsCache.MappedResultsColumns["bucket"]).Value,
						ReduceKey =
							keyFromDb,
						Etag = new Guid(Api.RetrieveColumn(session, MappedResults, tableColumnsCache.MappedResultsColumns["etag"])),
						Timestamp =
							Api.RetrieveColumnAsDateTime(session, MappedResults, tableColumnsCache.MappedResultsColumns["timestamp"]).
								Value,
						Data = loadData ? LoadMappedResults(keyFromDb) : null,
						Size = Api.RetrieveColumnSize(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"]) ?? 0
					};
				} while (Api.TryMoveNext(session, MappedResults));
			}
		}

		private RavenJObject LoadMappedResults(string key)
		{
			using (Stream stream = new BufferedStream(new ColumnStream(session, MappedResults, tableColumnsCache.MappedResultsColumns["data"])))
			using (var dataStream = documentCodecs.Aggregate(stream, (ds, codec) => codec.Decode(key, null, ds)))
			{
				return dataStream.ToJObject();
			}
		}

		private RavenJObject LoadReducedResults(string key)
		{
			using (Stream stream = new BufferedStream(new ColumnStream(session, ReducedResults, tableColumnsCache.ReduceResultsColumns["data"])))
			using (var dataStream = documentCodecs.Aggregate(stream, (ds, codec) => codec.Decode(key, null, ds)))
			{
				return dataStream.ToJObject();
			}
		}

		private void IncrementReduceKeyCounter(string view, string reduceKey)
		{
			ExecuteOnReduceKey(view, reduceKey, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns,
				() => Api.EscrowUpdate(session, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns["mapped_items_count"], 1));
		}

		private void DecrementReduceKeyCounter(string view, string reduceKey)
		{
			var removeReducedKeyStatus = false;

			ExecuteOnReduceKey(view, reduceKey, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns, 
				() =>
				{
					
					if (Api.EscrowUpdate(session, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns["mapped_items_count"], -1) == 1)
					{
						Api.JetDelete(session, ReduceKeysCounts);
						removeReducedKeyStatus = true;
					}
				});

			if (removeReducedKeyStatus)
			{
				ExecuteOnReduceKey(view, reduceKey, ReduceKeysStatus, tableColumnsCache.ReduceKeysStatusColumns,
				                   () => Api.JetDelete(session, ReduceKeysStatus));
			}
		}

		private int GetNumberOfMappedItemsPerReduceKey(string view, string reduceKey)
		{
			int numberOfMappedItemsPerReduceKey = 0;
			ExecuteOnReduceKey(view, reduceKey, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns, () =>
			{
				numberOfMappedItemsPerReduceKey = Api.RetrieveColumnAsInt32(session, ReduceKeysCounts, tableColumnsCache.ReduceKeysCountsColumns["mapped_items_count"]).Value;
			});
			
			return numberOfMappedItemsPerReduceKey;
		}
	}
}
