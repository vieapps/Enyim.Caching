using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching
{
	public interface IMemcachedClient : IDisposable
	{
		bool Store(StoreMode mode, string key, object value);
		bool Store(StoreMode mode, string key, object value, DateTime expiresAt);
		bool Store(StoreMode mode, string key, object value, TimeSpan validFor);
		Task<bool> StoreAsync(StoreMode mode, string key, object value, DateTime expiresAt);
		Task<bool> StoreAsync(StoreMode mode, string key, object value, TimeSpan validFor);

		bool Add(string key, object value, int cacheMinutes);
		Task<bool> AddAsync(string key, object value, int cacheMinutes);

		bool Replace(string key, object value, int cacheMinutes);
		Task<bool> ReplaceAsync(string key, object value, int cacheMinutes);

		CasResult<bool> Cas(StoreMode mode, string key, object value);
		CasResult<bool> Cas(StoreMode mode, string key, object value, ulong cas);
		CasResult<bool> Cas(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas);
		CasResult<bool> Cas(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas);

		ulong Decrement(string key, ulong defaultValue, ulong delta);
		ulong Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt);
		ulong Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor);

		CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, ulong cas);
		CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas);
		CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas);

		ulong Increment(string key, ulong defaultValue, ulong delta);
		ulong Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt);
		ulong Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor);

		CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, ulong cas);
		CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas);
		CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas);

		bool Append(string key, ArraySegment<byte> data);
		CasResult<bool> Append(string key, ulong cas, ArraySegment<byte> data);

		bool Prepend(string key, ArraySegment<byte> data);
		CasResult<bool> Prepend(string key, ulong cas, ArraySegment<byte> data);

		object Get(string key);
		T Get<T>(string key);
		Task<object> GetAsync(string key);
		Task<T> GetAsync<T>(string key);
		Task<IGetOperationResult<T>> DoGetAsync<T>(string key);
		IDictionary<string, object> Get(IEnumerable<string> keys);
		Task<IDictionary<string, object>> GetAsync(IEnumerable<string> keys);

		bool TryGet(string key, out object value);
		bool TryGetWithCas(string key, out CasResult<object> value);

		CasResult<object> GetWithCas(string key);
		CasResult<T> GetWithCas<T>(string key);
		IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys);

		bool Exists(string key);
		Task<bool> ExistsAsync(string key);

		bool Remove(string key);
		Task<bool> RemoveAsync(string key);

		void FlushAll();

		ServerStats Stats();
		ServerStats Stats(string type);

		event Action<IMemcachedNode> NodeFailed;
	}
}
