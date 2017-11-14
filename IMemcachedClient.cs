using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching
{
	public interface IMemcachedClient : IDisposable
	{
		event Action<IMemcachedNode> NodeFailed;

		bool Store(StoreMode mode, string key, object value);
		bool Store(StoreMode mode, string key, object value, TimeSpan validFor);
		bool Store(StoreMode mode, string key, object value, DateTime expiresAt);

		Task<bool> StoreAsync(StoreMode mode, string key, object value);
		Task<bool> StoreAsync(StoreMode mode, string key, object value, TimeSpan validFor);
		Task<bool> StoreAsync(StoreMode mode, string key, object value, DateTime expiresAt);

		CasResult<bool> Cas(StoreMode mode, string key, object value);
		CasResult<bool> Cas(StoreMode mode, string key, object value, ulong cas);
		CasResult<bool> Cas(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas);
		CasResult<bool> Cas(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas);

		Task<CasResult<bool>> CasAsync(StoreMode mode, string key, object value);
		Task<CasResult<bool>> CasAsync(StoreMode mode, string key, object value, ulong cas);
		Task<CasResult<bool>> CasAsync(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas);
		Task<CasResult<bool>> CasAsync(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas);

		bool Add(string key, object value, int cacheMinutes);
		Task<bool> AddAsync(string key, object value, int cacheMinutes);

		bool Replace(string key, object value, int cacheMinutes);
		Task<bool> ReplaceAsync(string key, object value, int cacheMinutes);

		ulong Increment(string key, ulong defaultValue, ulong delta);
		ulong Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt);
		ulong Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor);

		Task<ulong> IncrementAsync(string key, ulong defaultValue, ulong delta);
		Task<ulong> IncrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt);
		Task<ulong> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor);

		CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, ulong cas);
		CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas);
		CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas);

		Task<CasResult<ulong>> IncrementAsync(string key, ulong defaultValue, ulong delta, ulong cas);
		Task<CasResult<ulong>> IncrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas);
		Task<CasResult<ulong>> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas);

		ulong Decrement(string key, ulong defaultValue, ulong delta);
		ulong Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt);
		ulong Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor);

		Task<ulong> DecrementAsync(string key, ulong defaultValue, ulong delta);
		Task<ulong> DecrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt);
		Task<ulong> DecrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor);

		CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, ulong cas);
		CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas);
		CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas);

		Task<CasResult<ulong>> DecrementAsync(string key, ulong defaultValue, ulong delta, ulong cas);
		Task<CasResult<ulong>> DecrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas);
		Task<CasResult<ulong>> DecrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas);

		bool Append(string key, ArraySegment<byte> data);
		CasResult<bool> Append(string key, ulong cas, ArraySegment<byte> data);
		Task<bool> AppendAsync(string key, ArraySegment<byte> data);
		Task<CasResult<bool>> AppendAsync(string key, ulong cas, ArraySegment<byte> data);

		bool Prepend(string key, ArraySegment<byte> data);
		CasResult<bool> Prepend(string key, ulong cas, ArraySegment<byte> data);
		Task<bool> PrependAsync(string key, ArraySegment<byte> data);
		Task<CasResult<bool>> PrependAsync(string key, ulong cas, ArraySegment<byte> data);

		bool TryGet(string key, out object value);
		object Get(string key);
		T Get<T>(string key);
		Task<object> GetAsync(string key);
		Task<T> GetAsync<T>(string key);

		bool TryGetWithCas(string key, out CasResult<object> value);
		CasResult<object> GetWithCas(string key);
		CasResult<T> GetWithCas<T>(string key);
		Task<CasResult<object>> GetWithCasAsync(string key);
		Task<CasResult<T>> GetWithCasAsync<T>(string key);

		IDictionary<string, object> Get(IEnumerable<string> keys);
		Task<IDictionary<string, object>> GetAsync(IEnumerable<string> keys);
		IDictionary<string, T> Get<T>(IEnumerable<string> keys);
		Task<IDictionary<string, T>> GetAsync<T>(IEnumerable<string> keys);
		IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys);
		Task<IDictionary<string, CasResult<object>>> GetWithCasAsync(IEnumerable<string> keys);

		bool Remove(string key);
		Task<bool> RemoveAsync(string key);

		bool Exists(string key);
		Task<bool> ExistsAsync(string key);

		void FlushAll();
		Task FlushAllAsync();

		ServerStats Stats();
		ServerStats Stats(string type);
		Task<ServerStats> StatsAsync();
		Task<ServerStats> StatsAsync(string type);
	}
}
