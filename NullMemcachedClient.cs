using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Factories;

namespace Enyim.Caching
{
	public class NullMemcachedClient : IMemcachedClient
	{
		public event Action<IMemcachedNode> NodeFailed;

		public bool Append(string key, ArraySegment<byte> data)
		{
			return true;
		}

		public CasResult<bool> Append(string key, ulong cas, ArraySegment<byte> data)
		{
			return new CasResult<bool>();
		}

		public CasResult<bool> Cas(StoreMode mode, string key, object value)
		{
			return new CasResult<bool>();
		}

		public CasResult<bool> Cas(StoreMode mode, string key, object value, ulong cas)
		{
			return new CasResult<bool>();
		}

		public CasResult<bool> Cas(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas)
		{
			return new CasResult<bool>();
		}

		public CasResult<bool> Cas(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas)
		{
			throw new NotImplementedException();
		}

		public ulong Decrement(string key, ulong defaultValue, ulong delta)
		{
			throw new NotImplementedException();
		}

		public ulong Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
		{
			throw new NotImplementedException();
		}

		public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, ulong cas)
		{
			throw new NotImplementedException();
		}

		public ulong Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
		{
			throw new NotImplementedException();
		}

		public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
		{
			throw new NotImplementedException();
		}

		public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{

		}

		public void FlushAll()
		{
			throw new NotImplementedException();
		}

		public object Get(string key)
		{
			throw new NotImplementedException();
		}

		public T Get<T>(string key)
		{
			return default(T);
		}

		public Task<IGetOperationResult<T>> DoGetAsync<T>(string key)
		{
			var result = new DefaultGetOperationResultFactory<T>().Create();
			result.Success = false;
			result.Value = default(T);
			return Task.FromResult(result);
		}

		public IDictionary<string, object> Get(IEnumerable<string> keys)
		{
			throw new NotImplementedException();
		}

		public Task<IDictionary<string, object>> GetAsync(IEnumerable<string> keys)
		{
			throw new NotImplementedException();
		}

		public Task<object> GetAsync(string key)
		{
			return Task.FromResult<object>(null);
		}

		public Task<T> GetAsync<T>(string key)
		{
			return Task.FromResult(default(T));
		}

		public IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys)
		{
			throw new NotImplementedException();
		}

		public CasResult<object> GetWithCas(string key)
		{
			throw new NotImplementedException();
		}

		public CasResult<T> GetWithCas<T>(string key)
		{
			throw new NotImplementedException();
		}

		public ulong Increment(string key, ulong defaultValue, ulong delta)
		{
			throw new NotImplementedException();
		}

		public ulong Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
		{
			throw new NotImplementedException();
		}

		public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, ulong cas)
		{
			throw new NotImplementedException();
		}

		public ulong Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
		{
			throw new NotImplementedException();
		}

		public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
		{
			throw new NotImplementedException();
		}

		public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
		{
			throw new NotImplementedException();
		}

		public bool Prepend(string key, ArraySegment<byte> data)
		{
			throw new NotImplementedException();
		}

		public CasResult<bool> Prepend(string key, ulong cas, ArraySegment<byte> data)
		{
			throw new NotImplementedException();
		}

		public bool Remove(string key)
		{
			return true;
		}

		public Task<bool> RemoveAsync(string key)
		{
			return Task.FromResult(true);
		}

		public ServerStats Stats()
		{
			throw new NotImplementedException();
		}

		public ServerStats Stats(string type)
		{
			throw new NotImplementedException();
		}

		public bool Store(StoreMode mode, string key, object value)
		{
			return false;
		}

		public bool Store(StoreMode mode, string key, object value, TimeSpan validFor)
		{
			return false;
		}

		public Task<bool> StoreAsync(StoreMode mode, string key, object value, TimeSpan validFor)
		{
			return Task.FromResult(false);
		}

		public Task<bool> StoreAsync(StoreMode mode, string key, object value, DateTime expiresAt)
		{
			return Task.FromResult(false);
		}

		public bool Store(StoreMode mode, string key, object value, DateTime expiresAt)
		{
			return false;
		}

		public bool TryGet(string key, out object value)
		{
			throw new NotImplementedException();
		}

		public bool TryGetWithCas(string key, out CasResult<object> value)
		{
			throw new NotImplementedException();
		}

		public bool Add(string key, object value, int cacheMinutes)
		{
			throw new NotImplementedException();
		}

		public Task<bool> AddAsync(string key, object value, int cacheMinutes)
		{
			throw new NotImplementedException();
		}

		public bool Replace(string key, object value, int cacheMinutes)
		{
			throw new NotImplementedException();
		}

		public Task<bool> ReplaceAsync(string key, object value, int cacheMinutes)
		{
			throw new NotImplementedException();
		}

		public bool Exists(string key)
		{
			throw new NotImplementedException();
		}

		public Task<bool> ExistsAsync(string key)
		{
			throw new NotImplementedException();
		}
	}
}
