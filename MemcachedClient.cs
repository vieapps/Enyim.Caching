using System;
using System.Net;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Factories;
using Enyim.Caching.Memcached.Results.Extensions;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Caching.Distributed;

namespace Enyim.Caching
{
	/// <summary>
	/// Memcached client.
	/// </summary>
	public partial class MemcachedClient : IMemcachedClient, IMemcachedResultsClient, IDistributedCache
	{

		#region Attributes
		/// <summary>
		/// Represents a value which indicates that an item should never expire.
		/// </summary>
		public static readonly TimeSpan Infinite = TimeSpan.Zero;
		private ILogger<MemcachedClient> _logger;

		private IServerPool pool;
		private IMemcachedKeyTransformer keyTransformer;
		private ITranscoder transcoder;

		public IStoreOperationResultFactory StoreOperationResultFactory { get; set; }

		public IGetOperationResultFactory GetOperationResultFactory { get; set; }

		public IMutateOperationResultFactory MutateOperationResultFactory { get; set; }

		public IConcatOperationResultFactory ConcatOperationResultFactory { get; set; }

		public IRemoveOperationResultFactory RemoveOperationResultFactory { get; set; }

		protected IServerPool Pool { get { return this.pool; } }

		protected IMemcachedKeyTransformer KeyTransformer { get { return this.keyTransformer; } }

		protected ITranscoder Transcoder { get { return this.transcoder; } }

		public event Action<IMemcachedNode> NodeFailed;
		#endregion

		#region Constructors
		public MemcachedClient(MemcachedClientConfigurationSectionHandler configuration, ILoggerFactory loggerFactory = null)
		{
			loggerFactory = loggerFactory ?? new NullLoggerFactory();

			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			this.Initialize(new MemcachedClientConfiguration(loggerFactory, configuration), loggerFactory);
		}

		public MemcachedClient(IMemcachedClientConfiguration configuration, ILoggerFactory loggerFactory)
		{
			this.Initialize(configuration, loggerFactory);
		}

		void Initialize(IMemcachedClientConfiguration configuration, ILoggerFactory loggerFactory)
		{
			this._logger = loggerFactory.CreateLogger<MemcachedClient>();

			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			this.keyTransformer = configuration.CreateKeyTransformer() ?? new DefaultKeyTransformer();
			this.transcoder = configuration.CreateTranscoder() ?? new DefaultTranscoder();

			this.pool = configuration.CreatePool();
			this.pool.NodeFailed += (node) =>
			{
				this.NodeFailed?.Invoke(node);
			};
			this.pool.Start();

			this.StoreOperationResultFactory = new DefaultStoreOperationResultFactory();
			this.GetOperationResultFactory = new DefaultGetOperationResultFactory();
			this.MutateOperationResultFactory = new DefaultMutateOperationResultFactory();
			this.ConcatOperationResultFactory = new DefaultConcatOperationResultFactory();
			this.RemoveOperationResultFactory = new DefaultRemoveOperationResultFactory();
		}
		#endregion

		#region Store
		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
		/// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
		public bool Store(StoreMode mode, string key, object value)
		{
			ulong tmp = 0;
			return this.PerformStore(mode, key, value, 0, ref tmp, out int status).Success;
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
		/// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
		public async Task<bool> StoreAsync(StoreMode mode, string key, object value)
		{
			return (await this.PerformStoreAsync(mode, key, value, 0)).Success;
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
		public bool Store(StoreMode mode, string key, object value, TimeSpan validFor)
		{
			ulong tmp = 0;
			return this.PerformStore(mode, key, value, MemcachedClient.GetExpiration(validFor, null), ref tmp, out int status).Success;
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
		public async Task<bool> StoreAsync(StoreMode mode, string key, object value, TimeSpan validFor)
		{
			return (await this.PerformStoreAsync(mode, key, value, MemcachedClient.GetExpiration(validFor, null))).Success;
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
		/// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
		public bool Store(StoreMode mode, string key, object value, DateTime expiresAt)
		{
			ulong tmp = 0;
			return this.PerformStore(mode, key, value, MemcachedClient.GetExpiration(null, expiresAt), ref tmp, out int status).Success;
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
		/// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
		public async Task<bool> StoreAsync(StoreMode mode, string key, object value, DateTime expiresAt)
		{
			return (await this.PerformStoreAsync(mode, key, value, MemcachedClient.GetExpiration(null, expiresAt))).Success;
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location and returns its version.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
		/// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
		public CasResult<bool> Cas(StoreMode mode, string key, object value, ulong cas)
		{
			var result = this.PerformStore(mode, key, value, 0, cas);
			return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };

		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location and returns its version.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
		public CasResult<bool> Cas(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas)
		{
			var result = this.PerformStore(mode, key, value, MemcachedClient.GetExpiration(validFor, null), cas);
			return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location and returns its version.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
		public CasResult<bool> Cas(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas)
		{
			var result = this.PerformStore(mode, key, value, MemcachedClient.GetExpiration(null, expiresAt), cas);
			return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location and returns its version.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <remarks>The item does not expire unless it is removed due memory pressure. The text protocol does not support this operation, you need to Store then GetWithCas.</remarks>
		/// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
		public CasResult<bool> Cas(StoreMode mode, string key, object value)
		{
			var result = this.PerformStore(mode, key, value, 0, 0);
			return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };
		}

		private IStoreOperationResult PerformStore(StoreMode mode, string key, object value, uint expires, ulong cas)
		{
			ulong tmp = cas;
			var retval = this.PerformStore(mode, key, value, expires, ref tmp, out int status);
			retval.StatusCode = status;

			if (retval.Success)
			{
				retval.Cas = tmp;
			}
			return retval;
		}

		protected virtual IStoreOperationResult PerformStore(StoreMode mode, string key, object value, uint expires, ref ulong cas, out int statusCode)
		{
			var hashedKey = this.keyTransformer.Transform(key);
			var node = this.pool.Locate(hashedKey);
			var result = this.StoreOperationResultFactory.Create();

			statusCode = -1;
			if (value == null)
			{
				result.Fail("value is null");
				return result;
			}

			if (node != null)
			{
				CacheItem item;
				try
				{
					item = this.transcoder.Serialize(value);
				}
				catch (Exception e)
				{
					this._logger.LogError("PerformStore", e);

					result.Fail("PerformStore failed", e);
					return result;
				}

				var command = this.pool.OperationFactory.Store(mode, hashedKey, item, expires, cas);
				var commandResult = node.Execute(command);

				result.Cas = cas = command.CasValue;
				result.StatusCode = statusCode = command.StatusCode;

				if (commandResult.Success)
				{
					result.Pass();
					return result;
				}

				commandResult.Combine(result);
				return result;
			}

			result.Fail("Unable to locate node");
			return result;
		}

		protected async virtual Task<IStoreOperationResult> PerformStoreAsync(StoreMode mode, string key, object value, uint expires)
		{
			var hashedKey = this.keyTransformer.Transform(key);
			var node = this.pool.Locate(hashedKey);
			var result = this.StoreOperationResultFactory.Create();

			int statusCode = -1;
			ulong cas = 0;
			if (value == null)
			{
				result.Fail("value is null");
				return result;
			}

			if (node != null)
			{
				CacheItem item;
				try
				{
					item = this.transcoder.Serialize(value);
				}
				catch (Exception e)
				{
					this._logger.LogError(new EventId(), e, $"{nameof(this.PerformStoreAsync)} for '{key}' key");

					result.Fail("PerformStore failed", e);
					return result;
				}

				var command = this.pool.OperationFactory.Store(mode, hashedKey, item, expires, cas);
				var commandResult = await node.ExecuteAsync(command);

				result.Cas = cas = command.CasValue;
				result.StatusCode = statusCode = command.StatusCode;

				if (commandResult.Success)
				{
					result.Pass();
					return result;
				}

				commandResult.Combine(result);
				return result;
			}

			result.Fail("Unable to locate memcached node");
			return result;
		}
		#endregion

		#region Add
		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="value"></param>
		/// <param name="cacheMinutes"></param>
		/// <returns>true if the item was successfully added in the cache; false otherwise.</returns>
		public bool Add(string key, object value, int cacheMinutes)
		{
			return this.Store(StoreMode.Add, key, value, new TimeSpan(0, cacheMinutes, 0));
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="value"></param>
		/// <param name="cacheMinutes"></param>
		/// <returns>true if the item was successfully added in the cache; false otherwise.</returns>
		public Task<bool> AddAsync(string key, object value, int cacheMinutes)
		{
			return this.StoreAsync(StoreMode.Add, key, value, new TimeSpan(0, cacheMinutes, 0));
		}
		#endregion

		#region Replace
		/// <summary>
		/// Replaces an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="value"></param>
		/// <param name="cacheMinutes"></param>
		/// <returns>true if the item was successfully replaced in the cache; false otherwise.</returns>
		public bool Replace(string key, object value, int cacheMinutes)
		{
			return this.Store(StoreMode.Replace, key, value, new TimeSpan(0, cacheMinutes, 0));
		}

		/// <summary>
		/// Replaces an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="value"></param>
		/// <param name="cacheMinutes"></param>
		/// <returns>true if the item was successfully replaced in the cache; false otherwise.</returns>
		public Task<bool> ReplaceAsync(string key, object value, int cacheMinutes)
		{
			return this.StoreAsync(StoreMode.Replace, key, value, new TimeSpan(0, cacheMinutes, 0));
		}
		#endregion

		#region Mutate
		/// <summary>
		/// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public ulong Increment(string key, ulong defaultValue, ulong delta)
		{
			return this.PerformMutate(MutationMode.Increment, key, defaultValue, delta, 0).Value;
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public ulong Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
		{
			return this.PerformMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor, null)).Value;
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public ulong Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
		{
			return this.PerformMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(null, expiresAt)).Value;
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, ulong cas)
		{
			var result = this.CasMutate(MutationMode.Increment, key, defaultValue, delta, 0, cas);
			return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
		{
			var result = this.CasMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor, null), cas);
			return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
		{
			var result = this.CasMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(null, expiresAt), cas);
			return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public ulong Decrement(string key, ulong defaultValue, ulong delta)
		{
			return this.PerformMutate(MutationMode.Decrement, key, defaultValue, delta, 0).Value;
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public ulong Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
		{
			return this.PerformMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor, null)).Value;
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public ulong Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
		{
			return this.PerformMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(null, expiresAt)).Value;
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, ulong cas)
		{
			var result = this.CasMutate(MutationMode.Decrement, key, defaultValue, delta, 0, cas);
			return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
		{
			var result = this.CasMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor, null), cas);
			return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
		{
			var result = this.CasMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(null, expiresAt), cas);
			return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
		}

		private IMutateOperationResult PerformMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires)
		{
			ulong tmp = 0;
			return this.PerformMutate(mode, key, defaultValue, delta, expires, ref tmp);
		}

		private IMutateOperationResult CasMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas)
		{
			var tmp = cas;
			var retval = this.PerformMutate(mode, key, defaultValue, delta, expires, ref tmp);
			retval.Cas = tmp;
			return retval;
		}

		protected virtual IMutateOperationResult PerformMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ref ulong cas)
		{
			var hashedKey = this.keyTransformer.Transform(key);
			var node = this.pool.Locate(hashedKey);
			var result = this.MutateOperationResultFactory.Create();

			if (node != null)
			{
				var command = this.pool.OperationFactory.Mutate(mode, hashedKey, defaultValue, delta, expires, cas);
				var commandResult = node.Execute(command);

				result.Cas = cas = command.CasValue;
				result.StatusCode = command.StatusCode;

				if (commandResult.Success)
				{
					result.Value = command.Result;
					result.Pass();
					return result;
				}
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Mutate operation failed, see InnerResult or StatusCode for more details");
				}
			}

			// TODO: not sure about the return value when the command fails
			result.Fail("Unable to locate node");
			return result;
		}
		#endregion

		#region Concatenate
		/// <summary>
		/// Appends the data to the end of the specified item's data on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="data">The data to be appended to the item.</param>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public bool Append(string key, ArraySegment<byte> data)
		{
			ulong cas = 0;
			return this.PerformConcatenate(ConcatenationMode.Append, key, ref cas, data).Success;
		}

		/// <summary>
		/// Appends the data to the end of the specified item's data on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="data">The data to be appended to the item.</param>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public async Task<bool> AppendAsync(string key, ArraySegment<byte> data)
		{
			return (await this.PerformConcatenateAsync(ConcatenationMode.Append, key, data)).Success;
		}

		/// <summary>
		/// Inserts the data before the specified item's data on the server.
		/// </summary>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public bool Prepend(string key, ArraySegment<byte> data)
		{
			ulong cas = 0;
			return this.PerformConcatenate(ConcatenationMode.Prepend, key, ref cas, data).Success;
		}

		/// <summary>
		/// Inserts the data before the specified item's data on the server.
		/// </summary>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public async Task<bool> PrependAsync(string key, ArraySegment<byte> data)
		{
			return (await this.PerformConcatenateAsync(ConcatenationMode.Prepend, key, data)).Success;
		}

		/// <summary>
		/// Appends the data to the end of the specified item's data on the server, but only if the item's version matches the CAS value provided.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <param name="data">The data to be prepended to the item.</param>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public CasResult<bool> Append(string key, ulong cas, ArraySegment<byte> data)
		{
			ulong tmp = cas;
			var success = this.PerformConcatenate(ConcatenationMode.Append, key, ref tmp, data);
			return new CasResult<bool> { Cas = tmp, Result = success.Success };
		}

		/// <summary>
		/// Inserts the data before the specified item's data on the server, but only if the item's version matches the CAS value provided.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <param name="data">The data to be prepended to the item.</param>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public CasResult<bool> Prepend(string key, ulong cas, ArraySegment<byte> data)
		{
			ulong tmp = cas;
			var success = PerformConcatenate(ConcatenationMode.Prepend, key, ref tmp, data);
			return new CasResult<bool> { Cas = tmp, Result = success.Success };
		}

		protected virtual IConcatOperationResult PerformConcatenate(ConcatenationMode mode, string key, ref ulong cas, ArraySegment<byte> data)
		{
			var hashedKey = this.keyTransformer.Transform(key);
			var node = this.pool.Locate(hashedKey);
			var result = this.ConcatOperationResultFactory.Create();

			if (node != null)
			{
				var command = this.pool.OperationFactory.Concat(mode, hashedKey, cas, data);
				var commandResult = node.Execute(command);

				if (commandResult.Success)
				{
					result.Cas = cas = command.CasValue;
					result.StatusCode = command.StatusCode;
					result.Pass();
				}
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Concat operation failed, see InnerResult or StatusCode for details");
				}

				return result;
			}

			result.Fail("Unable to locate node");
			return result;
		}

		protected virtual async Task<IConcatOperationResult> PerformConcatenateAsync(ConcatenationMode mode, string key, ArraySegment<byte> data)
		{
			var hashedKey = this.keyTransformer.Transform(key);
			var node = this.pool.Locate(hashedKey);
			var result = this.ConcatOperationResultFactory.Create();
			ulong cas = 0;
			if (node != null)
			{
				var command = this.pool.OperationFactory.Concat(mode, hashedKey, cas, data);
				var commandResult = await node.ExecuteAsync(command);

				if (commandResult.Success)
				{
					result.Cas = cas = command.CasValue;
					result.StatusCode = command.StatusCode;
					result.Pass();
				}
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Concat operation failed, see InnerResult or StatusCode for details");
				}

				return result;
			}

			result.Fail("Unable to locate node");
			return result;
		}
		#endregion

		#region Get
		/// <summary>
		/// Retrieves the specified item from the cache.
		/// </summary>
		/// <param name="key">The identifier for the item to retrieve.</param>
		/// <returns>The retrieved item, or <value>null</value> if the key was not found.</returns>
		public object Get(string key)
		{
			return this.PerformTryGet(key, out ulong cas, out object value).Success ? value : null;
		}

		/// <summary>
		/// Retrieves the specified item from the cache.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="key">The identifier for the item to retrieve.</param>
		/// <returns>The retrieved item, or <value>default(T)</value> if the key was not found.</returns>
		public T Get<T>(string key)
		{
			var value = this.Get(key);
			return value == null
				? default(T)
				: typeof(T) == typeof(Guid) && value is string
					? (T)(object)new Guid(value as string)
					: value is T
						? (T)value
						: default(T);
		}

		/// <summary>
		/// Retrieves the specified item from the cache.
		/// </summary>
		/// <param name="key">The identifier for the item to retrieve.</param>
		/// <returns>The retrieved item, or <value>null</value> if the key was not found.</returns>
		public async Task<object> GetAsync(string key)
		{
			var result = await this.PerformTryGetAsync(key);
			return result.Success ? result.Value : null;
		}

		/// <summary>
		/// Retrieves the specified item from the cache.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="key">The identifier for the item to retrieve.</param>
		/// <returns>The retrieved item, or <value>null</value> if the key was not found.</returns>
		public async Task<T> GetAsync<T>(string key)
		{
			var value = await this.GetAsync(key);
			return value == null
				? default(T)
				: typeof(T) == typeof(Guid) && value is string
					? (T)(object)new Guid(value as string)
					: value is T
						? (T)value
						: default(T);
		}

		public async Task<IGetOperationResult<T>> DoGetAsync<T>(string key)
		{
			var result = new DefaultGetOperationResultFactory<T>().Create();

			var hashedKey = this.keyTransformer.Transform(key);
			var node = this.pool.Locate(hashedKey);

			if (node != null)
			{
				try
				{
					var command = this.pool.OperationFactory.Get(hashedKey);
					var commandResult = await node.ExecuteAsync(command);

					if (commandResult.Success)
					{
						if (typeof(T).GetTypeCode() == TypeCode.Object && typeof(T) != typeof(Byte[]))
						{
							result.Success = true;
							result.Value = this.transcoder.Deserialize<T>(command.Result);
							return result;
						}
						else
						{
							var tempResult = this.transcoder.Deserialize(command.Result);
							if (tempResult != null)
							{
								result.Success = true;
								if (typeof(T) == typeof(Guid))
								{
									result.Value = (T)(object)new Guid((string)tempResult);
								}
								else
								{
									result.Value = (T)tempResult;
								}
								return result;
							}
						}
					}
				}
				catch (Exception ex)
				{
					this._logger.LogError(0, ex, $"{nameof(this.DoGetAsync)}(\"{key}\")");
					throw ex;
				}
			}
			else
			{
				this._logger.LogError($"Unable to locate memcached node");
			}

			result.Success = false;
			result.Value = default(T);
			return result;
		}

		/// <summary>
		/// Tries to get an item from the cache.
		/// </summary>
		/// <param name="key">The identifier for the item to retrieve.</param>
		/// <param name="value">The retrieved item or null if not found.</param>
		/// <returns>The <value>true</value> if the item was successfully retrieved.</returns>
		public bool TryGet(string key, out object value)
		{
			return this.PerformTryGet(key, out ulong cas, out value).Success;
		}

		public CasResult<object> GetWithCas(string key)
		{
			return this.GetWithCas<object>(key);
		}

		public CasResult<T> GetWithCas<T>(string key)
		{
			return this.TryGetWithCas(key, out CasResult<object> tmp)
				? new CasResult<T> { Cas = tmp.Cas, Result = (T)tmp.Result }
				: new CasResult<T> { Cas = tmp.Cas, Result = default(T) };
		}

		public bool TryGetWithCas(string key, out CasResult<object> value)
		{
			var retval = this.PerformTryGet(key, out ulong cas, out object tmp);
			value = new CasResult<object> { Cas = cas, Result = tmp };
			return retval.Success;
		}

		protected virtual IGetOperationResult PerformTryGet(string key, out ulong cas, out object value)
		{
			var hashedKey = this.keyTransformer.Transform(key);
			var node = this.pool.Locate(hashedKey);
			var result = this.GetOperationResultFactory.Create();

			cas = 0;
			value = null;

			if (node != null)
			{
				var command = this.pool.OperationFactory.Get(hashedKey);
				var commandResult = node.Execute(command);

				if (commandResult.Success)
				{
					result.Value = value = this.transcoder.Deserialize(command.Result);
					result.Cas = cas = command.CasValue;

					result.Pass();
					return result;
				}
				else
				{
					commandResult.Combine(result);
					return result;
				}
			}

			result.Value = value;
			result.Cas = cas;

			result.Fail("Unable to locate node");
			return result;
		}

		protected virtual async Task<IGetOperationResult> PerformTryGetAsync(string key)
		{
			var hashedKey = this.keyTransformer.Transform(key);
			var node = this.pool.Locate(hashedKey);
			var result = this.GetOperationResultFactory.Create();

			ulong cas = 0;
			object value = null;

			if (node != null)
			{
				var command = this.pool.OperationFactory.Get(hashedKey);
				var commandResult = await node.ExecuteAsync(command);

				if (commandResult.Success)
				{
					result.Value = value = this.transcoder.Deserialize(command.Result);
					result.Cas = cas = command.CasValue;

					result.Pass();
					return result;
				}
				else
				{
					commandResult.Combine(result);
					return result;
				}
			}

			result.Value = value;
			result.Cas = cas;

			result.Fail("Unable to locate node");
			return result;
		}
		#endregion

		#region Multi Get
		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public IDictionary<string, object> Get(IEnumerable<string> keys)
		{
			return this.PerformMultiGet<object>(keys, (mget, kvp) => this.transcoder.Deserialize(kvp.Value));
		}

		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public Task<IDictionary<string, object>> GetAsync(IEnumerable<string> keys)
		{
			var tcs = new TaskCompletionSource<IDictionary<string, object>>();
			ThreadPool.QueueUserWorkItem(_ =>
			{
				try
				{
					var result = this.Get(keys);
					tcs.SetResult(result);
				}
				catch (Exception ex)
				{
					tcs.SetException(ex);
				}
			});
			return tcs.Task;
		}

		public IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys)
		{
			return this.PerformMultiGet<CasResult<object>>(keys, (mget, kvp) => new CasResult<object>
			{
				Result = this.transcoder.Deserialize(kvp.Value),
				Cas = mget.Cas[kvp.Key]
			});
		}

		protected virtual IDictionary<string, T> PerformMultiGet<T>(IEnumerable<string> keys, Func<IMultiGetOperation, KeyValuePair<string, CacheItem>, T> collector)
		{
			// transform the keys and index them by hashed => original
			// the multi-get results will be mapped using this index
			var hashed = keys.ToDictionary(key => this.keyTransformer.Transform(key), key => key);
			var byServer = this.GroupByServer(hashed.Keys);

			var retval = new Dictionary<string, T>(hashed.Count);
			var tasks = new List<Task>();

			// execute each list of keys on their respective node
			foreach (var slice in byServer)
			{
				var node = slice.Key;

				var nodeKeys = slice.Value;
				var mget = this.pool.OperationFactory.MultiGet(nodeKeys);

				// run gets in parallel
				var action = new Func<IOperation, IOperationResult>(node.Execute);

				// execute the multi-gets in parallel
				tasks.Add(Task.Run(() =>
				{
					try
					{
						if (action(mget).Success)
						{
							// deserialize the items in the dictionary
							foreach (var kvp in mget.Result)
							{
								if (hashed.TryGetValue(kvp.Key, out string original))
								{
									var result = collector(mget, kvp);

									// the lock will serialize the merge,
									// but at least the commands were not waiting on each other
									lock (retval)
										retval[original] = result;
								}
							}
						}
					}
					catch (Exception e)
					{
						this._logger.LogError("PerformMultiGet", e);
					}
				}));
			}

			// wait for all nodes to finish
			if (tasks.Count > 0)
			{
				Task.WaitAll(tasks.ToArray());
			}

			return retval;
		}

		protected Dictionary<IMemcachedNode, IList<string>> GroupByServer(IEnumerable<string> keys)
		{
			var retval = new Dictionary<IMemcachedNode, IList<string>>();

			foreach (var k in keys)
			{
				var node = this.pool.Locate(k);
				if (node == null)
					continue;

				if (!retval.TryGetValue(node, out IList<string> list))
					retval[node] = list = new List<string>(4);

				list.Add(k);
			}

			return retval;
		}
		#endregion

		#region Exists
		/// <summary>
		/// Determines whether an item that associated with the key is exists or not
		/// </summary>
		/// <param name="key">The key</param>
		/// <returns>Returns a boolean value indicating if the object that associates with the key is cached or not</returns>
		public bool Exists(string key)
		{
			if (!this.Append(key, new ArraySegment<byte>(new byte[0])))
			{
				this.Remove(key);
				return false;
			}
			return true;
		}

		/// <summary>
		/// Determines whether an item that associated with the key is exists or not
		/// </summary>
		/// <param name="key">The key</param>
		/// <returns>Returns a boolean value indicating if the object that associates with the key is cached or not</returns>
		public async Task<bool> ExistsAsync(string key)
		{
			if (!await this.AppendAsync(key, new ArraySegment<byte>(new byte[0])))
			{
				await this.RemoveAsync(key);
				return false;
			}
			return true;
		}
		#endregion

		#region Remove
		/// <summary>
		/// Removes the specified item from the cache.
		/// </summary>
		/// <param name="key">The identifier for the item to delete.</param>
		/// <returns>true if the item was successfully removed from the cache; false otherwise.</returns>
		public bool Remove(string key)
		{
			return this.ExecuteRemove(key).Success;
		}

		/// <summary>
		/// Removes the specified item from the cache.
		/// </summary>
		/// <param name="key">The identifier for the item to delete.</param>
		/// <returns>true if the item was successfully removed from the cache; false otherwise.</returns>
		public async Task<bool> RemoveAsync(string key)
		{
			return (await this.ExecuteRemoveAsync(key)).Success;
		}

		/// <summary>
		/// Removes all data from the cache. Note: this will invalidate all data on all servers in the pool.
		/// </summary>
		public void FlushAll()
		{
			foreach (var node in this.pool.GetWorkingNodes())
			{
				var command = this.pool.OperationFactory.Flush();
				node.Execute(command);
			}
		}

		/// <summary>
		/// Removes all data from the cache. Note: this will invalidate all data on all servers in the pool.
		/// </summary>
		public Task FlushAllAsync()
		{
			var tasks = new List<Task>();
			foreach (var node in this.pool.GetWorkingNodes())
			{
				var command = this.pool.OperationFactory.Flush();
				tasks.Add(node.ExecuteAsync(command));
			}
			return Task.WhenAll(tasks);
		}
		#endregion

		#region Stats
		/// <summary>
		/// Returns statistics about the servers.
		/// </summary>
		/// <returns></returns>
		public ServerStats Stats()
		{
			return this.Stats(null);
		}

		public ServerStats Stats(string type)
		{
			var results = new Dictionary<EndPoint, Dictionary<string, string>>();
			var tasks = new List<Task>();

			foreach (var node in this.pool.GetWorkingNodes())
			{
				var cmd = this.pool.OperationFactory.Stats(type);
				var action = new Func<IOperation, IOperationResult>(node.Execute);
				var endpoint = node.EndPoint;

				tasks.Add(Task.Run(() =>
				{
					action(cmd);
					lock (results)
						results[endpoint] = cmd.Result;
				}));
			}

			if (tasks.Count > 0)
			{
				Task.WaitAll(tasks.ToArray());
			}

			return new ServerStats(results);
		}
		#endregion

		#region Dispose
		~MemcachedClient()
		{
			try
			{
				((IDisposable)this).Dispose();
			}
			catch { }
		}

		void IDisposable.Dispose()
		{
			this.Dispose();
		}

		/// <summary>
		/// Releases all resources allocated by this instance
		/// </summary>
		/// <remarks>You should only call this when you are not using static instances of the client, so it can close all conections and release the sockets.</remarks>
		public void Dispose()
		{
			GC.SuppressFinalize(this);
			if (this.pool != null)
			{
				try
				{
					this.pool.Dispose();
				}
				catch { }
				finally
				{
					this.pool = null;
				}
			}
		}

		/// <summary>
		/// Waits for all WaitHandles and works in both STA and MTA mode.
		/// </summary>
		/// <param name="waitHandles"></param>
		private static void SafeWaitAllAndDispose(WaitHandle[] waitHandles)
		{
			try
			{
				if (Thread.CurrentThread.GetApartmentState() == ApartmentState.MTA)
					WaitHandle.WaitAll(waitHandles);

				else
					for (var index = 0; index < waitHandles.Length; index++)
						waitHandles[index].WaitOne();
			}
			catch { }
			finally
			{
				for (var index = 0; index < waitHandles.Length; index++)
					waitHandles[index].Dispose();
			}
		}
		#endregion

		#region Implement IDistributedCache
		protected const int MaxSeconds = 60 * 60 * 24 * 30;
		protected static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1);

		protected static uint GetExpiration(TimeSpan? validFor, DateTime? expiresAt = null, DateTimeOffset? absoluteExpiration = null, TimeSpan? relativeToNow = null)
		{
			if (validFor != null && expiresAt != null)
				throw new ArgumentException("You cannot specify both validFor and expiresAt.");

			if (validFor == null && expiresAt == null && absoluteExpiration == null && relativeToNow == null)
			{
				return 0;
			}

			if (absoluteExpiration != null)
			{
				return (uint)absoluteExpiration.Value.ToUnixTimeSeconds();
			}

			if (relativeToNow != null)
			{
				return (uint)(DateTimeOffset.UtcNow + relativeToNow.Value).ToUnixTimeSeconds();
			}

			// convert timespans to absolute dates
			if (validFor != null)
			{
				// infinity
				if (validFor == TimeSpan.Zero || validFor == TimeSpan.MaxValue) return 0;

				expiresAt = DateTime.Now.Add(validFor.Value);
			}

			DateTime dt = expiresAt.Value;

			if (dt < UnixEpoch) throw new ArgumentOutOfRangeException("expiresAt", "expiresAt must be >= 1970/1/1");

			// accept MaxValue as infinite
			if (dt == DateTime.MaxValue) return 0;

			uint retval = (uint)(dt.ToUniversalTime() - UnixEpoch).TotalSeconds;

			return retval;
		}

		protected static string GetExpiratonKey(string key)
		{
			return key + "-" + nameof(DistributedCacheEntryOptions);
		}

		byte[] IDistributedCache.Get(string key)
		{
			this._logger.LogInformation($"{nameof(IDistributedCache.Get)}(\"{key}\")");
			return this.Get<byte[]>(key);
		}

		async Task<byte[]> IDistributedCache.GetAsync(string key, CancellationToken token = default(CancellationToken))
		{
			this._logger.LogInformation($"{nameof(IDistributedCache.GetAsync)}(\"{key}\")");
			return await this.GetAsync<byte[]>(key);
		}

		void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
		{
			this._logger.LogInformation($"{nameof(IDistributedCache.Set)}(\"{key}\")");

			ulong cas = 0;
			var expires = MemcachedClient.GetExpiration(options.SlidingExpiration, null, options.AbsoluteExpiration, options.AbsoluteExpirationRelativeToNow);
			this.PerformStore(StoreMode.Set, key, value, expires, cas);
			if (expires > 0)
			{
				this.PerformStore(StoreMode.Set, MemcachedClient.GetExpiratonKey(key), expires, expires, cas);
			}
		}

		async Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken))
		{
			this._logger.LogInformation($"{nameof(IDistributedCache.SetAsync)}(\"{key}\")");

			var expires = MemcachedClient.GetExpiration(options.SlidingExpiration, null, options.AbsoluteExpiration, options.AbsoluteExpirationRelativeToNow);
			await this.PerformStoreAsync(StoreMode.Set, key, value, expires);
			if (expires > 0)
			{
				await this.PerformStoreAsync(StoreMode.Set, MemcachedClient.GetExpiratonKey(key), expires, expires);
			}
		}

		void IDistributedCache.Refresh(string key)
		{
			this._logger.LogInformation($"{nameof(IDistributedCache.Refresh)}(\"{key}\")");

			var value = Get(key);
			if (value != null)
			{
				var expirationValue = this.Get(GetExpiratonKey(key));
				if (expirationValue != null)
				{
					ulong cas = 0;
					this.PerformStore(StoreMode.Replace, key, value, uint.Parse(expirationValue.ToString()), cas);
				}
			}
		}

		async Task IDistributedCache.RefreshAsync(string key, CancellationToken token = default(CancellationToken))
		{
			this._logger.LogInformation($"{nameof(IDistributedCache.RefreshAsync)}(\"{key}\")");

			var result = await this.DoGetAsync<byte[]>(key);
			if (result.Success)
			{
				var expirationResult = await this.DoGetAsync<uint>(MemcachedClient.GetExpiratonKey(key));
				if (expirationResult.Success)
				{
					await this.PerformStoreAsync(StoreMode.Replace, key, result.Value, expirationResult.Value);
				}
			}
		}

		void IDistributedCache.Remove(string key)
		{
			this.Remove(key);
		}

		async Task IDistributedCache.RemoveAsync(string key, CancellationToken token = default(CancellationToken))
		{
			await this.RemoveAsync(key);
		}
		#endregion

	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk? enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
