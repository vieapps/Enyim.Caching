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
using Microsoft.Extensions.DependencyInjection;

using CacheUtils;

namespace Enyim.Caching
{
	/// <summary>
	/// Memcached client.
	/// </summary>
	public partial class MemcachedClient : IMemcachedClient, IMemcachedResultsClient, IDistributedCache
	{

		#region Attributes
		ILogger<MemcachedClient> _logger;

		IServerPool _serverPool;
		IMemcachedKeyTransformer _keyTransformer;
		ITranscoder _transcoder;

		public IStoreOperationResultFactory StoreOperationResultFactory { get; set; }

		public IGetOperationResultFactory GetOperationResultFactory { get; set; }

		public IMutateOperationResultFactory MutateOperationResultFactory { get; set; }

		public IConcatOperationResultFactory ConcatOperationResultFactory { get; set; }

		public IRemoveOperationResultFactory RemoveOperationResultFactory { get; set; }

		protected IServerPool Pool { get { return this._serverPool; } }

		protected IMemcachedKeyTransformer KeyTransformer { get { return this._keyTransformer; } }

		protected ITranscoder Transcoder { get { return this._transcoder; } }

		public event Action<IMemcachedNode> NodeFailed;
		#endregion

		/// <summary>
		/// Initializes new instance of memcached client using configuration section of appsettings.json file
		/// </summary>
		/// <param name="loggerFactory"></param>
		/// <param name="configuration"></param>
		public MemcachedClient(ILoggerFactory loggerFactory, IMemcachedClientConfiguration configuration)
		{
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			this.Prepare(loggerFactory ?? new NullLoggerFactory(), configuration);
		}

		/// <summary>
		/// Initializes new instance of memcached client using configuration section of app.config/web.config file
		/// </summary>
		/// <param name="configuration"></param>
		/// <param name="loggerFactory"></param>
		public MemcachedClient(MemcachedClientConfigurationSectionHandler configuration, ILoggerFactory loggerFactory = null)
		{
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			loggerFactory = loggerFactory ?? new NullLoggerFactory();
			this.Prepare(loggerFactory, new MemcachedClientConfiguration(loggerFactory, configuration));
		}

		#region Prepare
		void Prepare(ILoggerFactory loggerFactory, IMemcachedClientConfiguration configuration)
		{
			this._logger = loggerFactory.CreateLogger<MemcachedClient>();

			this._keyTransformer = configuration.CreateKeyTransformer() ?? new DefaultKeyTransformer();
			this._transcoder = configuration.CreateTranscoder() ?? new DefaultTranscoder();

			this._serverPool = configuration.CreatePool();
			this._serverPool.NodeFailed += (node) =>
			{
				this.NodeFailed?.Invoke(node);
			};
			this._serverPool.Start();

			this.StoreOperationResultFactory = new DefaultStoreOperationResultFactory();
			this.GetOperationResultFactory = new DefaultGetOperationResultFactory();
			this.MutateOperationResultFactory = new DefaultMutateOperationResultFactory();
			this.ConcatOperationResultFactory = new DefaultConcatOperationResultFactory();
			this.RemoveOperationResultFactory = new DefaultRemoveOperationResultFactory();

			this._logger.LogInformation("An instance of MemcachedClient was created successful");
		}

		static MemcachedClient _Instance = null;

		internal static MemcachedClient GetInstance(IServiceProvider svcProvider)
		{
			return MemcachedClient._Instance ?? (MemcachedClient._Instance = new MemcachedClient(svcProvider.GetService<ILoggerFactory>(), svcProvider.GetService<IMemcachedClientConfiguration>()));
		}
		#endregion

		#region Store
		protected virtual IStoreOperationResult PerformStore(StoreMode mode, string key, object value, uint expires, ref ulong cas, out int statusCode)
		{
			var result = this.StoreOperationResultFactory.Create();
			statusCode = -1;
			if (value == null)
			{
				result.Fail("value is null");
				return result;
			}

			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);
			if (node != null)
			{
				CacheItem item;
				try
				{
					item = this._transcoder.Serialize(value);
				}
				catch (ArgumentException)
				{
					throw;
				}
				catch (Exception ex)
				{
					this._logger.LogError(new EventId(), ex, $"{nameof(this.PerformStore)} for '{key}' key");
					result.Fail("PerformStore failed", ex);
					return result;
				}

				var command = this._serverPool.OperationFactory.Store(mode, hashedKey, item, expires, cas);
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

		IStoreOperationResult PerformStore(StoreMode mode, string key, object value, uint expires, ulong cas = 0)
		{
			ulong tmp = cas;
			var retval = this.PerformStore(mode, key, value, expires, ref tmp, out int status);
			retval.StatusCode = status;

			if (retval.Success)
				retval.Cas = tmp;
			return retval;
		}

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
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
		public bool Store(StoreMode mode, string key, object value, TimeSpan validFor)
		{
			ulong tmp = 0;
			return this.PerformStore(mode, key, value, validFor.GetExpiration(), ref tmp, out int status).Success;
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
			return this.PerformStore(mode, key, value, expiresAt.GetExpiration(), ref tmp, out int status).Success;
		}

		protected async virtual Task<IStoreOperationResult> PerformStoreAsync(StoreMode mode, string key, object value, uint expires, ulong cas = 0)
		{
			var result = this.StoreOperationResultFactory.Create();
			if (value == null)
			{
				result.Fail("value is null");
				return result;
			}

			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);

			if (node != null)
			{
				CacheItem item;
				try
				{
					item = this._transcoder.Serialize(value);
				}
				catch (ArgumentException)
				{
					throw;
				}
				catch (Exception ex)
				{
					this._logger.LogError(new EventId(), ex, $"{nameof(this.PerformStoreAsync)} for '{key}' key");
					result.Fail("PerformStoreAsync failed", ex);
					return result;
				}

				var command = this._serverPool.OperationFactory.Store(mode, hashedKey, item, expires, cas);
				var commandResult = await node.ExecuteAsync(command);

				result.Cas = command.CasValue;
				result.StatusCode = command.StatusCode;

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
		public async Task<bool> StoreAsync(StoreMode mode, string key, object value, TimeSpan validFor)
		{
			return (await this.PerformStoreAsync(mode, key, value, validFor.GetExpiration())).Success;
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
			return (await this.PerformStoreAsync(mode, key, value, expiresAt.GetExpiration())).Success;
		}
		#endregion

		#region Cas
		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location and returns its version.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
		/// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
		public CasResult<bool> Cas(StoreMode mode, string key, object value, ulong cas)
		{
			var result = this.PerformStore(mode, key, value, 0, cas);
			return new CasResult<bool>()
			{
				Cas = result.Cas,
				Result = result.Success,
				StatusCode = result.StatusCode.Value
			};
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
			return this.Cas(mode, key, value, 0);
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
			var result = this.PerformStore(mode, key, value, validFor.GetExpiration(), cas);
			return new CasResult<bool>()
			{
				Cas = result.Cas,
				Result = result.Success,
				StatusCode = result.StatusCode.Value
			};
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
			var result = this.PerformStore(mode, key, value, expiresAt.GetExpiration(), cas);
			return new CasResult<bool>()
			{
				Cas = result.Cas,
				Result = result.Success,
				StatusCode = result.StatusCode.Value
			};
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location and returns its version.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
		/// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
		public async Task<CasResult<bool>> CasAsync(StoreMode mode, string key, object value, ulong cas)
		{
			var result = await this.PerformStoreAsync(mode, key, value, 0, cas);
			return new CasResult<bool>()
			{
				Cas = result.Cas,
				Result = result.Success,
				StatusCode = result.StatusCode.Value
			};
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location and returns its version.
		/// </summary>
		/// <param name="mode">Defines how the item is stored in the cache.</param>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="value">The object to be inserted into the cache.</param>
		/// <remarks>The item does not expire unless it is removed due memory pressure. The text protocol does not support this operation, you need to Store then GetWithCas.</remarks>
		/// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
		public async Task<CasResult<bool>> CasAsync(StoreMode mode, string key, object value)
		{
			return await this.CasAsync(mode, key, value, 0);
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
		public async Task<CasResult<bool>> CasAsync(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas)
		{
			var result = await this.PerformStoreAsync(mode, key, value, validFor.GetExpiration(), cas);
			return new CasResult<bool>()
			{
				Cas = result.Cas,
				Result = result.Success,
				StatusCode = result.StatusCode.Value
			};
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
		public async Task<CasResult<bool>> CasAsync(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas)
		{
			var result = await this.PerformStoreAsync(mode, key, value, expiresAt.GetExpiration(), cas);
			return new CasResult<bool>()
			{
				Cas = result.Cas,
				Result = result.Success,
				StatusCode = result.StatusCode.Value
			};
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
		protected virtual IMutateOperationResult PerformMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas  = 0)
		{
			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);
			var result = this.MutateOperationResultFactory.Create();

			if (node != null)
			{
				var command = this._serverPool.OperationFactory.Mutate(mode, hashedKey, defaultValue, delta, expires, cas);
				var commandResult = node.Execute(command);

				result.Cas = command.CasValue;
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

		IMutateOperationResult CasMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas)
		{
			return this.PerformMutate(mode, key, defaultValue, delta, expires, cas);
		}

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
			return this.PerformMutate(MutationMode.Increment, key, defaultValue, delta, validFor.GetExpiration()).Value;
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
			return this.PerformMutate(MutationMode.Increment, key, defaultValue, delta, expiresAt.GetExpiration()).Value;
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
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
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
			var result = this.CasMutate(MutationMode.Increment, key, defaultValue, delta, validFor.GetExpiration(), cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
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
			var result = this.CasMutate(MutationMode.Increment, key, defaultValue, delta, expiresAt.GetExpiration(), cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
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
			return this.PerformMutate(MutationMode.Decrement, key, defaultValue, delta, validFor.GetExpiration()).Value;
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
			return this.PerformMutate(MutationMode.Decrement, key, defaultValue, delta, expiresAt.GetExpiration()).Value;
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
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
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
			var result = this.CasMutate(MutationMode.Decrement, key, defaultValue, delta, validFor.GetExpiration(), cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
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
			var result = this.CasMutate(MutationMode.Decrement, key, defaultValue, delta, expiresAt.GetExpiration(), cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
		}

		protected virtual async Task<IMutateOperationResult> PerformMutateAsync(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas = 0)
		{
			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);
			var result = this.MutateOperationResultFactory.Create();

			if (node != null)
			{
				var command = this._serverPool.OperationFactory.Mutate(mode, hashedKey, defaultValue, delta, expires, cas);
				var commandResult = await node.ExecuteAsync(command);

				result.Cas = command.CasValue;
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

		async Task<IMutateOperationResult> CasMutateAsync(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas)
		{
			return await this.PerformMutateAsync(mode, key, defaultValue, delta, expires, cas);
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<ulong> IncrementAsync(string key, ulong defaultValue, ulong delta)
		{
			return (await this.PerformMutateAsync(MutationMode.Increment, key, defaultValue, delta, 0)).Value;
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
		public async Task<ulong> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
		{
			return (await this.PerformMutateAsync(MutationMode.Increment, key, defaultValue, delta, validFor.GetExpiration())).Value;
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
		public async Task<ulong> IncrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
		{
			return (await this.PerformMutateAsync(MutationMode.Increment, key, defaultValue, delta, expiresAt.GetExpiration())).Value;
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
		public async Task<CasResult<ulong>> IncrementAsync(string key, ulong defaultValue, ulong delta, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Increment, key, defaultValue, delta, 0, cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
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
		public async Task<CasResult<ulong>> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Increment, key, defaultValue, delta, validFor.GetExpiration(), cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
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
		public async Task<CasResult<ulong>> IncrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Increment, key, defaultValue, delta, expiresAt.GetExpiration(), cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<ulong> DecrementAsync(string key, ulong defaultValue, ulong delta)
		{
			return (await this.PerformMutateAsync(MutationMode.Decrement, key, defaultValue, delta, 0)).Value;
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
		public async Task<ulong> DecrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
		{
			return (await this.PerformMutateAsync(MutationMode.Decrement, key, defaultValue, delta, validFor.GetExpiration())).Value;
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
		public async Task<ulong> DecrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
		{
			return (await this.PerformMutateAsync(MutationMode.Decrement, key, defaultValue, delta, expiresAt.GetExpiration())).Value;
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
		public async Task<CasResult<ulong>> DecrementAsync(string key, ulong defaultValue, ulong delta, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Decrement, key, defaultValue, delta, 0, cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
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
		public async Task<CasResult<ulong>> DecrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Decrement, key, defaultValue, delta, validFor.GetExpiration(), cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
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
		public async Task<CasResult<ulong>> DecrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Decrement, key, defaultValue, delta, expiresAt.GetExpiration(), cas);
			return new CasResult<ulong>()
			{
				Cas = result.Cas,
				Result = result.Value,
				StatusCode = result.StatusCode.Value
			};
		}
		#endregion

		#region Concatenate
		protected virtual IConcatOperationResult PerformConcatenate(ConcatenationMode mode, string key, ref ulong cas, ArraySegment<byte> data)
		{
			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);
			var result = this.ConcatOperationResultFactory.Create();

			if (node != null)
			{
				var command = this._serverPool.OperationFactory.Concat(mode, hashedKey, cas, data);
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
		/// Inserts the data before the specified item's data on the server.
		/// </summary>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public bool Prepend(string key, ArraySegment<byte> data)
		{
			ulong cas = 0;
			return this.PerformConcatenate(ConcatenationMode.Prepend, key, ref cas, data).Success;
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
			var result = this.PerformConcatenate(ConcatenationMode.Append, key, ref tmp, data);
			return new CasResult<bool>()
			{
				Cas = tmp,
				Result = result.Success
			};
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
			var result = this.PerformConcatenate(ConcatenationMode.Prepend, key, ref tmp, data);
			return new CasResult<bool>()
			{
				Cas = tmp,
				Result = result.Success
			};
		}

		protected virtual async Task<IConcatOperationResult> PerformConcatenateAsync(ConcatenationMode mode, string key, ArraySegment<byte> data, ulong cas = 0)
		{
			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);
			var result = this.ConcatOperationResultFactory.Create();

			if (node != null)
			{
				var command = this._serverPool.OperationFactory.Concat(mode, hashedKey, cas, data);
				var commandResult = await node.ExecuteAsync(command);

				if (commandResult.Success)
				{
					result.Cas = command.CasValue;
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
		/// Appends the data to the end of the specified item's data on the server, but only if the item's version matches the CAS value provided.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <param name="data">The data to be prepended to the item.</param>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public async Task<CasResult<bool>> AppendAsync(string key, ulong cas, ArraySegment<byte> data)
		{
			var result = await this.PerformConcatenateAsync(ConcatenationMode.Append, key, data, cas);
			return new CasResult<bool>()
			{
				Cas = result.Cas,
				Result = result.Success
			};
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
		/// Inserts the data before the specified item's data on the server, but only if the item's version matches the CAS value provided.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <param name="data">The data to be prepended to the item.</param>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public async Task<CasResult<bool>> PrependAsync(string key, ulong cas, ArraySegment<byte> data)
		{
			var result = await this.PerformConcatenateAsync(ConcatenationMode.Prepend, key, data, cas);
			return new CasResult<bool>()
			{
				Cas = result.Cas,
				Result = result.Success
			};
		}
		#endregion

		#region Get
		protected virtual IGetOperationResult PerformTryGet(string key, out ulong cas, out object value)
		{
			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);
			var result = this.GetOperationResultFactory.Create();

			cas = 0;
			value = null;

			if (node != null)
			{
				var command = this._serverPool.OperationFactory.Get(hashedKey);
				var commandResult = node.Execute(command);

				if (commandResult.Success)
				{
					result.Value = value = this._transcoder.Deserialize(command.Result);
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
		/// Tries to get an item from the cache with CAS.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public bool TryGetWithCas(string key, out CasResult<object> value)
		{
			var result = this.PerformTryGet(key, out ulong cas, out object tmp);
			value = new CasResult<object>()
			{
				Cas = cas,
				Result = tmp
			};
			return result.Success;
		}

		/// <summary>
		/// Retrieves the specified item from the cache.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="key"></param>
		/// <returns></returns>
		public CasResult<T> GetWithCas<T>(string key)
		{
			return this.TryGetWithCas(key, out CasResult<object> tmp)
				? new CasResult<T>()
				{
					Cas = tmp.Cas,
					Result = (T)tmp.Result
				}
				: new CasResult<T>
					{
						Cas = tmp.Cas,
						Result = default(T)
					};
		}

		/// <summary>
		/// Retrieves the specified item from the cache.
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		public CasResult<object> GetWithCas(string key)
		{
			return this.GetWithCas<object>(key);
		}

		protected virtual async Task<IGetOperationResult> PerformTryGetAsync(string key)
		{
			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);
			var result = this.GetOperationResultFactory.Create();

			if (node != null)
			{
				var command = this._serverPool.OperationFactory.Get(hashedKey);
				var commandResult = await node.ExecuteAsync(command);

				if (commandResult.Success)
				{
					result.Value = this._transcoder.Deserialize(command.Result);
					result.Cas = command.CasValue;

					result.Pass();
					return result;
				}
				else
				{
					commandResult.Combine(result);
					return result;
				}
			}

			result.Value = null;
			result.Cas = 0;

			result.Fail("Unable to locate node");
			return result;
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

		/// <summary>
		/// Retrieves the specified item from the cache.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="key"></param>
		/// <returns></returns>
		public async Task<CasResult<T>> GetWithCasAsync<T>(string key)
		{
			var result = await this.PerformTryGetAsync(key);
			return result.Success
				? new CasResult<T>()
				{
					Cas = result.Cas,
					Result = (T)result.Value
				}
				: new CasResult<T>
				{
					Cas = result.Cas,
					Result = default(T)
				};
		}

		/// <summary>
		/// Retrieves the specified item from the cache.
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		public Task<CasResult<object>> GetWithCasAsync(string key)
		{
			return this.GetWithCasAsync<object>(key);
		}
		#endregion

		#region Multi Get
		protected Dictionary<IMemcachedNode, IList<string>> GroupByServer(IEnumerable<string> keys)
		{
			var results = new Dictionary<IMemcachedNode, IList<string>>();

			foreach (var key in keys)
			{
				var node = this._serverPool.Locate(key);
				if (node == null)
					continue;

				if (!results.TryGetValue(node, out IList<string> list))
					results[node] = list = new List<string>(4);

				list.Add(key);
			}

			return results;
		}

		protected virtual IDictionary<string, T> PerformMultiGet<T>(IEnumerable<string> keys, Func<IMultiGetOperation, KeyValuePair<string, CacheItem>, T> collector)
		{
			// transform the keys and index them by hashed => original
			// the multi-get results will be mapped using this index
			var hashed = keys.ToDictionary(key => this._keyTransformer.Transform(key), key => key);
			var values = new Dictionary<string, T>(hashed.Count);

			// action to execute command in parallel
			Func<IMemcachedNode, IList<string>, Task> actionAsync = (node, nodeKeys) =>
			{
				return Task.Run(() =>
				{
					try
					{
						// execute command
						var command = this._serverPool.OperationFactory.MultiGet(nodeKeys);
						var commandResult = node.Execute(command);

						// deserialize the items in the dictionary
						if (commandResult.Success)
							foreach (var kvp in command.Result)
								if (hashed.TryGetValue(kvp.Key, out string original))
								{
									var result = collector(command, kvp);

									// the lock will serialize the merge, but at least the commands were not waiting on each other
									lock (values)
										values[original] = result;
								}
					}
					catch (Exception ex)
					{
						this._logger.LogError("PerformMultiGet", ex);
					}
				});
			};

			// execute each list of keys on their respective node (in parallel)
			var tasks = this.GroupByServer(hashed.Keys)
				.Select(slice => actionAsync(slice.Key, slice.Value))
				.ToList();

			// wait for all nodes to finish
			if (tasks.Count > 0)
				Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(13));

			return values;
		}

		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public IDictionary<string, object> Get(IEnumerable<string> keys)
		{
			return this.PerformMultiGet<object>(keys, (mget, kvp) => this._transcoder.Deserialize(kvp.Value));
		}

		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public IDictionary<string, T> Get<T>(IEnumerable<string> keys)
		{
			return this.PerformMultiGet<T>(keys, (mget, kvp) => this._transcoder.Deserialize<T>(kvp.Value));
		}

		/// <summary>
		/// Retrieves multiple items from the cache with CAS.
		/// </summary>
		/// <param name="keys"></param>
		/// <returns></returns>
		public IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys)
		{
			return this.PerformMultiGet<CasResult<object>>(keys, (mget, kvp) => new CasResult<object>
			{
				Result = this._transcoder.Deserialize(kvp.Value),
				Cas = mget.Cas[kvp.Key]
			});
		}

		protected virtual async Task<IDictionary<string, T>> PerformMultiGetAsync<T>(IEnumerable<string> keys, Func<IMultiGetOperation, KeyValuePair<string, CacheItem>, T> collector)
		{
			// transform the keys and index them by hashed => original
			// the multi-get results will be mapped using this index
			var hashed = keys.ToDictionary(key => this._keyTransformer.Transform(key), key => key);
			var values = new Dictionary<string, T>(hashed.Count);

			// action to execute command in parallel
			Func<IMemcachedNode, IList<string>, Task> actionAsync = async (node, nodeKeys) =>
			{
				try
				{
					// execute command
					var command = this._serverPool.OperationFactory.MultiGet(nodeKeys);
					var commandResult = await node.ExecuteAsync(command);

					// deserialize the items in the dictionary
					if (commandResult.Success)
						foreach (var kvp in command.Result)
							if (hashed.TryGetValue(kvp.Key, out string original))
							{
								var result = collector(command, kvp);

								// the lock will serialize the merge, but at least the commands were not waiting on each other
								lock (values)
									values[original] = result;
							}
				}
				catch (Exception ex)
				{
					this._logger.LogError("PerformMultiGetAsync", ex);
				}
			};

			// execute each list of keys on their respective node (in parallel)
			var tasks = this.GroupByServer(hashed.Keys)
				.Select(slice => actionAsync(slice.Key, slice.Value))
				.ToList();

			// wait for all nodes to finish
			if (tasks.Count > 0)
				await Task.WhenAll(tasks);

			return values;
		}

		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public Task<IDictionary<string, object>> GetAsync(IEnumerable<string> keys)
		{
			return this.PerformMultiGetAsync<object>(keys, (mget, kvp) => this._transcoder.Deserialize(kvp.Value));
		}

		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public Task<IDictionary<string, T>> GetAsync<T>(IEnumerable<string> keys)
		{
			return this.PerformMultiGetAsync<T>(keys, (mget, kvp) => this._transcoder.Deserialize<T>(kvp.Value));
		}

		/// <summary>
		/// Retrieves multiple items from the cache with CAS.
		/// </summary>
		/// <param name="keys"></param>
		/// <returns></returns>
		public Task<IDictionary<string, CasResult<object>>> GetWithCasAsync(IEnumerable<string> keys)
		{
			return this.PerformMultiGetAsync<CasResult<object>>(keys, (mget, kvp) => new CasResult<object>
			{
				Result = this._transcoder.Deserialize(kvp.Value),
				Cas = mget.Cas[kvp.Key]
			});
		}
		#endregion

		#region Remove
		protected IRemoveOperationResult PerformRemove(string key)
		{
			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);
			var result = this.RemoveOperationResultFactory.Create();

			if (node != null)
			{
				var command = this._serverPool.OperationFactory.Delete(hashedKey, 0);
				var commandResult = node.Execute(command);

				if (commandResult.Success)
				{
					result.Pass();
				}
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Failed to remove item, see InnerResult or StatusCode for details");
				}

				return result;
			}

			result.Fail("Unable to locate node");
			return result;
		}

		/// <summary>
		/// Removes the specified item from the cache.
		/// </summary>
		/// <param name="key">The identifier for the item to delete.</param>
		/// <returns>true if the item was successfully removed from the cache; false otherwise.</returns>
		public bool Remove(string key)
		{
			return this.PerformRemove(key).Success;
		}

		protected async Task<IRemoveOperationResult> PerformRemoveAsync(string key)
		{
			var hashedKey = this._keyTransformer.Transform(key);
			var node = this._serverPool.Locate(hashedKey);
			var result = this.RemoveOperationResultFactory.Create();

			if (node != null)
			{
				var command = this._serverPool.OperationFactory.Delete(hashedKey, 0);
				var commandResult = await node.ExecuteAsync(command);

				if (commandResult.Success)
				{
					result.Pass();
				}
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Failed to remove item, see InnerResult or StatusCode for details");
				}

				return result;
			}

			result.Fail("Unable to locate memcached node");
			return result;
		}

		/// <summary>
		/// Removes the specified item from the cache.
		/// </summary>
		/// <param name="key">The identifier for the item to delete.</param>
		/// <returns>true if the item was successfully removed from the cache; false otherwise.</returns>
		public async Task<bool> RemoveAsync(string key)
		{
			return (await this.PerformRemoveAsync(key)).Success;
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

		#region Flush
		/// <summary>
		/// Removes all data from the cache. Note: this will invalidate all data on all servers in the pool.
		/// </summary>
		public void FlushAll()
		{
			foreach (var node in this._serverPool.GetWorkingNodes())
				node.Execute(this._serverPool.OperationFactory.Flush());
		}

		/// <summary>
		/// Removes all data from the cache. Note: this will invalidate all data on all servers in the pool.
		/// </summary>
		public Task FlushAllAsync()
		{
			var tasks = this._serverPool.GetWorkingNodes().Select(node => node.ExecuteAsync(this._serverPool.OperationFactory.Flush()));
			return Task.WhenAll(tasks);
		}
		#endregion

		#region Stats
		/// <summary>
		/// Gets statistics about the servers.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public ServerStats Stats(string type)
		{
			var results = new Dictionary<EndPoint, Dictionary<string, string>>();

			Func<IMemcachedNode, IStatsOperation, EndPoint, Task> actionAsync = (node, command, endpoint) =>
			{
				return Task.Run(() =>
				{
					node.Execute(command);
					lock (results)
						results[endpoint] = command.Result;
				});
			};

			var tasks = this._serverPool.GetWorkingNodes().Select(node => actionAsync(node, this._serverPool.OperationFactory.Stats(type), node.EndPoint)).ToArray();
			if (tasks.Length > 0)
				Task.WaitAll(tasks, TimeSpan.FromSeconds(13));

			return new ServerStats(results);
		}

		/// <summary>
		/// Gets statistics about the servers.
		/// </summary>
		/// <returns></returns>
		public ServerStats Stats()
		{
			return this.Stats(null);
		}

		/// <summary>
		/// Gets statistics about the servers.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public async Task<ServerStats> StatsAsync(string type)
		{
			var results = new Dictionary<EndPoint, Dictionary<string, string>>();

			Func<IMemcachedNode, IStatsOperation, EndPoint, Task> actionAsync = async (node, command, endpoint) =>
			{
				await node.ExecuteAsync(command);
				lock (results)
					results[endpoint] = command.Result;
			};

			var tasks = this._serverPool.GetWorkingNodes().Select(node => actionAsync(node, this._serverPool.OperationFactory.Stats(type), node.EndPoint)).ToList();
			if (tasks.Count > 0)
				await Task.WhenAll(tasks);

			return new ServerStats(results);
		}

		/// <summary>
		/// Gets statistics about the servers.
		/// </summary>
		/// <returns></returns>
		public Task<ServerStats> StatsAsync()
		{
			return this.StatsAsync(null);
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
			if (this._serverPool != null)
				try
				{
					this._serverPool.Dispose();
				}
				catch { }
				finally
				{
					this._serverPool = null;
				}
		}
		#endregion

		#region IDistributedCache 
		void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));

			var expires = options == null ? 0 : options.GetExpiration();
			this.PerformStore(StoreMode.Set, key, value, expires);
			if (expires > 0)
				this.PerformStore(StoreMode.Set, key.GetExpirationKey(), expires, expires);
		}

		Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));

			var expires = options == null ? 0 : options.GetExpiration();
			return Task.WhenAll(
				this.PerformStoreAsync(StoreMode.Set, key, value, expires),
				expires > 0 ? this.PerformStoreAsync(StoreMode.Set, key.GetExpirationKey(), expires, expires) : Task.CompletedTask
			);
		}

		byte[] IDistributedCache.Get(string key)
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));

			return this.Get<byte[]>(key);
		}

		Task<byte[]> IDistributedCache.GetAsync(string key, CancellationToken token = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));

			return this.GetAsync<byte[]>(key);
		}

		void IDistributedCache.Refresh(string key)
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));

			var value = this.Get<byte[]>(key);
			var expires = value != null ? this.Get<uint?>(key.GetExpirationKey()) : null;
			if (value != null && expires != null && expires.Value > 0)
			{
				this.PerformStore(StoreMode.Replace, key, value, expires.Value);
				this.PerformStore(StoreMode.Replace, key.GetExpirationKey(), expires.Value, expires.Value);
			}
		}

		async Task IDistributedCache.RefreshAsync(string key, CancellationToken token = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));

			var value = await this.GetAsync<byte[]>(key);
			var expires = value != null ? await this.GetAsync<uint?>(key.GetExpirationKey()) : null;
			if (value != null && expires != null && expires.Value > 0)
				await Task.WhenAll(
					this.PerformStoreAsync(StoreMode.Replace, key, value, expires.Value),
					this.PerformStoreAsync(StoreMode.Replace, key.GetExpirationKey(), expires.Value, expires.Value)
				);
		}

		void IDistributedCache.Remove(string key)
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));

			this.Remove(key);
			this.Remove(key.GetExpirationKey());
		}

		Task IDistributedCache.RemoveAsync(string key, CancellationToken token = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));

			return Task.WhenAll(
				this.RemoveAsync(key),
				this.RemoveAsync(key.GetExpirationKey())
			);
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
