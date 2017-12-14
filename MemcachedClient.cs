#region Related components
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

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;

using CacheUtils;
#endregion

namespace Enyim.Caching
{
	public partial class MemcachedClient : IMemcachedClient, IMemcachedResultsClient, IDistributedCache
	{

		#region Attributes
		ILogger _logger;

		IServerPool _serverPool;
		IKeyTransformer _keyTransformer;
		ITranscoder _transcoder;

		public IStoreOperationResultFactory StoreOperationResultFactory { get; set; }

		public IGetOperationResultFactory GetOperationResultFactory { get; set; }

		public IMutateOperationResultFactory MutateOperationResultFactory { get; set; }

		public IConcatOperationResultFactory ConcatOperationResultFactory { get; set; }

		public IRemoveOperationResultFactory RemoveOperationResultFactory { get; set; }

		protected IServerPool Pool { get { return this._serverPool; } }

		protected IKeyTransformer KeyTransformer { get { return this._keyTransformer; } }

		protected ITranscoder Transcoder { get { return this._transcoder; } }

		public event Action<IMemcachedNode> NodeFailed;
		#endregion

		/// <summary>
		/// Initializes a new instance of Memcached client (using configuration section of appsettings.json file)
		/// </summary>
		/// <param name="loggerFactory"></param>
		/// <param name="configuration"></param>
		public MemcachedClient(ILoggerFactory loggerFactory, IMemcachedClientConfiguration configuration)
		{
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			this.Prepare(loggerFactory, configuration);
		}

		/// <summary>
		/// Initializes a new instance of Memcached client (using configuration section of app.config/web.config file)
		/// </summary>
		/// <param name="configuration"></param>
		/// <param name="loggerFactory"></param>
		public MemcachedClient(MemcachedClientConfigurationSectionHandler configuration, ILoggerFactory loggerFactory = null)
		{
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			this.Prepare(loggerFactory, new MemcachedClientConfiguration(loggerFactory, configuration));
		}

		#region Prepare
		void Prepare(ILoggerFactory loggerFactory, IMemcachedClientConfiguration configuration)
		{
			Logger.AssignLoggerFactory(loggerFactory);
			this._logger = Logger.CreateLogger<MemcachedClient>();

			this._keyTransformer = configuration.CreateKeyTransformer() ?? new DefaultKeyTransformer();
			this._transcoder = configuration.CreateTranscoder() ?? new DefaultTranscoder();

			this._serverPool = configuration.CreatePool();
			this._serverPool.NodeFailed += node => this.NodeFailed?.Invoke(node);
			this._serverPool.Start();

			this.StoreOperationResultFactory = new DefaultStoreOperationResultFactory();
			this.GetOperationResultFactory = new DefaultGetOperationResultFactory();
			this.MutateOperationResultFactory = new DefaultMutateOperationResultFactory();
			this.ConcatOperationResultFactory = new DefaultConcatOperationResultFactory();
			this.RemoveOperationResultFactory = new DefaultRemoveOperationResultFactory();

			if (this._logger.IsEnabled(LogLevel.Debug))
				this._logger.LogInformation("An instance of Memcached client was created successful");
		}
		#endregion

		#region Get instance (singleton)
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
				this._logger.LogError("Value is null");
				result.Fail("Value is null");
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
				catch (Exception ex)
				{
					this._logger.LogError(ex, $"Cannot serialize the value of '{key}'");
					throw;
				}

				var command = this._serverPool.OperationFactory.Store(mode, hashedKey, item, expires, cas);
				var commandResult = node.Execute(command);

				result.Cas = cas = command.CasValue;
				result.StatusCode = statusCode = command.StatusCode;

				if (commandResult.Success)
					result.Pass();
				else
					commandResult.Combine(result);
				return result;
			}

			this._logger.LogError("Unable to locate node");
			result.Fail("Unable to locate node");
			return result;
		}

		IStoreOperationResult PerformStore(StoreMode mode, string key, object value, uint expires, ulong cas = 0)
		{
			ulong tmp = cas;
			return this.PerformStore(mode, key, value, expires, ref tmp, out int status);
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
				this._logger.LogError("Value is null");
				result.Fail("Value is null");
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
				catch (Exception ex)
				{
					this._logger.LogError(ex, $"Cannot serialize the value of '{key}'");
					throw;
				}

				var command = this._serverPool.OperationFactory.Store(mode, hashedKey, item, expires, cas);
				var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

				result.Cas = command.CasValue;
				result.StatusCode = command.StatusCode;

				if (commandResult.Success)
					result.Pass();
				else
					commandResult.Combine(result);
				return result;
			}

			this._logger.LogError("Unable to locate node");
			result.Fail("Unable to locate node");
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
			return (await this.PerformStoreAsync(mode, key, value, 0).ConfigureAwait(false)).Success;
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
			return (await this.PerformStoreAsync(mode, key, value, validFor.GetExpiration()).ConfigureAwait(false)).Success;
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
			return (await this.PerformStoreAsync(mode, key, value, expiresAt.GetExpiration()).ConfigureAwait(false)).Success;
		}
		#endregion

		#region CAS (Check And Set)
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
			var result = await this.PerformStoreAsync(mode, key, value, 0, cas).ConfigureAwait(false);
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
			return await this.CasAsync(mode, key, value, 0).ConfigureAwait(false);
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
			var result = await this.PerformStoreAsync(mode, key, value, validFor.GetExpiration(), cas).ConfigureAwait(false);
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
			var result = await this.PerformStoreAsync(mode, key, value, expiresAt.GetExpiration(), cas).ConfigureAwait(false);
			return new CasResult<bool>()
			{
				Cas = result.Cas,
				Result = result.Success,
				StatusCode = result.StatusCode.Value
			};
		}
		#endregion

		#region Set
		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="value"></param>
		/// <param name="cacheMinutes"></param>
		/// <returns>true if the item was successfully added in the cache; false otherwise.</returns>
		public bool Set(string key, object value, int cacheMinutes)
		{
			return this.Store(StoreMode.Set, key, value, cacheMinutes < 1 ? TimeSpan.Zero : TimeSpan.FromMinutes(cacheMinutes));
		}

		/// <summary>
		/// Inserts an item into the cache with a cache key to reference its location.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="value"></param>
		/// <param name="cacheMinutes"></param>
		/// <returns>true if the item was successfully added in the cache; false otherwise.</returns>
		public Task<bool> SetAsync(string key, object value, int cacheMinutes)
		{
			return this.StoreAsync(StoreMode.Set, key, value, cacheMinutes < 1 ? TimeSpan.Zero : TimeSpan.FromMinutes(cacheMinutes));
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
			return this.Store(StoreMode.Add, key, value, cacheMinutes < 1 ? TimeSpan.Zero : TimeSpan.FromMinutes(cacheMinutes));
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
			return this.StoreAsync(StoreMode.Add, key, value, cacheMinutes < 1 ? TimeSpan.Zero : TimeSpan.FromMinutes(cacheMinutes));
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
			return this.Store(StoreMode.Replace, key, value, cacheMinutes < 1 ? TimeSpan.Zero : TimeSpan.FromMinutes(cacheMinutes));
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
			return this.StoreAsync(StoreMode.Replace, key, value, cacheMinutes < 1 ? TimeSpan.Zero : TimeSpan.FromMinutes(cacheMinutes));
		}
		#endregion

		#region Mutate
		protected virtual IMutateOperationResult PerformMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas = 0)
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
				}
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Mutate operation failed, see InnerResult or StatusCode for more details");
				}
				return result;
			}

			this._logger.LogError("Unable to locate node");
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
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
				var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

				result.Cas = command.CasValue;
				result.StatusCode = command.StatusCode;

				if (commandResult.Success)
				{
					result.Value = command.Result;
					result.Pass();
				}
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Mutate operation failed, see InnerResult or StatusCode for more details");
				}
				return result;
			}

			this._logger.LogError("Unable to locate node");
			result.Fail("Unable to locate node");
			return result;
		}

		Task<IMutateOperationResult> CasMutateAsync(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas)
		{
			return this.PerformMutateAsync(mode, key, defaultValue, delta, expires, cas);
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<ulong> IncrementAsync(string key, ulong defaultValue, ulong delta)
		{
			return (await this.PerformMutateAsync(MutationMode.Increment, key, defaultValue, delta, 0).ConfigureAwait(false)).Value;
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<ulong> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
		{
			return (await this.PerformMutateAsync(MutationMode.Increment, key, defaultValue, delta, validFor.GetExpiration()).ConfigureAwait(false)).Value;
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<ulong> IncrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
		{
			return (await this.PerformMutateAsync(MutationMode.Increment, key, defaultValue, delta, expiresAt.GetExpiration()).ConfigureAwait(false)).Value;
		}

		/// <summary>
		/// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to increase the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<CasResult<ulong>> IncrementAsync(string key, ulong defaultValue, ulong delta, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Increment, key, defaultValue, delta, 0, cas).ConfigureAwait(false);
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<CasResult<ulong>> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Increment, key, defaultValue, delta, validFor.GetExpiration(), cas).ConfigureAwait(false);
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<CasResult<ulong>> IncrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Increment, key, defaultValue, delta, expiresAt.GetExpiration(), cas).ConfigureAwait(false);
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<ulong> DecrementAsync(string key, ulong defaultValue, ulong delta)
		{
			return (await this.PerformMutateAsync(MutationMode.Decrement, key, defaultValue, delta, 0).ConfigureAwait(false)).Value;
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <param name="validFor">The interval after the item is invalidated in the cache.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<ulong> DecrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
		{
			return (await this.PerformMutateAsync(MutationMode.Decrement, key, defaultValue, delta, validFor.GetExpiration()).ConfigureAwait(false)).Value;
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<ulong> DecrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
		{
			return (await this.PerformMutateAsync(MutationMode.Decrement, key, defaultValue, delta, expiresAt.GetExpiration()).ConfigureAwait(false)).Value;
		}

		/// <summary>
		/// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
		/// <param name="delta">The amount by which the client wants to decrease the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <returns>The new value of the item or defaultValue if the key was not found.</returns>
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<CasResult<ulong>> DecrementAsync(string key, ulong defaultValue, ulong delta, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Decrement, key, defaultValue, delta, 0, cas).ConfigureAwait(false);
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<CasResult<ulong>> DecrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Decrement, key, defaultValue, delta, validFor.GetExpiration(), cas).ConfigureAwait(false);
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
		/// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
		public async Task<CasResult<ulong>> DecrementAsync(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
		{
			var result = await this.CasMutateAsync(MutationMode.Decrement, key, defaultValue, delta, expiresAt.GetExpiration(), cas).ConfigureAwait(false);
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

			this._logger.LogError("Unable to locate node");
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
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="data">The data to be appended to the item.</param>
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
				var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

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

			this._logger.LogError("Unable to locate node");
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
			return (await this.PerformConcatenateAsync(ConcatenationMode.Append, key, data).ConfigureAwait(false)).Success;
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
			var result = await this.PerformConcatenateAsync(ConcatenationMode.Append, key, data, cas).ConfigureAwait(false);
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
			return (await this.PerformConcatenateAsync(ConcatenationMode.Prepend, key, data).ConfigureAwait(false)).Success;
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
			var result = await this.PerformConcatenateAsync(ConcatenationMode.Prepend, key, data, cas).ConfigureAwait(false);
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
				}
				else
					commandResult.Combine(result);
				return result;
			}

			this._logger.LogError("Unable to locate node");

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
			return value != null
				? value is string && typeof(T) == typeof(Guid)
					? (T)(object)new Guid(value as string)
					: (T)value
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
			var success = this.TryGetWithCas(key, out CasResult<object> tmp);
			return new CasResult<T>()
			{
				Cas = tmp.Cas,
				Result = success ? (T)tmp.Result : default(T)
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
				var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

				if (commandResult.Success)
				{
					result.Value = this._transcoder.Deserialize(command.Result);
					result.Cas = command.CasValue;
					result.Pass();
				}
				else
					commandResult.Combine(result);
				return result;
			}

			this._logger.LogError("Unable to locate node");

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
			var result = await this.PerformTryGetAsync(key).ConfigureAwait(false);
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
			var value = await this.GetAsync(key).ConfigureAwait(false);
			return value != null
				? value is string && typeof(T) == typeof(Guid)
					? (T)(object)new Guid(value as string)
					: (T)value
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
			var result = await this.PerformTryGetAsync(key).ConfigureAwait(false);
			return new CasResult<T>()
			{
				Cas = result.Cas,
				Result = result.Success ? (T)result.Value : default(T)
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
				if (node != null)
				{
					if (!results.TryGetValue(node, out IList<string> list))
						results[node] = list = new List<string>(4);
					list.Add(key);
				}
			}
			return results;
		}

		protected virtual IDictionary<string, T> PerformMultiGet<T>(IEnumerable<string> keys, Func<IMultiGetOperation, KeyValuePair<string, CacheItem>, T> collector)
		{
			// transform the keys and index them by hashed => original
			// the multi-get results will be mapped using this index
			var hashed = keys.ToDictionary(key => this._keyTransformer.Transform(key), key => key);
			var values = new Dictionary<string, T>(hashed.Count);

			// execute get commands in parallel
			Func<IMemcachedNode, IList<string>, Task> func = (node, nodeKeys) =>
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
									// collect the cached item
									var result = collector(command, kvp);

									// the lock will serialize the merge, but at least the commands were not waiting on each other
									lock (values)
										values[original] = result;
								}
					}
					catch (Exception ex)
					{
						this._logger.LogError(ex, $"Perform multi-get for {keys.Count()} keys failed");
					}
				});
			};

			// execute each list of keys on their respective node (in parallel)
			var tasks = this.GroupByServer(hashed.Keys)
				.Select(slice => func(slice.Key, slice.Value))
				.ToArray();

			// wait for all nodes to finish
			if (tasks.Length > 0)
				Task.WaitAll(tasks, TimeSpan.FromSeconds(13));

			return values;
		}

		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public IDictionary<string, object> Get(IEnumerable<string> keys)
		{
			return this.PerformMultiGet<object>(keys, (multigetOp, kvp) => this._transcoder.Deserialize(kvp.Value));
		}

		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public IDictionary<string, T> Get<T>(IEnumerable<string> keys)
		{
			return this.PerformMultiGet<T>(keys, (multigetOp, kvp) => this._transcoder.Deserialize<T>(kvp.Value));
		}

		/// <summary>
		/// Retrieves multiple items from the cache with CAS.
		/// </summary>
		/// <param name="keys"></param>
		/// <returns></returns>
		public IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys)
		{
			return this.PerformMultiGet<CasResult<object>>(keys, (multigetOp, kvp) => new CasResult<object>
			{
				Result = this._transcoder.Deserialize(kvp.Value),
				Cas = multigetOp.Cas[kvp.Key]
			});
		}

		protected virtual async Task<IDictionary<string, T>> PerformMultiGetAsync<T>(IEnumerable<string> keys, Func<IMultiGetOperation, KeyValuePair<string, CacheItem>, T> collector)
		{
			// transform the keys and index them by hashed => original
			// the multi-get results will be mapped using this index
			var hashed = keys.ToDictionary(key => this._keyTransformer.Transform(key), key => key);
			var values = new Dictionary<string, T>(hashed.Count);

			// action to execute command in parallel
			Func<IMemcachedNode, IList<string>, Task> func = async (node, nodeKeys) =>
			{
				try
				{
					// execute command
					var command = this._serverPool.OperationFactory.MultiGet(nodeKeys);
					var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

					// deserialize the items in the dictionary
					if (commandResult.Success)
						foreach (var kvp in command.Result)
							if (hashed.TryGetValue(kvp.Key, out string original))
							{
								// collect the cached item
								var result = collector(command, kvp);

								// the lock will serialize the merge, but at least the commands were not waiting on each other
								lock (values)
									values[original] = result;
							}
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, $"Perform multi-get (async) for {keys.Count()} keys failed");
				}
			};

			// execute each list of keys on their respective node (in parallel)
			var tasks = this.GroupByServer(hashed.Keys)
				.Select(slice => func(slice.Key, slice.Value))
				.ToList();

			// wait for all nodes to finish
			if (tasks.Count > 0)
				await Task.WhenAll(tasks).ConfigureAwait(false);

			return values;
		}

		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public Task<IDictionary<string, object>> GetAsync(IEnumerable<string> keys)
		{
			return this.PerformMultiGetAsync<object>(keys, (multigetOp, kvp) => this._transcoder.Deserialize(kvp.Value));
		}

		/// <summary>
		/// Retrieves multiple items from the cache.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="keys">The list of identifiers for the items to retrieve.</param>
		/// <returns>a Dictionary holding all items indexed by their key.</returns>
		public Task<IDictionary<string, T>> GetAsync<T>(IEnumerable<string> keys)
		{
			return this.PerformMultiGetAsync<T>(keys, (multigetOp, kvp) => this._transcoder.Deserialize<T>(kvp.Value));
		}

		/// <summary>
		/// Retrieves multiple items from the cache with CAS.
		/// </summary>
		/// <param name="keys"></param>
		/// <returns></returns>
		public Task<IDictionary<string, CasResult<object>>> GetWithCasAsync(IEnumerable<string> keys)
		{
			return this.PerformMultiGetAsync<CasResult<object>>(keys, (multigetOp, kvp) => new CasResult<object>
			{
				Result = this._transcoder.Deserialize(kvp.Value),
				Cas = multigetOp.Cas[kvp.Key]
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
					result.Pass();
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Failed to remove item, see InnerResult or StatusCode for details");
				}
				return result;
			}

			this._logger.LogError("Unable to locate node");
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
				var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

				if (commandResult.Success)
					result.Pass();
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Failed to remove item, see InnerResult or StatusCode for details");
				}
				return result;
			}

			this._logger.LogError("Unable to locate node");
			result.Fail("Unable to locate node");
			return result;
		}

		/// <summary>
		/// Removes the specified item from the cache.
		/// </summary>
		/// <param name="key">The identifier for the item to delete.</param>
		/// <returns>true if the item was successfully removed from the cache; false otherwise.</returns>
		public async Task<bool> RemoveAsync(string key)
		{
			return (await this.PerformRemoveAsync(key).ConfigureAwait(false)).Success;
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
			if (!await this.AppendAsync(key, new ArraySegment<byte>(new byte[0])).ConfigureAwait(false))
			{
				await this.RemoveAsync(key).ConfigureAwait(false);
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
			return Task.WhenAll(this._serverPool.GetWorkingNodes().Select(node => node.ExecuteAsync(this._serverPool.OperationFactory.Flush())));
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

			Func<IMemcachedNode, IStatsOperation, EndPoint, Task> func = (node, command, endpoint) =>
			{
				return Task.Run(() =>
				{
					node.Execute(command);
					lock (results)
						results[endpoint] = command.Result;
				});
			};

			var tasks = this._serverPool.GetWorkingNodes().Select(node => func(node, this._serverPool.OperationFactory.Stats(type), node.EndPoint)).ToArray();
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

			Func<IMemcachedNode, IStatsOperation, EndPoint, Task> func = async (node, command, endpoint) =>
			{
				await node.ExecuteAsync(command).ConfigureAwait(false);
				lock (results)
					results[endpoint] = command.Result;
			};

			var tasks = this._serverPool.GetWorkingNodes().Select(node => func(node, this._serverPool.OperationFactory.Stats(type), node.EndPoint)).ToList();
			if (tasks.Count > 0)
				await Task.WhenAll(tasks).ConfigureAwait(false);

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
			try
			{
				this._serverPool?.Dispose();
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
			var expires = options == null ? TimeSpan.Zero : options.GetExpiration();
			var validFor = expires is TimeSpan
				? (TimeSpan)expires
				: CacheUtils.Helper.UnixEpoch.AddSeconds((long)expires).ToTimeSpan();
			if (this.Store(StoreMode.Set, key, value, validFor) && expires is TimeSpan && validFor != TimeSpan.Zero)
				this.Store(StoreMode.Set, key.GetIDistributedCacheExpirationKey(), expires, validFor);
		}

		async Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));
			var expires = options == null ? TimeSpan.Zero : options.GetExpiration();
			var validFor = expires is TimeSpan
				? (TimeSpan)expires
				: CacheUtils.Helper.UnixEpoch.AddSeconds((long)expires).ToTimeSpan();
			if (await this.StoreAsync(StoreMode.Set, key, value, validFor).ConfigureAwait(false) && expires is TimeSpan && validFor != TimeSpan.Zero)
				await this.StoreAsync(StoreMode.Set, key.GetIDistributedCacheExpirationKey(), expires, validFor).ConfigureAwait(false);
		}

		byte[] IDistributedCache.Get(string key)
		{
			return string.IsNullOrWhiteSpace(key)
				? throw new ArgumentNullException(nameof(key))
				: this.Get<byte[]>(key);
		}

		Task<byte[]> IDistributedCache.GetAsync(string key, CancellationToken token = default(CancellationToken))
		{
			return string.IsNullOrWhiteSpace(key)
				? throw new ArgumentNullException(nameof(key))
				: this.GetAsync<byte[]>(key);
		}

		void IDistributedCache.Refresh(string key)
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));
			var value = this.Get<byte[]>(key);
			var expires = value != null ? this.Get(key.GetIDistributedCacheExpirationKey()) : null;
			if (value != null && expires != null && expires is TimeSpan && this.Store(StoreMode.Replace, key, value, (TimeSpan)expires))
				this.Store(StoreMode.Replace, key.GetIDistributedCacheExpirationKey(), expires, (TimeSpan)expires);
		}

		async Task IDistributedCache.RefreshAsync(string key, CancellationToken token = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));
			var value = await this.GetAsync<byte[]>(key).ConfigureAwait(false);
			var expires = value != null ? await this.GetAsync(key.GetIDistributedCacheExpirationKey()).ConfigureAwait(false) : null;
			if (value != null && expires != null && expires is TimeSpan && await this.StoreAsync(StoreMode.Replace, key, value, (TimeSpan)expires).ConfigureAwait(false))
				await this.StoreAsync(StoreMode.Replace, key.GetIDistributedCacheExpirationKey(), expires, (TimeSpan)expires).ConfigureAwait(false);
		}

		void IDistributedCache.Remove(string key)
		{
			if (string.IsNullOrWhiteSpace(key))
				throw new ArgumentNullException(nameof(key));
			this.Remove(key);
			this.Remove(key.GetIDistributedCacheExpirationKey());
		}

		Task IDistributedCache.RemoveAsync(string key, CancellationToken token = default(CancellationToken))
		{
			return string.IsNullOrWhiteSpace(key)
				? throw new ArgumentNullException(nameof(key))
				: Task.WhenAll(
					this.RemoveAsync(key),
					this.RemoveAsync(key.GetIDistributedCacheExpirationKey())
				);
		}
		#endregion

	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    � 2010 Attila Kisk� (aka Enyim), � 2016 CNBlogs, � 2018 VIEApps.net
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
