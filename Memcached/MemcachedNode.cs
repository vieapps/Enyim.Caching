#region Related components
using System;
using System.IO;
using System.Net;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Enyim.Collections;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached.Results;

using Microsoft.Extensions.Logging;
#endregion

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Represents a Memcached node in the pool.
	/// </summary>
	[DebuggerDisplay("Address: {EndPoint}, Alive = {IsAlive}")]
	public class MemcachedNode : IMemcachedNode
	{
		static readonly object Locker = new object();

		#region Attributes
		readonly ILogger _logger;
		readonly ISocketPoolConfiguration _config;
		readonly EndPoint _endpoint;
		InternalPoolImpl _internalPoolImpl;
		INodeFailurePolicy _failurePolicy;
		bool _isDisposed, _isInitialized;

		public event Action<IMemcachedNode> Failed = null;
		#endregion

		public MemcachedNode(EndPoint endpoint, ISocketPoolConfiguration config)
		{
			if (config.ConnectionTimeout.TotalMilliseconds >= Int32.MaxValue)
				throw new InvalidOperationException($"ConnectionTimeout must be < {Int32.MaxValue}");

			this._logger = Logger.CreateLogger<IMemcachedNode>();
			this._endpoint = endpoint;
			this._config = config;
			this._internalPoolImpl = new InternalPoolImpl(this, this._config);
		}

		protected INodeFailurePolicy FailurePolicy => this._failurePolicy ?? (this._failurePolicy = this._config.FailurePolicyFactory.Create(this));

		/// <summary>
		/// Gets the <see cref="IPEndPoint"/> of this instance
		/// </summary>
		public EndPoint EndPoint => this._endpoint;

		/// <summary>
		/// <para>Gets a value indicating whether the server is working or not. Returns a <b>cached</b> state.</para>
		/// <para>To get real-time information and update the cached state, use the <see cref="Ping"/> method.</para>
		/// </summary>
		/// <remarks>Used by the <see cref="IServerPool"/> to quickly check if the server's state is valid.</remarks>
		public bool IsAlive => this._internalPoolImpl.IsAlive;

		/// <summary>
		/// Gets a value indicating whether the server is working or not.
		/// If the server is back online, we'll ercreate the internal socket pool and mark the server as alive so operations can target it.
		/// </summary>
		/// <returns>true if the server is alive; false otherwise.</returns>
		public bool Ping()
		{
			// is the server working?
			if (this.IsAlive)
				return true;

			// this codepath is (should be) called very rarely
			// if you get here hundreds of times then you have bigger issues
			// and try to make the memcached instaces more stable and/or increase the deadTimeout
			try
			{
				// we could connect to the server, let's recreate the socket pool
				lock (MemcachedNode.Locker)
				{
					if (this._isDisposed)
						return false;

					// try to connect to the server
					using (var socket = this.CreateSocket())
					{
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Try to connect to the memcached server: {this.EndPoint}");
					}

					if (this.IsAlive)
						return true;

					// it's easier to create a new pool than reinitializing a dead one
					// rewrite-then-dispose to avoid a race condition with Acquire (which does no locking)
					var oldPool = this._internalPoolImpl;
					var newPool = new InternalPoolImpl(this, this._config);
					Interlocked.Exchange(ref this._internalPoolImpl, newPool);
					try
					{
						oldPool.Dispose();
					}
					catch { }
				}

				return true;
			}
			catch
			{
				return false;   // could not reconnect
			}
		}

		/// <summary>
		/// Acquires a new item from the pool
		/// </summary>
		/// <returns>An <see cref="PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
		public IPooledSocketResult Acquire()
		{
			if (!this._isInitialized)
				lock (this._internalPoolImpl)
				{
					if (!this._isInitialized)
					{
						var startTime = DateTime.Now;
						this._internalPoolImpl.InitPool();
						this._isInitialized = true;
						if (this._logger.IsEnabled(LogLevel.Trace))
							this._logger.LogInformation($"Cost for initiaizing pool: {(DateTime.Now - startTime).TotalMilliseconds}ms");
					}
				}

			try
			{
				return this._internalPoolImpl.Acquire();
			}
			catch (Exception e)
			{
				var message = "Acquire failed. Maybe we're already disposed?";
				this._logger.LogError(e, message);

				var result = new PooledSocketResult();
				result.Fail(message, e);
				return result;
			}
		}

		protected internal virtual PooledSocket CreateSocket()
		{
			try
			{
				return new PooledSocket(this.EndPoint, this._config.ConnectionTimeout, this._config.ReceiveTimeout, this._config.NoDelay);
			}
			catch (Exception ex)
			{
				this._logger.LogError(ex, $"Cannot create socket ({this.EndPoint})");
				throw ex;
			}
		}

		#region Dispose
		~MemcachedNode()
		{
			try
			{
				((IDisposable)this).Dispose();
			}
			catch { }
		}

		/// <summary>
		/// Releases all resources allocated by this instance
		/// </summary>
		public void Dispose()
		{
			if (this._isDisposed)
				return;

			GC.SuppressFinalize(this);

			// this is not a graceful shutdown if someone uses a pooled item then it's 99% that an exception will be thrown somewhere,
			// but since the dispose is mostly used when everyone else is finished this should not kill any kittens
			lock (MemcachedNode.Locker)
			{
				if (this._isDisposed)
					return;

				this._isDisposed = true;
				this._internalPoolImpl.Dispose();
			}
		}

		void IDisposable.Dispose() => this.Dispose();
		#endregion

		#region [ InternalPoolImpl                  ]
		class InternalPoolImpl : IDisposable
		{
			/// <summary>
			/// A list of already connected but free to use sockets
			/// </summary>
			InterlockedStack<PooledSocket> _freeItems;

			bool _isDisposed = false;
			readonly int _minItems;
			readonly int _maxItems;
			MemcachedNode _ownerNode;
			readonly EndPoint _endpoint;
			readonly TimeSpan _queueTimeout;
			Semaphore _semaphore;
			readonly ILogger _logger;

			internal InternalPoolImpl(MemcachedNode ownerNode, ISocketPoolConfiguration config)
			{
				if (config.MinPoolSize < 0)
					throw new InvalidOperationException("Min pool size must be larger >= 0", null);
				if (config.MaxPoolSize < config.MinPoolSize)
					throw new InvalidOperationException("Max pool size must be larger than min pool size", null);
				if (config.QueueTimeout < TimeSpan.Zero)
					throw new InvalidOperationException("queueTimeout must be >= TimeSpan.Zero", null);

				this._ownerNode = ownerNode;
				this._endpoint = ownerNode.EndPoint;
				this._queueTimeout = config.QueueTimeout;

				this._minItems = config.MinPoolSize;
				this._maxItems = config.MaxPoolSize;

				this._semaphore = new Semaphore(this._maxItems, this._maxItems);
				this._freeItems = new InterlockedStack<PooledSocket>();

				this._logger = Logger.CreateLogger<InternalPoolImpl>();
			}

			internal void InitPool()
			{
				if (this._minItems > 0)
					try
					{
						for (int index = 0; index < this._minItems; index++)
						{
							this._freeItems.Push(this.CreateSocket());
							if (!this.IsAlive) // cannot connect to the server
								break;
						}
						if (this.IsAlive && this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Pool has been initialized for {this._endpoint} with {this._minItems} sockets");
					}
					catch (Exception e)
					{
						this._logger.LogError(e, $"Could not initialize pool of sockets for {this._endpoint}");
						this.MarkAsDead();
					}
			}

			PooledSocket CreateSocket()
			{
				var socket = this._ownerNode.CreateSocket();
				socket.CleanupCallback = this.ReleaseSocket;
				return socket;
			}

			public bool IsAlive { get; private set; } = true;

			public DateTime MarkedAsDeadUtc { get; set; }

			/// <summary>
			/// Acquires a new item from the pool
			/// </summary>
			/// <returns>An <see cref="PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
			public IPooledSocketResult Acquire()
			{
				var result = new PooledSocketResult();
				if (this._logger.IsEnabled(LogLevel.Trace))
					this._logger.LogDebug($"Acquire a socket from pool ({this._endpoint})");

				if (!this.IsAlive || this._isDisposed)
				{
					var message = $"Pool is dead or disposed, returning null ({this._endpoint})";
					result.Fail(message);
					if (this._logger.IsEnabled(LogLevel.Trace))
						this._logger.LogWarning(message);
					return result;
				}

				// everyone is so busy
				if (!this._semaphore.WaitOne(this._queueTimeout))
				{
					var message = $"Pool is full, timeouting ({this._endpoint})";
					result.Fail(message, new TimeoutException());
					if (this._logger.IsEnabled(LogLevel.Trace))
						this._logger.LogWarning(message);
					return result;
				}

				// maybe we died while waiting
				if (!this.IsAlive)
				{
					var message = $"Pool is dead, returning null ({this._endpoint})";
					result.Fail(message);
					if (this._logger.IsEnabled(LogLevel.Trace))
						this._logger.LogWarning(message);
					return result;
				}

				// do we have free items?
				if (this._freeItems.TryPop(out PooledSocket socket))
					try
					{
						socket.Reset();
						result.Pass();
						result.Value = socket;
						if (this._logger.IsEnabled(LogLevel.Trace))
							this._logger.LogDebug($"Socket is ready to use ({socket.InstanceID})");
						return result;
					}
					catch (Exception e)
					{
						this.MarkAsDead();
						var message = "Failed to reset an acquired socket";
						this._logger.LogError(e, message);
						result.Fail(message, e);
						return result;
					}

				// free item pool is empty
				if (this._logger.IsEnabled(LogLevel.Trace))
					this._logger.LogInformation($"Could not get a socket from the pool => create new ({this._endpoint})");

				try
				{
					// okay, create the new item
					var startTime = DateTime.Now;
					socket = this.CreateSocket();
					result.Value = socket;
					result.Pass();
					if (this._logger.IsEnabled(LogLevel.Trace))
						this._logger.LogWarning($"A new socket is created ({socket.InstanceID}). Cost for creating socket when acquire: {(DateTime.Now - startTime).TotalMilliseconds}ms");
				}
				catch (Exception e)
				{
					var message = $"Failed to create a new socket ({this._endpoint})";
					this._logger.LogError(message, e);

					// eventhough this item failed the failure policy may keep the pool alive
					// so we need to make sure to release the semaphore, so new connections can be
					// acquired or created (otherwise dead conenctions would "fill up" the pool
					// while the FP pretends that the pool is healthy)
					this._semaphore.Release();

					this.MarkAsDead();
					result.Fail(message);
					return result;
				}

				return result;
			}

			void MarkAsDead()
			{
				if (this._logger.IsEnabled(LogLevel.Trace))
					this._logger.LogDebug($"Mark as dead was requested ({this._endpoint})");

				var shouldFail = this._ownerNode.FailurePolicy.ShouldFail();
				if (this._logger.IsEnabled(LogLevel.Trace))
					this._logger.LogDebug("FailurePolicy.ShouldFail(): " + shouldFail);

				if (shouldFail)
				{
					if (this._logger.IsEnabled(LogLevel.Trace))
						this._logger.LogWarning($"Marking node {this._endpoint} is dead");

					this.IsAlive = false;
					this.MarkedAsDeadUtc = DateTime.UtcNow;

					this._ownerNode.Failed?.Invoke(this._ownerNode);
				}
			}

			/// <summary>
			/// Releases an item back into the pool
			/// </summary>
			/// <param name="socket"></param>
			void ReleaseSocket(PooledSocket socket)
			{
				if (this._logger.IsEnabled(LogLevel.Trace))
					this._logger.LogDebug($"Releasing socket ({socket.InstanceID}) - Alive: {this.IsAlive}");

				if (this.IsAlive)
				{
					// is it still working (i.e. the server is still connected)
					if (socket.IsAlive)
					{
						// mark the item as free
						this._freeItems.Push(socket);

						// signal the event so if someone is waiting for it can reuse this item
						this._semaphore.Release();
					}
					else
					{
						// kill this item
						socket.Destroy();

						// mark ourselves as not working for a while
						this.MarkAsDead();

						// make sure to signal the Acquire so it can create a new conenction
						// if the failure policy keeps the pool alive
						this._semaphore.Release();
					}
				}
				else
				{
					// one of our previous sockets has died, so probably all of them 
					// are dead. so, kill the socket (this will eventually clear the pool as well)
					socket.Destroy();
				}
			}

			~InternalPoolImpl()
			{
				try
				{
					((IDisposable)this).Dispose();
				}
				catch { }
			}

			/// <summary>
			/// Releases all resources allocated by this instance
			/// </summary>
			public void Dispose()
			{
				// this is not a graceful shutdown
				// if someone uses a pooled item then 99% that an exception will be thrown
				// somewhere. But since the dispose is mostly used when everyone else is finished
				// this should not kill any kittens
				if (!this._isDisposed)
				{
					this.IsAlive = false;
					this._isDisposed = true;

					while (this._freeItems.TryPop(out PooledSocket socket))
						try
						{
							socket.Destroy();
						}
						catch { }

					this._ownerNode = null;
					this._semaphore.Dispose();
					this._semaphore = null;
					this._freeItems = null;
				}
			}

			void IDisposable.Dispose()
				=> this.Dispose();
		}
		#endregion

		#region [ Comparer                                  ]
		internal sealed class Comparer : IEqualityComparer<IMemcachedNode>
		{
			public static readonly Comparer Instance = new Comparer();

			bool IEqualityComparer<IMemcachedNode>.Equals(IMemcachedNode x, IMemcachedNode y) => x.EndPoint.Equals(y.EndPoint);

			int IEqualityComparer<IMemcachedNode>.GetHashCode(IMemcachedNode obj) => obj.EndPoint.GetHashCode();
		}
		#endregion

		protected virtual IPooledSocketResult ExecuteOperation(IOperation op)
		{
			var result = this.Acquire();
			if (result.Success && result.HasValue)
				try
				{
					var startTime = DateTime.Now;
					var socket = result.Value;
					socket.Send(op.GetBuffer());
					if (this._logger.IsEnabled(LogLevel.Trace))
						this._logger.LogInformation($"Cost for writting into socket when execute operation: {(DateTime.Now - startTime).TotalMilliseconds}ms");

					var readResult = op.ReadResponse(socket);
					if (readResult.Success)
						result.Pass();
					else
						readResult.Combine(result);

					return result;
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, "Failed to execute an operation");
					result.Fail("Failed to execute an operation", ex);
					return result;
				}
				finally
				{
					((IDisposable)result.Value).Dispose();
				}

			this._logger.LogError("Failed to obtain socket from pool");
			result.Fail("Failed to obtain socket from pool");
			return result;
		}

		protected async virtual Task<IPooledSocketResult> ExecuteOperationAsync(IOperation op, CancellationToken cancellationToken = default)
		{
			var result = this.Acquire();
			if (result.Success && result.HasValue)
				try
				{
					var startTime = DateTime.Now;
					var socket = result.Value;
					await socket.SendAsync(op.GetBuffer(), cancellationToken).ConfigureAwait(false);
					if (this._logger.IsEnabled(LogLevel.Trace))
						this._logger.LogInformation($"Cost for writting into socket when execute operation (async): {(DateTime.Now - startTime).TotalMilliseconds}ms");

					var readResult = await op.ReadResponseAsync(socket, cancellationToken).ConfigureAwait(false);
					if (readResult.Success)
						result.Pass();
					else
						readResult.Combine(result);

					return result;
				}
				catch (OperationCanceledException)
				{
					throw;
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, "Failed to execute an operation (async)");
					result.Fail("Failed to execute an operation (async)", ex);
					return result;
				}
				finally
				{
					((IDisposable)result.Value).Dispose();
				}

			this._logger.LogError("Failed to obtain socket from pool");
			result.Fail("Failed to obtain socket from pool");
			return result;
		}

		protected virtual bool ExecuteOperationAsync(IOperation op, Action<bool> next)
		{
			var socket = this.Acquire().Value;
			if (socket != null)
				try
				{
					socket.Send(op.GetBuffer());
					return op.ReadResponseAsync(socket, readSuccess =>
					{
						((IDisposable)socket).Dispose();
						next(readSuccess);
					});
				}
				catch (Exception e)
				{
					this._logger.LogError(e, "Error occurred while executing an operation (with next action)");
					((IDisposable)socket).Dispose();
				}
			return false;
		}

		#region [ IMemcachedNode               ]
		EndPoint IMemcachedNode.EndPoint => this.EndPoint;

		bool IMemcachedNode.IsAlive => this.IsAlive;

		bool IMemcachedNode.Ping() => this.Ping();

		IOperationResult IMemcachedNode.Execute(IOperation op)
			=> this.ExecuteOperation(op);

		async Task<IOperationResult> IMemcachedNode.ExecuteAsync(IOperation op, CancellationToken cancellationToken = default)
			=> await this.ExecuteOperationAsync(op).ConfigureAwait(false);

		bool IMemcachedNode.ExecuteAsync(IOperation op, Action<bool> next)
			=> this.ExecuteOperationAsync(op, next);

		event Action<IMemcachedNode> IMemcachedNode.Failed
		{
			add
			{
				this.Failed += value;
			}
			remove
			{
				this.Failed -= value;
			}
		}
		#endregion

	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2020 VIEApps.net
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
