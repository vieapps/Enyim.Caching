using System;
using System.Linq;
using System.Net;
using System.Collections.Generic;
using System.Threading;

using Enyim.Caching.Configuration;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached
{
	public class DefaultServerPool : IServerPool, IDisposable
	{
		ILogger _logger;

		IMemcachedNode[] _allNodes;

		IMemcachedClientConfiguration _configuration;
		IOperationFactory _factory;
		IMemcachedNodeLocator _nodeLocator;

		object _deadSync = new Object();
		int _deadTimeoutMsec;
		bool _isTimerActive, _isDisposed;
		event Action<IMemcachedNode> _nodeFailed;
		System.Threading.Timer _resurrectTimer;

		public DefaultServerPool(IMemcachedClientConfiguration configuration, IOperationFactory opFactory)
		{
			this._configuration = configuration ?? throw new ArgumentNullException(nameof(configuration), "Socket configuration is invalid");
			this._factory = opFactory ?? throw new ArgumentNullException(nameof(opFactory), "Operation factory is invalid");
			this._deadTimeoutMsec = (int)this._configuration.SocketPool.DeadTimeout.TotalMilliseconds;
			this._logger = LogManager.CreateLogger<DefaultServerPool>();
		}

		~DefaultServerPool()
		{
			try
			{
				((IDisposable)this).Dispose();
			}
			catch { }
		}

		protected virtual IMemcachedNode CreateNode(EndPoint endpoint)
		{
			return new MemcachedNode(endpoint, this._configuration.SocketPool);
		}

		void OnResurrectCallback(object state)
		{
			var isDebug = this._logger.IsEnabled(LogLevel.Debug);
			if (isDebug)
				this._logger.LogDebug("Checking the dead servers.");

			// how this works:
			// 1. timer is created but suspended
			// 2. Locate encounters a dead server, so it starts the timer which will trigger after deadTimeout has elapsed
			// 3. if another server goes down before the timer is triggered, nothing happens in Locate (isRunning == true).
			//		however that server will be inspected sooner than Dead Timeout.
			//		   S1 died   S2 died    dead timeout
			//		|----*--------*------------*-
			//           |                     |
			//          timer start           both servers are checked here
			// 4. we iterate all the servers and record it in another list
			// 5. if we found a dead server which responds to Ping(), the locator will be reinitialized
			// 6. if at least one server is still down (Ping() == false), we restart the timer
			// 7. if all servers are up, we set isRunning to false, so the timer is suspended
			// 8. GOTO 2

			lock (this._deadSync)
			{
				if (this._isDisposed)
				{
					if (this._logger.IsEnabled(LogLevel.Warning))
						this._logger.LogWarning("IsAlive timer was triggered but the pool is already disposed. Ignoring.");
					return;
				}

				var nodes = this._allNodes;
				var aliveList = new List<IMemcachedNode>(nodes.Length);
				var changed = false;
				var deadCount = 0;

				for (var index = 0; index < nodes.Length; index++)
				{
					var node = nodes[index];
					if (node.IsAlive)
					{
						if (isDebug)
							this._logger.LogDebug("Alive: {0}", node.EndPoint);
						aliveList.Add(node);
					}
					else
					{
						if (isDebug)
							this._logger.LogDebug("Dead: {0}", node.EndPoint);

						if (node.Ping())
						{
							changed = true;
							aliveList.Add(node);
							if (isDebug)
								this._logger.LogDebug("Ping ok.");
						}
						else
						{
							if (isDebug)
								this._logger.LogDebug("Still dead.");
							deadCount++;
						}
					}
				}

				// reinit the locator
				if (changed)
				{
					if (isDebug)
						this._logger.LogDebug("Reinitializing the locator.");
					this._nodeLocator.Initialize(aliveList);
				}

				// stop or restart the timer
				if (deadCount == 0)
				{
					if (isDebug)
						this._logger.LogDebug("deadCount == 0, stopping the timer.");
					this._isTimerActive = false;
				}
				else
				{
					if (isDebug)
						this._logger.LogDebug("deadCount == {0}, starting the timer.", deadCount);
					this._resurrectTimer.Change(this._deadTimeoutMsec, Timeout.Infinite);
				}
			}
		}

		void NodeFail(IMemcachedNode node)
		{
			var isDebug = this._logger.IsEnabled(LogLevel.Debug);
			if (isDebug)
				this._logger.LogDebug("Node {0} is dead.", node.EndPoint);

			// the timer is stopped until we encounter the first dead server
			// when we have one, we trigger it and it will run after DeadTimeout has elapsed
			lock (this._deadSync)
			{
				if (this._isDisposed)
				{
					if (this._logger.IsEnabled(LogLevel.Warning))
						this._logger.LogWarning("Got a node fail but the pool is already disposed. Ignoring.");
					return;
				}

				// bubble up the fail event to the client
				this._nodeFailed?.Invoke(node);

				// re-initialize the locator
				var newLocator = this._configuration.CreateNodeLocator();
				newLocator.Initialize(this._allNodes.Where(n => n.IsAlive).ToArray());
				Interlocked.Exchange(ref this._nodeLocator, newLocator);

				// the timer is stopped until we encounter the first dead server
				// when we have one, we trigger it and it will run after DeadTimeout has elapsed
				if (!this._isTimerActive)
				{
					if (isDebug)
						this._logger.LogDebug("Starting the recovery timer.");

					if (this._resurrectTimer == null)
						this._resurrectTimer = new Timer(this.OnResurrectCallback, null, this._deadTimeoutMsec, Timeout.Infinite);
					else
						this._resurrectTimer.Change(this._deadTimeoutMsec, Timeout.Infinite);

					this._isTimerActive = true;

					if (isDebug)
						this._logger.LogDebug("Recovery timer is started.");
				}
			}
		}

		#region [ IServerPool                  ]
		IMemcachedNode IServerPool.Locate(string key)
		{
			return this._nodeLocator.Locate(key);
		}

		IOperationFactory IServerPool.OperationFactory
		{
			get { return this._factory; }
		}

		IEnumerable<IMemcachedNode> IServerPool.GetWorkingNodes()
		{
			return this._nodeLocator.GetWorkingNodes();
		}

		void IServerPool.Start()
		{
			this._allNodes = this._configuration.Servers
				.Select(endpoint =>
				{
					var node = this.CreateNode(endpoint);
					node.Failed += this.NodeFail;
					return node;
				})
				.ToArray();

			// initialize the locator
			var locator = this._configuration.CreateNodeLocator();
			locator.Initialize(this._allNodes);

			this._nodeLocator = locator;
		}

		event Action<IMemcachedNode> IServerPool.NodeFailed
		{
			add
			{
				this._nodeFailed += value;
			}
			remove
			{
				this._nodeFailed -= value;
			}
		}
		#endregion

		#region [ IDisposable                  ]
		void IDisposable.Dispose()
		{
			GC.SuppressFinalize(this);

			lock (this._deadSync)
			{
				if (this._isDisposed) return;

				this._isDisposed = true;

				// dispose the locator first, maybe it wants to access 
				// the nodes one last time
				if (this._nodeLocator is IDisposable nd)
					try
					{
						nd.Dispose();
					}
					catch (Exception e)
					{
						this._logger.LogError(nameof(DefaultServerPool), e);
					}

				this._nodeLocator = null;

				for (var index = 0; index < this._allNodes.Length; index++)
					try
					{
						this._allNodes[index].Dispose();
					}
					catch (Exception e)
					{
						this._logger.LogError(nameof(DefaultServerPool), e);
					}

				// stop the timer
				if (this._resurrectTimer != null)
					using (this._resurrectTimer)
					{
						this._resurrectTimer.Change(Timeout.Infinite, Timeout.Infinite);
					}

				this._allNodes = null;
				this._resurrectTimer = null;
			}
		}
		#endregion

	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2017 VIEApps.net
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
