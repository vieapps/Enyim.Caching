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
		readonly ILogger _logger;

		IMemcachedNode[] _allNodes;
		readonly IMemcachedClientConfiguration _configuration;
		readonly IOperationFactory _factory;
		INodeLocator _nodeLocator;

		readonly object _locker = new object();
		readonly int _deadTimeoutMsec;
		bool _isTimerActive, _isDisposed;
		event Action<IMemcachedNode> _onNodeFailed;
		Timer _resurrectTimer;

		public DefaultServerPool(IMemcachedClientConfiguration configuration, IOperationFactory opFactory)
		{
			this._configuration = configuration ?? throw new ArgumentNullException(nameof(configuration), "Configuration is invalid");
			this._factory = opFactory ?? throw new ArgumentNullException(nameof(opFactory), "Operation factory is invalid");
			this._deadTimeoutMsec = (int)this._configuration.SocketPool.DeadTimeout.TotalMilliseconds;
			this._logger = Logger.CreateLogger<DefaultServerPool>();
		}

		~DefaultServerPool()
		{
			try
			{
				((IDisposable)this).Dispose();
			}
			catch { }
		}

		protected virtual IMemcachedNode CreateNode(EndPoint endpoint) => new MemcachedNode(endpoint, this._configuration.SocketPool);

		void OnResurrectCallback(object state)
		{
			this._logger.Log(LogLevel.Debug, LogLevel.Debug, "Checking the dead servers");

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

			lock (this._locker)
			{
				if (this._isDisposed)
				{
					this._logger.Log(LogLevel.Debug, LogLevel.Warning, "IsAlive timer was triggered but the pool is already disposed. Ignoring.");
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
						this._logger.Log(LogLevel.Debug, LogLevel.Debug, $"Alive: {node.EndPoint}");
						aliveList.Add(node);
					}
					else
					{
						this._logger.Log(LogLevel.Debug, LogLevel.Debug, $"Dead: {node.EndPoint}");
						if (node.Ping())
						{
							changed = true;
							aliveList.Add(node);
							this._logger.Log(LogLevel.Debug, LogLevel.Debug, "Ping OK, go back alive");
						}
						else
						{
							this._logger.Log(LogLevel.Debug, LogLevel.Debug, "Still dead");
							deadCount++;
						}
					}
				}

				// reinit the locator
				if (changed)
				{
					this._logger.Log(LogLevel.Debug, LogLevel.Debug, "Reinitializing the locator");
					this._nodeLocator.Initialize(aliveList);
				}

				// stop or restart the timer
				if (deadCount == 0)
				{
					this._logger.Log(LogLevel.Debug, LogLevel.Debug, "Count of dead is 0, stopping the timer.");
					this._isTimerActive = false;
				}
				else
				{
					this._logger.Log(LogLevel.Debug, LogLevel.Debug, $"Count of dead is {deadCount}, starting the timer.");
					this._resurrectTimer.Change(this._deadTimeoutMsec, Timeout.Infinite);
				}
			}
		}

		void NodeFail(IMemcachedNode node)
		{
			this._logger.Log(LogLevel.Debug, LogLevel.Debug, $"Node {node.EndPoint} is dead");

			// the timer is stopped until we encounter the first dead server
			// when we have one, we trigger it and it will run after DeadTimeout has elapsed
			lock (this._locker)
			{
				if (this._isDisposed)
				{
					this._logger.Log(LogLevel.Debug, LogLevel.Warning, "Got a node fail but the pool is already disposed. Ignoring.");
					return;
				}

				// bubble up the fail event to the client
				this._onNodeFailed?.Invoke(node);

				// re-initialize the locator
				var newLocator = this._configuration.CreateNodeLocator();
				newLocator.Initialize(this._allNodes.Where(n => n.IsAlive).ToArray());
				Interlocked.Exchange(ref this._nodeLocator, newLocator);

				// the timer is stopped until we encounter the first dead server
				// when we have one, we trigger it and it will run after DeadTimeout has elapsed
				if (!this._isTimerActive)
				{
					this._logger.Log(LogLevel.Debug, LogLevel.Debug, "Starting the recovery timer");
					if (this._resurrectTimer == null)
						this._resurrectTimer = new Timer(this.OnResurrectCallback, null, this._deadTimeoutMsec, Timeout.Infinite);
					else
						this._resurrectTimer.Change(this._deadTimeoutMsec, Timeout.Infinite);

					this._isTimerActive = true;
					this._logger.Log(LogLevel.Debug, LogLevel.Debug, "Recovery timer is started");
				}
			}
		}

		#region [ IServerPool                  ]
		IMemcachedNode IServerPool.Locate(string key) => this._nodeLocator.Locate(key);

		IOperationFactory IServerPool.OperationFactory => this._factory;

		IEnumerable<IMemcachedNode> IServerPool.GetWorkingNodes() => this._nodeLocator.GetWorkingNodes();

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
				this._onNodeFailed += value;
			}
			remove
			{
				this._onNodeFailed -= value;
			}
		}
		#endregion

		#region [ IDisposable                  ]
		void IDisposable.Dispose()
		{
			GC.SuppressFinalize(this);

			lock (this._locker)
			{
				if (this._isDisposed) return;

				this._isDisposed = true;

				// dispose the locator first, maybe it wants to access the nodes one last time
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
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2019 VIEApps.net
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
