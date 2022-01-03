using System;
using System.Linq;
using System.Net;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Represents the statistics of a Memcached node.
	/// </summary>
	public sealed class ServerStats
	{
		const int OpAllowsSum = 1;
		readonly ILogger _logger;

		/// <summary>
		/// Defines a value which indicates that the statstics should be retrieved for all servers in the pool.
		/// </summary>
		public static readonly EndPoint All = new IPEndPoint(IPAddress.Any, 0);

		// defines which values can be summed and which not
		static readonly int[] Optable =
		{
			0, 0, 0, 1, 1, 1, 1, 1,
			1, 1, 1, 1, 1, 1, 1, 1
		};

		static readonly string[] StatKeys =
		{
			"uptime",
			"time",
			"version",
			"curr_items",
			"total_items",
			"curr_connections",
			"total_connections",
			"connection_structures",
			"cmd_get",
			"cmd_set",
			"get_hits",
			"get_misses",
			"bytes",
			"bytes_read",
			"bytes_written",
			"limit_maxbytes",
		};

		readonly IDictionary<EndPoint, Dictionary<string, string>> results;

		internal ServerStats(IDictionary<EndPoint, Dictionary<string, string>> results)
		{
			this._logger = Logger.CreateLogger<ServerStats>();
			this.results = results;
		}

		/// <summary>
		/// Gets a stat value for the specified server.
		/// </summary>
		/// <param name="server">The adress of the server. If <see cref="IPAddress.Any"/> is specified it will return the sum of all server stat values.</param>
		/// <param name="item">The stat to be returned</param>
		/// <returns>The value of the specified stat item</returns>
		public long GetValue(IPEndPoint server, StatItem item)
		{
			// asked for a specific server
			if (server.Address != IPAddress.Any)
			{
				var tmp = this.GetRaw(server, item);
				return string.IsNullOrEmpty(tmp)
					?  throw new ArgumentException("Item was not found: " + item)
					: Int64.TryParse(tmp, out var value)
						? value
						: throw new ArgumentException("Invalid value string was returned: " + tmp);
			}

			// check if we can sum the value for all servers
			if ((ServerStats.Optable[(int)item] & ServerStats.OpAllowsSum) != ServerStats.OpAllowsSum)
				throw new ArgumentException("The " + item + " values cannot be summarized");

			// sum & return
			long result = 0;
			foreach (IPEndPoint ep in this.results.Keys)
				result += this.GetValue(ep, item);
			return result;
		}

		/// <summary>
		/// Returns the server of memcached running on the specified server.
		/// </summary>
		/// <param name="server">The adress of the server</param>
		/// <returns>The version of memcached</returns>
		public Version GetVersion(IPEndPoint server)
		{
			var version = this.GetRaw(server, StatItem.Version);
			return String.IsNullOrEmpty(version)
				? throw new ArgumentException("No version found for the server " + server)
				: new Version(version);
		}

		/// <summary>
		/// Returns the uptime of the specific server.
		/// </summary>
		/// <param name="server">The adress of the server</param>
		/// <returns>A value indicating how long the server is running</returns>
		public TimeSpan GetUptime(IPEndPoint server)
		{
			string uptime = this.GetRaw(server, StatItem.Uptime);
			return string.IsNullOrEmpty(uptime)
				? throw new ArgumentException("No uptime found for the server " + server)
				: !Int64.TryParse(uptime, out var value)
					? throw new ArgumentException("Invalid uptime string was returned: " + uptime)
					: TimeSpan.FromSeconds(value);
		}

		/// <summary>
		/// Returns the stat value for a specific server. The value is not converted but returned as the server returned it.
		/// </summary>
		/// <param name="server">The adress of the server</param>
		/// <param name="key">The name of the stat value</param>
		/// <returns>The value of the stat item</returns>
		public string GetRaw(IPEndPoint server, string key)
		{
			if (this.results.TryGetValue(server, out var serverValues))
			{
				if (serverValues.TryGetValue(key, out var result))
					return result;

				if (this._logger.IsEnabled(LogLevel.Debug))
					this._logger.LogDebug("The stat item {0} does not exist for {1}", key, server);
			}
			else
			{
				if (this._logger.IsEnabled(LogLevel.Debug))
					this._logger.LogDebug("No stats are stored for {0}", server);
			}

			return null;
		}

		/// <summary>
		/// Returns the stat value for a specific server. The value is not converted but returned as the server returned it.
		/// </summary>
		/// <param name="server">The adress of the server</param>
		/// <param name="item">The stat value to be returned</param>
		/// <returns>The value of the stat item</returns>
		public string GetRaw(IPEndPoint server, StatItem item)
		{
			return (int)item < ServerStats.StatKeys.Length && (int)item >= 0
				? this.GetRaw(server, ServerStats.StatKeys[(int)item])
				: throw new ArgumentOutOfRangeException(nameof(item));
		}

		public IEnumerable<KeyValuePair<EndPoint, string>> GetRaw(string key)
		{
			return this.results.Select(kvp => new KeyValuePair<EndPoint, string>(kvp.Key, kvp.Value.TryGetValue(key, out string tmp) ? tmp : null)).ToList();
		}
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    � 2010 Attila Kisk� (aka Enyim), � 2016 CNBlogs, � 2022 VIEApps.net
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *		http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
