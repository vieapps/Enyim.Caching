using System;
using System.Net;
using System.Collections.Generic;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Text
{
	public class StatsOperation : Operation, IStatsOperation
	{
		ILogger _logger;

		string _type;
		Dictionary<string, string> _result;

		public StatsOperation(string type)
		{
			this._logger = LogManager.CreateLogger<StatsOperation>();
			this._type = type;
		}

		protected internal override IList<ArraySegment<byte>> GetBuffer()
		{
			var command = String.IsNullOrEmpty(this._type)
				? "stats" + TextSocketHelper.CommandTerminator
				: "stats " + this._type + TextSocketHelper.CommandTerminator;
			return TextSocketHelper.GetCommandBuffer(command);
		}

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			var serverData = new Dictionary<string, string>();

			while (true)
			{
				string line = TextSocketHelper.ReadResponse(socket);

				// stat values are terminated by END
				if (String.Compare(line, "END", StringComparison.Ordinal) == 0)
					break;

				// expected response is STAT item_name item_value
				if (line.Length < 6 || String.Compare(line, 0, "STAT ", 0, 5, StringComparison.Ordinal) != 0)
				{
					if (this._logger.IsEnabled(LogLevel.Warning))
						this._logger.LogWarning("Unknow response: " + line);

					continue;
				}

				// get the key&value
				string[] parts = line.Remove(0, 5).Split(' ');
				if (parts.Length != 2)
				{
					if (this._logger.IsEnabled(LogLevel.Warning))
						this._logger.LogWarning("Unknow response: " + line);

					continue;
				}

				// store the stat item
				serverData[parts[0]] = parts[1];
			}

			this._result = serverData;

			return new TextOperationResult().Pass();
		}

		Dictionary<string, string> IStatsOperation.Result
		{
			get { return _result; }
		}

		protected internal override Task<IOperationResult> ReadResponseAsync(PooledSocket socket)
		{
			return Task.FromResult(this.ReadResponse(socket));
		}

		protected internal override bool ReadResponseAsync(PooledSocket socket, System.Action<bool> next)
		{
			throw new System.NotSupportedException();
		}
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
