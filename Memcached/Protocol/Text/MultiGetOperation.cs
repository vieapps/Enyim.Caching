using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Text
{
	public class MultiGetOperation : MultiItemOperation, IMultiGetOperation
	{
		readonly ILogger _logger;

		Dictionary<string, CacheItem> _result;

		public MultiGetOperation(IList<string> keys) : base(keys)
			=> this._logger = Logger.CreateLogger<MultiGetOperation>();

		protected internal override IList<ArraySegment<byte>> GetBuffer()
		{
			var command = "gets " + String.Join(" ", Keys.ToArray()) + TextSocketHelper.CommandTerminator;
			return TextSocketHelper.GetCommandBuffer(command);
		}

		Dictionary<string, CacheItem> IMultiGetOperation.Result => this._result;

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			var result = new Dictionary<string, CacheItem>();
			var cas = new Dictionary<string, ulong>();
			try
			{
				GetResponse response;
				while ((response = GetHelper.ReadItem(socket)) != null)
				{
					var key = response.Key;

					result[key] = response.Item;
					cas[key] = response.CasValue;
				}
			}
			catch (NotSupportedException)
			{
				throw;
			}
			catch (Exception e)
			{
				this._logger.LogError(e, "Error occurred while performing multi-get");
			}

			this._result = result;
			this.Cas = cas;

			return new TextOperationResult().Pass();
		}

		protected internal override async Task<IOperationResult> ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default)
		{
			var result = new Dictionary<string, CacheItem>();
			var cas = new Dictionary<string, ulong>();
			try
			{
				GetResponse response;
				while ((response = await GetHelper.ReadItemAsync(socket, cancellationToken).ConfigureAwait(false)) != null)
				{
					var key = response.Key;

					result[key] = response.Item;
					cas[key] = response.CasValue;
				}
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (NotSupportedException)
			{
				throw;
			}
			catch (Exception e)
			{
				this._logger.LogError(e, "Error occurred while performing multi-get");
			}

			this._result = result;
			this.Cas = cas;

			return new TextOperationResult().Pass();
		}

		protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
			=> throw new NotSupportedException();
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
