using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Text
{
	public class GetOperation : SingleItemOperation, IGetOperation
	{
		CacheItem result;

		internal GetOperation(string key) : base(key) { }

		protected internal override IList<ArraySegment<byte>> GetBuffer()
		{
			var command = "gets " + this.Key + TextSocketHelper.CommandTerminator;
			return TextSocketHelper.GetCommandBuffer(command);
		}

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			var response = GetHelper.ReadItem(socket);
			var result = new TextOperationResult();

			if (response == null)
				return result.Fail("Failed to read response");

			this.result = response.Item;
			this.Cas = response.CasValue;

			GetHelper.FinishCurrent(socket);

			return result.Pass();
		}

		protected internal override async Task<IOperationResult> ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default)
		{
			var response = await GetHelper.ReadItemAsync(socket, cancellationToken).ConfigureAwait(false);
			var result = new TextOperationResult();

			if (response == null)
				return result.Fail("Failed to read response");

			this.result = response.Item;
			this.Cas = response.CasValue;

			await GetHelper.FinishCurrentAsync(socket, cancellationToken).ConfigureAwait(false);

			return result.Pass();
		}

		protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
			=> throw new NotSupportedException();

		CacheItem IGetOperation.Result => this.result;
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2022 VIEApps.net
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
