using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public abstract class BinarySingleItemOperation : SingleItemOperation
	{
		protected BinarySingleItemOperation(string key) : base(key) { }

		protected abstract BinaryRequest Build();

		protected internal override IList<ArraySegment<byte>> GetBuffer()
			=> this.Build().CreateBuffer();

		protected abstract IOperationResult ProcessResponse(BinaryResponse response);

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			var response = new BinaryResponse();
			var success = response.Read(socket);

			this.Cas = response.CAS;
			this.StatusCode = response.StatusCode;

			var result = new BinaryOperationResult
			{
				Success = success,
				Cas = this.Cas,
				StatusCode = this.StatusCode
			};

			IOperationResult responseResult;
			if (!(responseResult = this.ProcessResponse(response)).Success)
			{
				result.InnerResult = responseResult;
				responseResult.Combine(result);
			}

			return result;
		}

		protected internal override async Task<IOperationResult> ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default)
		{
			var response = new BinaryResponse();
			var success = await response.ReadAsync(socket, cancellationToken).ConfigureAwait(false);

			this.Cas = response.CAS;
			this.StatusCode = response.StatusCode;

			var result = new BinaryOperationResult
			{
				Success = success,
				Cas = this.Cas,
				StatusCode = this.StatusCode
			};

			IOperationResult responseResult;
			if (!(responseResult = this.ProcessResponse(response)).Success)
			{
				result.InnerResult = responseResult;
				responseResult.Combine(result);
			}

			return result;
		}

		protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
			=> throw new NotImplementedException();
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
