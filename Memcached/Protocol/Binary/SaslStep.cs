using System;
using System.Text;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public abstract class SaslStep : BinaryOperation
	{
		protected SaslStep(ISaslAuthenticationProvider provider)
			=> this.Provider = provider;

		protected ISaslAuthenticationProvider Provider { get; private set; }

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			var response = new BinaryResponse();
			var success = response.Read(socket);

			this.StatusCode = response.StatusCode;
			this.Data = response.Data.Array;

			var result = new BinaryOperationResult
			{
				StatusCode = this.StatusCode
			};
			result.PassOrFail(success, "Failed to read response");
			return result;
		}

		protected internal override async Task<IOperationResult> ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default)
		{
			var response = new BinaryResponse();
			var success = await response.ReadAsync(socket, cancellationToken).ConfigureAwait(false);

			this.StatusCode = response.StatusCode;
			this.Data = response.Data.Array;

			var result = new BinaryOperationResult
			{
				StatusCode = this.StatusCode
			};
			result.PassOrFail(success, "Failed to read response");
			return result;
		}

		public byte[] Data { get; private set; }
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2021 VIEApps.net
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
