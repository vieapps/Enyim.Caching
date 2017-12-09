using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class FlushOperation : BinaryOperation, IFlushOperation
	{
		public FlushOperation() { }

		protected override BinaryRequest Build()
		{
			return new BinaryRequest(OpCode.Flush);
		}

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			var response = new BinaryResponse();
			var success = response.Read(socket);

			this.StatusCode = StatusCode;
			var result = new BinaryOperationResult()
			{
				Success = success,
				StatusCode = this.StatusCode
			};

			result.PassOrFail(success, "Failed to read response");
			return result;
		}

		protected internal override async Task<IOperationResult> ReadResponseAsync(PooledSocket socket)
		{
			var response = new BinaryResponse();
			var success = await response.ReadAsync(socket).ConfigureAwait(false);

			this.StatusCode = StatusCode;
			var result = new BinaryOperationResult()
			{
				Success = success,
				StatusCode = this.StatusCode
			};

			result.PassOrFail(success, "Failed to read response");
			return result;
		}
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2018 VIEApps.net
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
