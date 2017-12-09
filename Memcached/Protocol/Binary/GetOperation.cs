using Enyim.Caching.Memcached.Results;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class GetOperation : BinarySingleItemOperation, IGetOperation
	{
		CacheItem result;

		public GetOperation(string key) : base(key) { }

		protected override BinaryRequest Build()
		{
			return new BinaryRequest(OpCode.Get)
			{
				Key = this.Key,
				Cas = this.Cas
			};
		}

		protected override IOperationResult ProcessResponse(BinaryResponse response)
		{
			var status = response.StatusCode;
			var result = new BinaryOperationResult();

			this.StatusCode = status;
			if (status == 0)
			{
				int flags = BinaryConverter.DecodeInt32(response.Extra, 0);
				this.result = new CacheItem((ushort)flags, response.Data);
				this.Cas = response.CAS;

				return result.Pass();
			}

			this.Cas = 0;
			return result.Fail(OperationResultHelper.ProcessResponseData(response.Data));
		}

		CacheItem IGetOperation.Result
		{
			get { return this.result; }
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
