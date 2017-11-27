using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class StatsOperation : BinaryOperation, IStatsOperation
	{
		string _type;
		Dictionary<string, string> _result;

		public StatsOperation(string type)
		{
			this._type = type;
		}

		protected override BinaryRequest Build()
		{
			var request = new BinaryRequest(OpCode.Stat);
			if (!String.IsNullOrEmpty(this._type))
				request.Key = this._type;

			return request;
		}

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			var response = new BinaryResponse();
			var serverData = new Dictionary<string, string>();
			var success = false;

			while (response.Read(socket) && response.KeyLength > 0)
			{
				success = true;

				var data = response.Data;
				var key = BinaryConverter.DecodeKey(data.Array, data.Offset, response.KeyLength);
				var value = BinaryConverter.DecodeKey(data.Array, data.Offset + response.KeyLength, data.Count - response.KeyLength);

				serverData[key] = value;
			}

			this._result = serverData;
			this.StatusCode = response.StatusCode;

			var result = new BinaryOperationResult()
			{
				StatusCode = StatusCode
			};

			result.PassOrFail(success, "Failed to read response");
			return result;
		}

        protected internal override async Task<IOperationResult> ReadResponseAsync(PooledSocket socket)
        {
			var response = new BinaryResponse();
			var serverData = new Dictionary<string, string>();
			var success = false;

			while (await response.ReadAsync(socket).ConfigureAwait(false) && response.KeyLength > 0)
			{
				success = true;

				var data = response.Data;
				var key = BinaryConverter.DecodeKey(data.Array, data.Offset, response.KeyLength);
				var value = BinaryConverter.DecodeKey(data.Array, data.Offset + response.KeyLength, data.Count - response.KeyLength);

				serverData[key] = value;
			}

			this._result = serverData;
			this.StatusCode = response.StatusCode;

			var result = new BinaryOperationResult()
			{
				StatusCode = StatusCode
			};

			result.PassOrFail(success, "Failed to read response");
			return result;
		}

		Dictionary<string, string> IStatsOperation.Result
		{
			get { return this._result; }
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
