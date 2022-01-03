using System;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class StoreOperation : BinarySingleItemOperation, IStoreOperation
	{
		readonly StoreMode _mode;
		readonly uint _expires;
		CacheItem _value;

		public StoreOperation(StoreMode mode, string key, CacheItem value, uint expires) : base(key)
		{
			this._mode = mode;
			this._value = value;
			this._expires = expires;
		}

		protected override BinaryRequest Build()
		{
			OpCode op;
			switch (this._mode)
			{
				case StoreMode.Set:
					op = OpCode.Set;
					break;

				case StoreMode.Add:
					op = OpCode.Add;
					break;

				case StoreMode.Replace:
					op = OpCode.Replace;
					break;

				default:
					throw new ArgumentOutOfRangeException("mode", $"{this._mode} is not supported");
			}

			var extra = new byte[8];

			BinaryConverter.EncodeUInt32((uint)this._value.Flags, extra, 0);
			BinaryConverter.EncodeUInt32(this._expires, extra, 4);

			var request = new BinaryRequest(op)
			{
				Key = this.Key,
				Cas = this.Cas,
				Extra = new ArraySegment<byte>(extra),
				Data = this._value.Data
			};

			return request;
		}

		protected override IOperationResult ProcessResponse(BinaryResponse response)
		{
			this.StatusCode = response.StatusCode;
			var result = new BinaryOperationResult();
			return response.StatusCode == 0
				? result.Pass()
				: result.Fail(OperationResultHelper.ProcessResponseData(response.Data));
		}

		StoreMode IStoreOperation.Mode => this._mode;

		protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
			=> throw new NotSupportedException();
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
