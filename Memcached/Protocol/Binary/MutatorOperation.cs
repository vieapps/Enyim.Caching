using System;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class MutatorOperation : BinarySingleItemOperation, IMutatorOperation
	{
		readonly ulong _defaultValue;
		readonly ulong _delta;
		readonly uint _expires;
		MutationMode _mode;
		ulong _result;

		public MutatorOperation(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires) : base(key)
		{
			if (delta < 0)
				throw new ArgumentOutOfRangeException(nameof(delta), $"{nameof(delta)} must be >= 0");

			this._defaultValue = defaultValue;
			this._delta = delta;
			this._expires = expires;
			this._mode = mode;
		}

		protected unsafe void UpdateExtra(BinaryRequest request)
		{
			byte[] extra = new byte[20];

			fixed (byte* buffer = extra)
			{
				BinaryConverter.EncodeUInt64(this._delta, buffer, 0);
				BinaryConverter.EncodeUInt64(this._defaultValue, buffer, 8);
				BinaryConverter.EncodeUInt32(this._expires, buffer, 16);
			}

			request.Extra = new ArraySegment<byte>(extra);
		}

		protected override BinaryRequest Build()
		{
			var request = new BinaryRequest((OpCode)this._mode)
			{
				Key = this.Key,
				Cas = this.Cas
			};

			this.UpdateExtra(request);

			return request;
		}

		protected override IOperationResult ProcessResponse(BinaryResponse response)
		{
			var result = new BinaryOperationResult();
			var status = response.StatusCode;
			this.StatusCode = status;

			if (status == 0)
			{
				var data = response.Data;
				if (data.Count != 8)
					return result.Fail("Result must be 8 bytes long, received: " + data.Count, new InvalidOperationException());

				this._result = BinaryConverter.DecodeUInt64(data.Array, data.Offset);
				return result.Pass();
			}

			var message = OperationResultHelper.ProcessResponseData(response.Data);
			return result.Fail(message);
		}

		MutationMode IMutatorOperation.Mode => this._mode;

		ulong IMutatorOperation.Result => this._result;
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
