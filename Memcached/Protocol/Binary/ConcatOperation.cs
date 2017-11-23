using System;

using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	/// <summary>
	/// Implements append/prepend.
	/// </summary>
	public class ConcatOperation : BinarySingleItemOperation, IConcatOperation
	{
		ArraySegment<byte> _data;
		ConcatenationMode _mode;

		public ConcatOperation(ConcatenationMode mode, string key, ArraySegment<byte> data) : base(key)
		{
			this._data = data;
			this._mode = mode;
		}

		protected override BinaryRequest Build()
		{
			return new BinaryRequest((OpCode)this._mode)
			{
				Key = this.Key,
				Cas = this.Cas,
				Data = this._data
			};
		}

		protected override IOperationResult ProcessResponse(BinaryResponse response)
		{
			return new BinaryOperationResult() { Success = true };
		}

		ConcatenationMode IConcatOperation.Mode
		{
			get { return this._mode; }
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
