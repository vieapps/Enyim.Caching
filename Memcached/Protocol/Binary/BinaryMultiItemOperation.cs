using System;
using System.Collections.Generic;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public abstract class BinaryMultiItemOperation : MultiItemOperation
	{
		public BinaryMultiItemOperation(IList<string> keys) : base(keys) { }

		protected abstract BinaryRequest Build(string key);

		protected internal override IList<ArraySegment<byte>> GetBuffer()
		{
			var keys = this.Keys;
			var result = new List<ArraySegment<byte>>(keys.Count * 2);

			foreach (var key in keys)
				this.Build(key).CreateBuffer(result);

			return result;
		}
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    � 2010 Attila Kisk� (aka Enyim), � 2016 CNBlogs, � 2021 VIEApps.net
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
