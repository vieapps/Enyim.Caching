using System;

namespace Enyim.Caching.Memcached.Protocol.Text
{
	public class ConcateOperation : StoreOperationBase, IConcatOperation
	{
		private ConcatenationMode mode;

		internal ConcateOperation(ConcatenationMode mode, string key, ArraySegment<byte> data) : base(mode == ConcatenationMode.Append ? StoreCommand.Append : StoreCommand.Prepend, key, new CacheItem() { Data = data, Flags = 0 }, 0, 0)
            => this.mode = mode;

        ConcatenationMode IConcatOperation.Mode => this.mode;

        protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
            => throw new NotSupportedException();
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
