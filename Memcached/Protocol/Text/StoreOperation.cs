namespace Enyim.Caching.Memcached.Protocol.Text
{
	public class StoreOperation : StoreOperationBase, IStoreOperation
	{
		readonly StoreMode mode;

        internal StoreOperation(StoreMode mode, string key, CacheItem value, uint expires) : base((StoreCommand)mode, key, value, expires, 0)
            => this.mode = mode;

        StoreMode IStoreOperation.Mode => this.mode;
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    � 2010 Attila Kisk� (aka Enyim), � 2016 CNBlogs, � 2022 VIEApps.net
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
