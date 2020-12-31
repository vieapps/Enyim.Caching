using System;
using System.Collections.Generic;
using System.Linq;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// This is a simple node locator with no computation overhead, always returns the first server from the list. Use only in single server deployments.
	/// </summary>
	public sealed class SingleNodeLocator : INodeLocator
	{
		IMemcachedNode _node;
		bool _isInitialized;

		void INodeLocator.Initialize(IList<IMemcachedNode> nodes)
		{
			if (nodes.Count > 0)
				this._node = nodes[0];
			this._isInitialized = true;
		}

		IMemcachedNode INodeLocator.Locate(string key)
			=> !this._isInitialized
				? throw new InvalidOperationException("You must call Initialize first")
				: this._node;

		IEnumerable<IMemcachedNode> INodeLocator.GetWorkingNodes()
			=> this._node.IsAlive
				? new IMemcachedNode[] { this._node }
				: Enumerable.Empty<IMemcachedNode>();
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
