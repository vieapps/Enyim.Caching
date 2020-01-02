using System.Net;
using System.Collections.Generic;

using Enyim.Caching.Memcached;

namespace Enyim.Caching.Configuration
{
	/// <summary>
	/// Defines an interface for configuring the <see cref="MemcachedClient"/>.
	/// </summary>
	public interface IMemcachedClientConfiguration
	{
		/// <summary>
		/// Gets a list of <see cref="IPEndPoint"/> each representing a Memcached server in the pool.
		/// </summary>
		IList<EndPoint> Servers { get; }

		/// <summary>
		/// Gets the configuration of the socket pool.
		/// </summary>
		ISocketPoolConfiguration SocketPool { get; }

		/// <summary>
		/// Gets the authentication settings.
		/// </summary>
		IAuthenticationConfiguration Authentication { get; }

		/// <summary>
		/// Creates an <see cref="IKeyTransformer"/> instance which will be used to convert item keys for Memcached.
		/// </summary>
		IKeyTransformer CreateKeyTransformer();

		/// <summary>
		/// Creates an <see cref="INodeLocator"/> instance which will be used to assign items to Memcached nodes.
		/// </summary>
		INodeLocator CreateNodeLocator();

		/// <summary>
		/// Creates an <see cref="ITranscoder"/> instance which will be used to serialize or deserialize items.
		/// </summary>
		ITranscoder CreateTranscoder();

		IServerPool CreatePool();

	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2020 VIEApps.net
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
