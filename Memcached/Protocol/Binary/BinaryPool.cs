using System;
using System.Net;

using Enyim.Reflection;
using Enyim.Caching.Configuration;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	/// <summary>
	/// Server pool implementing the binary protocol.
	/// </summary>
	public class BinaryPool : DefaultServerPool
	{
		IMemcachedClientConfiguration _configuration;
		ISaslAuthenticationProvider _authenticationProvider;

		public BinaryPool(IMemcachedClientConfiguration configuration) : base(configuration, new BinaryOperationFactory())
		{
			this._configuration = configuration;
			this._authenticationProvider = configuration.Authentication != null && !string.IsNullOrWhiteSpace(configuration.Authentication.Type)
				? FastActivator.Create(Type.GetType(configuration.Authentication.Type)) as ISaslAuthenticationProvider
				: null;
			this._authenticationProvider?.Initialize(configuration.Authentication.Parameters);
		}

		protected override IMemcachedNode CreateNode(EndPoint endpoint)
		{
			return new BinaryNode(endpoint, this._configuration.SocketPool, this._authenticationProvider);
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
