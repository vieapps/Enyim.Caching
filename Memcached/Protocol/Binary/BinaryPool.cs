using System;
using System.Net;

using Enyim.Caching.Configuration;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	/// <summary>
	/// Server pool implementing the binary protocol.
	/// </summary>
	public class BinaryPool : DefaultServerPool
	{
		ISaslAuthenticationProvider authenticationProvider;
		IMemcachedClientConfiguration configuration;
		private readonly ILogger _logger;

		public BinaryPool(IMemcachedClientConfiguration configuration, ILogger logger) : base(configuration, new BinaryOperationFactory(logger), logger)
		{
			this.authenticationProvider = GetProvider(configuration);
			this.configuration = configuration;
			this._logger = logger;
		}

		protected override IMemcachedNode CreateNode(EndPoint endpoint)
		{
			return new BinaryNode(endpoint, this.configuration.SocketPool, this.authenticationProvider, this._logger);
		}

		private static ISaslAuthenticationProvider GetProvider(IMemcachedClientConfiguration configuration)
		{
			// create&initialize the authenticator, if any
			// we'll use this single instance everywhere, so it must be thread safe
			if (configuration.Authentication != null)
			{
				var provider = configuration.Authentication.Type == null
					? null
					: Reflection.FastActivator.Create(configuration.Authentication.Type) as ISaslAuthenticationProvider;

				if (provider != null)
				{
					provider.Initialize(configuration.Authentication.Parameters);
					return provider;
				}
			}

			return null;
		}
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk? enyim.com
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
