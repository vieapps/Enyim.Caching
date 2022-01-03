using System.Net;
using System.Security;

using Enyim.Caching.Configuration;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	/// <summary>
	/// A node which is used by the BinaryPool. It implements the binary protocol's SASL authentication mechanism.
	/// </summary>
	public class BinaryNode : MemcachedNode
	{
		readonly ILogger _logger;
		readonly ISaslAuthenticationProvider _authenticationProvider;

		public BinaryNode(EndPoint endpoint, ISocketPoolConfiguration config, ISaslAuthenticationProvider authenticationProvider) : base(endpoint, config)
		{
			this._authenticationProvider = authenticationProvider;
			this._logger = Logger.CreateLogger<BinaryNode>();
		}

		/// <summary>
		/// Authenticates the new socket before it is put into the pool.
		/// </summary>
		protected internal override PooledSocket CreateSocket()
		{
			var socket = base.CreateSocket();

			if (this._authenticationProvider != null && !this.Authenticate(socket))
			{
				this._logger.LogError($"Authentication failed: {this.EndPoint}");
				throw new SecurityException($"Authentication failed: {this.EndPoint}");
			}

			return socket;
		}

		/// <summary>
		/// Implements memcached's SASL authenticate sequence (see the protocol docs for more details.)
		/// </summary>
		/// <param name="socket"></param>
		/// <returns></returns>
		bool Authenticate(PooledSocket socket)
		{
			SaslStep step = new SaslStart(this._authenticationProvider);
			socket.Send(step.GetBuffer());

			while (!step.ReadResponse(socket).Success)
			{
				// challenge-response authentication
				if (step.StatusCode == 0x21)
				{
					step = new SaslContinue(this._authenticationProvider, step.Data);
					socket.Send(step.GetBuffer());
				}

				// invalid credentials or other error
				else
				{
					this._logger.LogWarning("Authentication failed, return code: 0x{0:x}", step.StatusCode);
					return false;
				}
			}

			return true;
		}
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
