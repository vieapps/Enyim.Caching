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

		ISaslAuthenticationProvider _authProvider;

		public BinaryNode(EndPoint endpoint, ISocketPoolConfiguration config, ISaslAuthenticationProvider authenticationProvider, ILogger logger) : base(endpoint, config, logger)
		{
			this._authProvider = authenticationProvider;
			this._logger = logger;
		}

		/// <summary>
		/// Authenticates the new socket before it is put into the pool.
		/// </summary>
		protected internal override PooledSocket CreateSocket()
		{
			var socket = base.CreateSocket();

			if (this._authProvider != null && !this.Auth(socket))
			{
				this._logger.LogError("Authentication failed: " + this.EndPoint);
				throw new SecurityException("Authentication failed: " + this.EndPoint);
			}

			return socket;
		}

		/// <summary>
		/// Implements memcached's SASL auth sequence. (See the protocol docs for more details.)
		/// </summary>
		/// <param name="socket"></param>
		/// <returns></returns>
		bool Auth(PooledSocket socket)
		{
			SaslStep currentStep = new SaslStart(this._authProvider);

			socket.Write(currentStep.GetBuffer());

			while (!currentStep.ReadResponse(socket).Success)
			{
				// challenge-response authentication
				if (currentStep.StatusCode == 0x21)
				{
					currentStep = new SaslContinue(this._authProvider, currentStep.Data);
					socket.Write(currentStep.GetBuffer());
				}
				else
				{
					this._logger.LogWarning("Authentication failed, return code: 0x{0:x}", currentStep.StatusCode);

					// invalid credentials or other error
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
