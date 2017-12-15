using System.Text;
using System.Collections.Generic;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Implements the default plain text ("PLAIN") Memcached authentication.
	/// </summary>
	/// <remarks>Either use the parametrized constructor, or pass the "userName" and "password" parameters during initalization.</remarks>
	public sealed class PlainTextAuthenticator : ISaslAuthenticationProvider
	{
		byte[] _authenticateData;

		public PlainTextAuthenticator() { }

		public PlainTextAuthenticator(string zone, string userName, string password)
		{
			this._authenticateData = PlainTextAuthenticator.CreateAuthenticateData(zone, userName, password);
		}

		string ISaslAuthenticationProvider.Type
		{
			get { return "PLAIN"; }
		}

		void ISaslAuthenticationProvider.Initialize(Dictionary<string, object> parameters)
		{
			if (parameters != null)
				this._authenticateData = PlainTextAuthenticator.CreateAuthenticateData(
					this.GetParameter(parameters, "zone"),
					this.GetParameter(parameters, "userName"),
					this.GetParameter(parameters, "password")
				);
		}

		string GetParameter(Dictionary<string, object> parameters, string key)
		{
			return parameters.ContainsKey(key)
				? (string)parameters[key]
				: throw new MemcachedClientException($"Unable to find '{key}' authentication parameter for {nameof(PlainTextAuthenticator)}");
		}

		byte[] ISaslAuthenticationProvider.Authenticate()
		{
			return this._authenticateData;
		}

		byte[] ISaslAuthenticationProvider.Continue(byte[] data)
		{
			return null;
		}

		static byte[] CreateAuthenticateData(string zone, string userName, string password)
		{
			// message  = [authzid] UTF8NUL authcid UTF8NUL passwd
			// authcid = 1*SAFE ; MUST accept up to 255 octets
			// authzid = 1*SAFE ; MUST accept up to 255 octets
			// passwd = 1*SAFE ; MUST accept up to 255 octets
			// UTF8NUL = %x00 ; UTF-8 encoded NUL character
			return Encoding.UTF8.GetBytes(zone + "\0" + userName + "\0" + password);
		}
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    � 2010 Attila Kisk� (aka Enyim), � 2016 CNBlogs, � 2018 VIEApps.net
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