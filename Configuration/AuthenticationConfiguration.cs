using System;
using System.Collections.Generic;

using Enyim.Caching.Memcached;

namespace Enyim.Caching.Configuration
{
	public class AuthenticationConfiguration : IAuthenticationConfiguration
	{
		string _type;
		Dictionary<string, object> _parameters;

		public string Type
		{
			get { return this._type; }
			set
			{
				if (!string.IsNullOrWhiteSpace(value))
					ConfigurationHelper.CheckForInterface(System.Type.GetType(value), typeof(ISaslAuthenticationProvider));
				this._type = value;
			}
		}

		public Dictionary<string, object> Parameters
		{
			get
			{
				return this._parameters ?? (this._parameters = new Dictionary<string, object>());
			}
		}
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2019 VIEApps.net
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
