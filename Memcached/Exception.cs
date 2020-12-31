using System;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// The exception that is thrown when a client error occures during communicating with the Memcached servers.
	/// </summary>
	[Serializable]
	public class MemcachedClientException : Exception
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="MemcachedClientException"/> class.
		/// </summary>
		public MemcachedClientException() { }

		/// <summary>
		/// Initializes a new instance of the <see cref="MemcachedClientException"/> class with a specified error message.
		/// </summary>
		public MemcachedClientException(string message) : base(message) { }

		/// <summary>
		/// Initializes a new instance of the <see cref="MemcachedClientException"/> class with a specified error message and a reference to the inner exception that is the cause of this exception.
		/// </summary>
		public MemcachedClientException(string message, Exception inner) : base(message, inner) { }
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
