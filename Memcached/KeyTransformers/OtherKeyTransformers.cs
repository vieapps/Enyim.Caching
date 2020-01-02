using System;
using System.Text;
using System.Security.Cryptography;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// A key transformer which converts the item keys into Base64.
	/// </summary>
	public class Base64KeyTransformer : KeyTransformerBase
	{
		public override string Transform(string key)
			=> Convert.ToBase64String(Encoding.UTF8.GetBytes(key));
	}

	/// <summary>
	/// A key transformer which converts the item keys into their SHA1 hash.
	/// </summary>
	public class SHA1KeyTransformer : KeyTransformerBase
	{
		public override string Transform(string key)
		{
			using (var hasher = SHA1.Create())
			{
				return Convert.ToBase64String(hasher.ComputeHash(Encoding.Unicode.GetBytes(key)));
			}
		}
	}

	/// <summary>
	/// A key transformer which converts the item keys into their MD5 hash.
	/// </summary>
	public class MD5KeyTransformer : KeyTransformerBase
	{
		public override string Transform(string key)
		{
			using (var hasher = MD5.Create())
			{
				return Convert.ToBase64String(hasher.ComputeHash(Encoding.Unicode.GetBytes(key)));
			}
		}
	}

	/// <summary>
	/// A key transformer which converts the item keys into their FNV64 hash.
	/// </summary>
	public class FNV64HashKeyTransformer : KeyTransformerBase
	{
		public override string Transform(string key)
		{
			using (var hasher = new FNV64())
			{
				return Convert.ToBase64String(hasher.ComputeHash(Encoding.Unicode.GetBytes(key)));
			}
		}
	}

	/// <summary>
	/// A key transformer which converts the item keys into their CRC32 hash.
	/// </summary>
	public class CRC32HashKeyTransformer : KeyTransformerBase
	{
		public override string Transform(string key)
		{
			using (var hasher = new HashkitCrc32())
			{
				return Convert.ToBase64String(hasher.ComputeHash(Encoding.Unicode.GetBytes(key)));
			}
		}
	}

	/// <summary>
	/// A key transformer which converts the item keys into their Murmur hash.
	/// </summary>
	public class MurmurHashKeyTransformer : KeyTransformerBase
	{
		public override string Transform(string key)
		{
			using (var hasher = new HashkitMurmur())
			{
				return Convert.ToBase64String(hasher.ComputeHash(Encoding.Unicode.GetBytes(key)));
			}
		}
	}

	/// <summary>
	/// A key transformer which converts the item keys into their OneAtATime hash.
	/// </summary>
	public class OneAtATimeHashKeyTransformer : KeyTransformerBase
	{
		public override string Transform(string key)
		{
			using (var hasher = new HashkitOneAtATime())
			{
				return Convert.ToBase64String(hasher.ComputeHash(Encoding.Unicode.GetBytes(key)));
			}
		}
	}

	/// <summary>
	/// A key transformer which converts the item keys into their Tiger hash.
	/// </summary>
	public class TigerHashKeyTransformer : KeyTransformerBase
	{
		public override string Transform(string key)
		{
			using (var hasher = new TigerHash())
			{
				return Convert.ToBase64String(hasher.ComputeHash(Encoding.Unicode.GetBytes(key)));
			}
		}
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
