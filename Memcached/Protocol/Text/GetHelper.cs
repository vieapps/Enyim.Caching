using System;
using System.Globalization;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Text
{
	internal static class GetHelper
	{
		public static void FinishCurrent(PooledSocket socket)
		{
			string response = TextSocketHelper.ReadResponse(socket);

			if (String.Compare(response, "END", StringComparison.Ordinal) != 0)
				throw new MemcachedClientException("No END was received.");
		}

		public static GetResponse ReadItem(PooledSocket socket)
		{
			var description = TextSocketHelper.ReadResponse(socket);
			if (String.Compare(description, "END", StringComparison.Ordinal) == 0)
				return null;

			else if (description.Length < 6 || String.Compare(description, 0, "VALUE ", 0, 6, StringComparison.Ordinal) != 0)
				throw new MemcachedClientException("No VALUE response received.\r\n" + description);

			// response is:
			// VALUE <key> <flags> <bytes> [<cas unique>]
			// 0     1     2       3       4
			//
			// cas only exists in 1.2.4+
			//

			ulong cas = 0;
			var parts = description.Split(' ');
			if (parts.Length == 5)
			{
				if (!UInt64.TryParse(parts[4], out cas))
					throw new MemcachedClientException("Invalid CAS VALUE received.");
			}
			else if (parts.Length < 4)
				throw new MemcachedClientException("Invalid VALUE response received: " + description);

			var flags = UInt16.Parse(parts[2], CultureInfo.InvariantCulture);
			var length = Int32.Parse(parts[3], CultureInfo.InvariantCulture);

			var allData = new byte[length];
			var eod = new byte[2];

			socket.Read(allData, 0, length);
			socket.Read(eod, 0, 2); // data is terminated by \r\n

			var result = new GetResponse(parts[1], flags, cas, allData);

			GetHelper.Logger = GetHelper.Logger ?? Caching.Logger.CreateLogger(typeof(GetHelper));
			if (GetHelper.Logger.IsEnabled(LogLevel.Debug))
				GetHelper.Logger.LogDebug("Received value. Data type: {0}, size: {1}.", result.Item.Flags, result.Item.Data.Count);

			return result;
		}

		static ILogger Logger = null;
	}

	#region [ T:GetResponse                  ]
	public class GetResponse
	{
		GetResponse() { }

		public GetResponse(string key, ushort flags, ulong casValue, byte[] data) : this(key, flags, casValue, data, 0, data.Length) { }

		public GetResponse(string key, ushort flags, ulong casValue, byte[] data, int offset, int count)
		{
			this.Key = key;
			this.CasValue = casValue;

			this.Item = new CacheItem(flags, new ArraySegment<byte>(data, offset, count));
		}

		public readonly string Key;
		public readonly ulong CasValue;
		public readonly CacheItem Item;
	}
	#endregion

}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2017 VIEApps.net
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
