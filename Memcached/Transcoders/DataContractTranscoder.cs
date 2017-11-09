using System;
using System.IO;

using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Default <see cref="T:Enyim.Caching.Memcached.ITranscoder"/> implementation.
	/// Primitive types are manually serialized, the rest is serialized using Json.NET Bson.
	/// </summary>
	public class DataContractTranscoder : DefaultTranscoder
	{
		/// <summary>
		/// Serializes an object into array of bytes using BSON
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		protected override ArraySegment<byte> SerializeObject(object value)
		{
			using (var stream = new MemoryStream())
			{
				using (var writer = new BsonDataWriter(stream))
				{
					(new JsonSerializer()).Serialize(writer, value);
					return new ArraySegment<byte>(stream.ToArray(), 0, (int)stream.Length);
				}
			}
		}

		/// <summary>
		/// Deserializes an object from array of bytes using BSON
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		protected override object DeserializeObject(ArraySegment<byte> value)
		{
			using (var stream = new MemoryStream(value.Array, value.Offset, value.Count))
			{
				using (var reader = new BsonDataReader(stream))
				{
					return (new JsonSerializer()).Deserialize(reader);
				}
			}
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
