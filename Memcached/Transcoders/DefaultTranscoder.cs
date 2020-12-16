using System;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Default <see cref="ITranscoder"/> implementation.
	/// Primitive types are manually serialized, the rest is serialized using <see cref="System.Runtime.Serialization.Formatters.Binary.BinaryFormatter"/>.
	/// </summary>
	public class DefaultTranscoder : ITranscoder
	{
		protected virtual ArraySegment<byte> SerializeObject(object value)
			=> new ArraySegment<byte>(CacheUtils.Helper.Serialize(value).Item2);

		protected virtual CacheItem Serialize(object value)
		{
			// ArraySegment<byte> is only passed in when a part of buffer is being serialized,
			// usually from a MemoryStream (to avoid duplicating arrays, the byte[] returned by MemoryStream.GetBuffer is placed into an ArraySegment)
			if (value != null && value is ArraySegment<byte> segment)
				return new CacheItem(flags: CacheUtils.Helper.FlagOfRawData, data: segment);

			// or we just received a byte[], means no further processing is needed
			if (value != null && value is byte[] bytes)
				return new CacheItem(flags: CacheUtils.Helper.FlagOfRawData, data: new ArraySegment<byte>(bytes));

			// prepare type code
			var typeCode = value == null
				? TypeCode.DBNull
				: Type.GetTypeCode(value.GetType());

			// serialize object
			if (typeCode.Equals(TypeCode.Object))
				return new CacheItem(flags: (int)TypeCode.Object | 0x0100, data: this.SerializeObject(value));

			// serialize primitive
			var data = CacheUtils.Helper.Serialize(value);
			return new CacheItem(flags: (uint)data.Item1, data: new ArraySegment<byte>(data.Item2));
		}

		CacheItem ITranscoder.Serialize(object value)
			=> this.Serialize(value);

		protected virtual object DeserializeObject(ArraySegment<byte> value)
			=> CacheUtils.Helper.Deserialize(value.Array, (int)TypeCode.Object | 0x0100, value.Offset, value.Count);

		protected virtual object Deserialize(CacheItem item)
		{
			if (item.Data == null || item.Data.Array == null)
				return null;

			// raw data
			if (item.Flags == CacheUtils.Helper.FlagOfRawData)
			{
				var tmp = item.Data;
				if (tmp.Count == tmp.Array.Length)
					return tmp.Array;

				// we should never arrive here, but it's better to be safe than sorry
				var result = new byte[tmp.Count];
				Buffer.BlockCopy(tmp.Array, tmp.Offset, result, 0, tmp.Count);
				return result;
			}

			// prepare
			var typeCode = (TypeCode)((int)item.Flags & 0xff);

			// incrementing a non-existing key then getting it returns as a string,
			// but the flag will be 0 so treat all 0 flagged items as string this may help inter-client data management as well
			// however we store 'null' as Empty + an empty array,  so this must special-cased for compatibilty with  earlier versions (we introduced DBNull as null marker in emc2.6)
			if (typeCode.Equals(TypeCode.Empty))
				return (item.Data.Array == null || item.Data.Count == 0)
					? null
					: CacheUtils.Helper.Deserialize(item.Data.Array, (int)TypeCode.String | 0x0100, item.Data.Offset, item.Data.Count);

			// object
			if (typeCode.Equals(TypeCode.Object))
				return this.DeserializeObject(item.Data);

			// primitive
			return CacheUtils.Helper.Deserialize(item.Data.Array, (int)item.Flags, item.Data.Offset, item.Data.Count);
		}

		object ITranscoder.Deserialize(CacheItem item)
			=> this.Deserialize(item);

		T ITranscoder.Deserialize<T>(CacheItem item)
		{
			var @object = this.Deserialize(item);
			return @object != null && @object is T instance
				? instance
				: default;
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
