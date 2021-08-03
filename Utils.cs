#region Related components
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IO;
using MsgPack;
using MsgPack.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Enyim.Caching;
using Enyim.Caching.Configuration;
#endregion

namespace CacheUtils
{
	public static class Helper
	{

		#region Work with caching data
		/// <summary>
		/// Gets the Unix epoch
		/// </summary>
		public static DateTime UnixEpoch { get; } = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		/// <summary>
		/// Converts this date-time to time-span with Unix epoch
		/// </summary>
		/// <param name="datetime"></param>
		/// <returns></returns>
		public static TimeSpan ToUnixTimeSpan(this DateTime datetime)
			=> datetime < Helper.UnixEpoch
				? throw new ArgumentOutOfRangeException(nameof(datetime), $"{nameof(datetime)} must be >= 1970/1/1")
				: datetime.ToUniversalTime() - Helper.UnixEpoch;

		/// <summary>
		/// Converts this date-time to time-span
		/// </summary>
		/// <param name="datetime"></param>
		/// <param name="useUTC"></param>
		/// <returns></returns>
		public static TimeSpan ToTimeSpan(this DateTime datetime, bool useUTC = false)
			=> datetime == DateTime.MaxValue
				? TimeSpan.Zero
				: datetime < DateTime.Now
					? TimeSpan.FromMilliseconds(1)
					: useUTC
						? datetime.ToUniversalTime() - DateTime.Now.ToUniversalTime()
						: datetime.ToLocalTime() - DateTime.Now;

		/// <summary>
		/// Gets the expiration of the date-time value
		/// </summary>
		/// <param name="expiresAt"></param>
		/// <returns></returns>
		public static uint GetExpiration(this DateTime expiresAt)
			=> expiresAt < Helper.UnixEpoch
				? throw new ArgumentOutOfRangeException(nameof(expiresAt), $"{nameof(expiresAt)} must be >= 1970/1/1")
				: expiresAt == DateTime.MaxValue
					? 0
					: (uint)expiresAt.ToUnixTimeSpan().TotalSeconds;

		/// <summary>
		/// Gets the expiration of the time-span value
		/// </summary>
		/// <param name="validFor"></param>
		/// <returns></returns>
		public static uint GetExpiration(this TimeSpan validFor)
			=> validFor == TimeSpan.Zero || validFor == TimeSpan.MaxValue
				? 0
				: DateTime.Now.Add(validFor).GetExpiration();

		/// <summary>
		/// Gets the expiration of the distributed cache entry options
		/// </summary>
		/// <param name="options"></param>
		/// <returns></returns>
		public static object GetExpiration(this DistributedCacheEntryOptions options)
			=> options.SlidingExpiration != null && options.AbsoluteExpiration != null
				? throw new ArgumentException("You cannot specify both sliding expiration and absolute expiration")
				: options.AbsoluteExpiration != null
					? options.AbsoluteExpiration.Value.ToUnixTimeSeconds()
					: options.AbsoluteExpirationRelativeToNow != null
						? (DateTimeOffset.UtcNow + options.AbsoluteExpirationRelativeToNow.Value).ToUnixTimeSeconds()
						: options.SlidingExpiration == null || options.SlidingExpiration.Value == TimeSpan.Zero || options.SlidingExpiration.Value == TimeSpan.MaxValue
							? TimeSpan.Zero
							: (object)DateTime.Now.Add(options.SlidingExpiration.Value).ToTimeSpan();

		/// <summary>
		/// Gets the key for storing related information of an IDistributedCache item
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		public static string GetIDistributedCacheExpirationKey(this string key)
			=> $"i-distributed-cache#{key}";

		/// <summary>
		/// Concatenates the arrays of bytes
		/// </summary>
		/// <param name="arrays">The arrays of bytes to concatenate</param>
		/// <returns></returns>
		public static byte[] Concat(IEnumerable<byte[]> arrays)
		{
			if (arrays == null || !arrays.Any())
				return Array.Empty<byte>();

			var offset = 0;
			var data = arrays.Where(array => array != null).ToList();
			var result = new byte[data.Sum(array => array.Length)];
			data.ForEach(array =>
			{
				Buffer.BlockCopy(array, 0, result, offset, array.Length);
				offset += array.Length;
			});
			return result;
		}

		/// <summary>
		/// Splits the array of bytes into the sub-arrays of bytes with the specified size
		/// </summary>
		/// <param name="bytes">The arrays of bytes to split</param>
		/// <param name="size">The size (count/length of one array)</param>
		/// <returns></returns>
		public static IEnumerable<byte[]> Split(byte[] bytes, int size)
		{
			var result = new List<byte[]>();
			if (bytes == null)
				return result;

			if (size < 1 || bytes.Length < 1)
			{
				result.Add(bytes);
				return result;
			}

			var offset = 0;
			var length = bytes.Length;
			while (offset < bytes.Length)
			{
				var count = size > length ? length : size;
				var block = new byte[count];
				Buffer.BlockCopy(bytes, offset, block, 0, count);
				result.Add(block);
				offset += count;
				length -= count;
			}
			return result;
		}
		#endregion

		#region Serialize & Deserialize
		/// <summary>
		/// Gets the flag of raw data
		/// </summary>
		public const int FlagOfRawData = 0xfa52;

		static RecyclableMemoryStreamManager RecyclableMemoryStreamManager { get; } = new RecyclableMemoryStreamManager();

		static Regex MsgPackTypeRegex { get; } = new Regex(@", Version=\d+.\d+.\d+.\d+, Culture=\w+, PublicKeyToken=\w+", RegexOptions.Compiled);

		static ConcurrentDictionary<string, Type> MsgPackReadTypes { get; } = new ConcurrentDictionary<string, Type>();

		static ConcurrentDictionary<Type, string> MsgPackWriteTypes { get; } = new ConcurrentDictionary<Type, string>();

		static SerializationContext MsgPackSerializationContext { get; } = new SerializationContext(PackerCompatibilityOptions.ProhibitExtendedTypeObjects)
		{
			SerializationMethod = SerializationMethod.Map,
			DefaultDateTimeConversionMethod = DateTimeConversionMethod.Native
		};

		static JsonSerializer JSONSerializer { get; } = new JsonSerializer();

		/// <summary>
		/// Creates a recyclable memory stream
		/// </summary>
		/// <param name="buffer">The buffer to initialize data of the stream</param>
		/// <param name="index">The zero-based byte offset in buffer at which to begin copying bytes to the stream</param>
		/// <param name="count">The maximum number of bytes to write</param>
		/// <returns></returns>
		public static MemoryStream CreateMemoryStream(byte[] buffer = null, int index = 0, int count = 0)
		{
			var stream = Helper.RecyclableMemoryStreamManager.GetStream();
			if (buffer != null && buffer.Any())
			{
				index = index > -1 && index < buffer.Length ? index : 0;
				count = count > 0 && count < buffer.Length - index ? count : buffer.Length - index;
				stream.Write(buffer, index, count);
				stream.Seek(0, SeekOrigin.Begin);
			}
			return stream;
		}

		/// <summary>
		/// Creates a recyclable memory stream
		/// </summary>
		/// <param name="buffer">The buffer to initialize data of the stream</param>
		/// <returns></returns>
		public static MemoryStream CreateMemoryStream(ArraySegment<byte> buffer)
			=> Helper.CreateMemoryStream(buffer.Array, buffer.Offset, buffer.Count);

		/// <summary>
		/// Gets the array buffer of this stream with TryGetBuffer first, then ToArray if not success
		/// </summary>
		/// <param name="stream"></param>
		/// <returns></returns>
		public static byte[] ToBytes(this MemoryStream stream)
		{
			if (stream.TryGetBuffer(out var buffer))
			{
				var array = new byte[buffer.Count];
				Buffer.BlockCopy(buffer.Array, buffer.Offset, array, 0, buffer.Count);
				return array;
			}
			return stream.ToArray();
		}

		/// <summary>
		/// Gets the array segment of this stream with TryGetBuffer first, then ToArray if not success
		/// </summary>
		/// <param name="stream"></param>
		/// <returns></returns>
		public static ArraySegment<byte> ToArraySegment(this MemoryStream stream)
			=> stream.TryGetBuffer(out var buffer)
				? buffer
				: new ArraySegment<byte>(stream.ToArray());

		/// <summary>
		/// Serializes an object to array of bytes
		/// </summary>
		/// <param name="value"></param>
		/// <param name="serializationContext"></param>
		/// <returns></returns>
		public static Tuple<int, byte[]> Serialize(object value, SerializationContext serializationContext = null)
		{
			var data = Array.Empty<byte>();
			var typeCode = value == null ? TypeCode.DBNull : Type.GetTypeCode(value.GetType());
			var typeFlag = (int)typeCode | 0x0100;
			switch (typeCode)
			{
				case TypeCode.Empty:
				case TypeCode.DBNull:
					break;

				case TypeCode.Boolean:
					data = BitConverter.GetBytes((bool)value);
					break;

				case TypeCode.DateTime:
					data = BitConverter.GetBytes(((DateTime)value).ToBinary());
					break;

				case TypeCode.Char:
					data = BitConverter.GetBytes((char)value);
					break;

				case TypeCode.String:
					data = Encoding.UTF8.GetBytes((string)value);
					break;

				case TypeCode.Byte:
					data = BitConverter.GetBytes((byte)value);
					break;

				case TypeCode.SByte:
					data = BitConverter.GetBytes((sbyte)value);
					break;

				case TypeCode.Int16:
					data = BitConverter.GetBytes((short)value);
					break;

				case TypeCode.UInt16:
					data = BitConverter.GetBytes((ushort)value);
					break;

				case TypeCode.Int32:
					data = BitConverter.GetBytes((int)value);
					break;

				case TypeCode.UInt32:
					data = BitConverter.GetBytes((uint)value);
					break;

				case TypeCode.Int64:
					data = BitConverter.GetBytes((long)value);
					break;

				case TypeCode.UInt64:
					data = BitConverter.GetBytes((ulong)value);
					break;

				case TypeCode.Single:
					data = BitConverter.GetBytes((float)value);
					break;

				case TypeCode.Double:
					data = BitConverter.GetBytes((double)value);
					break;

				case TypeCode.Decimal:
					data = Helper.Concat(Decimal.GetBits((decimal)value).Select(@int => BitConverter.GetBytes(@int)));
					break;

				default:
					if (value is byte[])
					{
						typeFlag = Helper.FlagOfRawData;
						data = value as byte[];
					}
					else if (value is ArraySegment<byte> segment)
					{
						typeFlag = Helper.FlagOfRawData;
						data = new byte[segment.Count];
						Buffer.BlockCopy(segment.Array, segment.Offset, data, 0, segment.Count);
					}
					else
						data = Helper.SerializeByMsgPackCLI(value, serializationContext);
					break;
			}

			return new Tuple<int, byte[]>(typeFlag, data);
		}

		/// <summary>
		/// Serializes an object to array of bytes using MsgPack CLI Serializer
		/// </summary>
		/// <param name="value"></param>
		/// <param name="serializationContext"></param>
		/// <returns></returns>
		public static byte[] SerializeByMsgPackCLI(object value, SerializationContext serializationContext = null)
		{
			using (var stream = Helper.CreateMemoryStream())
			{
				using (var packer = Packer.Create(stream))
				{
					var type = value.GetType();
					packer.PackArrayHeader(2);
					packer.PackString(Helper.MsgPackWriteTypes.GetOrAdd(type, Helper.MsgPackTypeRegex.Replace(type.AssemblyQualifiedName, "")));
					MessagePackSerializer.Get(type, serializationContext ?? Helper.MsgPackSerializationContext).PackTo(packer, value);
					return stream.ToBytes();
				}
			}
		}

		/// <summary>
		/// Serializes an object to array of bytes using MsgPack CLI Serializer
		/// </summary>
		/// <param name="value"></param>
		/// <param name="serializationContext"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public static async Task<byte[]> SerializeByMsgPackCLIAsync(object value, SerializationContext serializationContext = null, CancellationToken cancellationToken = default)
		{
			using (var stream = Helper.CreateMemoryStream())
			{
				using (var packer = Packer.Create(stream))
				{
					var type = value.GetType();
					await packer.PackArrayHeaderAsync(2, cancellationToken).ConfigureAwait(false);
					await packer.PackStringAsync(Helper.MsgPackWriteTypes.GetOrAdd(type, Helper.MsgPackTypeRegex.Replace(type.AssemblyQualifiedName, "")), cancellationToken).ConfigureAwait(false);
					await MessagePackSerializer.Get(type, serializationContext ?? Helper.MsgPackSerializationContext).PackToAsync(packer, value, cancellationToken).ConfigureAwait(false);
					return stream.ToBytes();
				}
			}
		}

		/// <summary>
		/// Serializes an object to array of bytes using Json.NET BSON Serializer
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static byte[] SerializeByBson(object value)
		{
			using (var stream = Helper.CreateMemoryStream())
			{
				using (var writer = new BsonDataWriter(stream))
				{
					Helper.JSONSerializer.Serialize(writer, value);
					return stream.ToBytes();
				}
			}
		}

		/// <summary>
		/// Deserializes object from an array of bytes
		/// </summary>
		/// <param name="data"></param>
		/// <param name="typeFlag"></param>
		/// <param name="start"></param>
		/// <param name="count"></param>
		/// <param name="serializationContext"></param>
		/// <returns></returns>
		public static object Deserialize(byte[] data, int typeFlag, int start = -1, int count = -1, SerializationContext serializationContext = null)
		{
			if (data == null || data.Length < 1)
				return null;

			start = start > -1 ? start : 0;
			count = count > -1 ? count : data.Length;

			if (typeFlag.Equals(Helper.FlagOfRawData))
			{
				if (start > 0)
				{
					var temp = new byte[count];
					Buffer.BlockCopy(data, start, temp, 0, count);
					return temp;
				}
				return data;
			}

			var bytes = Array.Empty<byte>();
			var typeCode = (TypeCode)(typeFlag & 0xff);
			if (!typeCode.Equals(TypeCode.Empty) && !typeCode.Equals(TypeCode.DBNull))
			{
				bytes = new byte[count];
				Buffer.BlockCopy(data, start, bytes, 0, count);
			}

			switch (typeCode)
			{
				case TypeCode.Empty:
				case TypeCode.DBNull:
					return null;

				case TypeCode.Boolean:
					return BitConverter.ToBoolean(bytes, 0);

				case TypeCode.DateTime:
					return DateTime.FromBinary(BitConverter.ToInt64(bytes, 0));

				case TypeCode.Char:
					return BitConverter.ToChar(bytes, 0);

				case TypeCode.String:
					return Encoding.UTF8.GetString(bytes);

				case TypeCode.Byte:
					return bytes[0];

				case TypeCode.SByte:
					return (sbyte)bytes[0];

				case TypeCode.Int16:
					return BitConverter.ToInt16(bytes, 0);

				case TypeCode.UInt16:
					return BitConverter.ToUInt16(bytes, 0);

				case TypeCode.Int32:
					return BitConverter.ToInt32(bytes, 0);

				case TypeCode.UInt32:
					return BitConverter.ToUInt32(bytes, 0);

				case TypeCode.Int64:
					return BitConverter.ToInt64(bytes, 0);

				case TypeCode.UInt64:
					return BitConverter.ToUInt64(bytes, 0);

				case TypeCode.Single:
					return BitConverter.ToSingle(bytes, 0);

				case TypeCode.Double:
					return BitConverter.ToDouble(bytes, 0);

				case TypeCode.Decimal:
					var bits = new int[4];
					for (var index = 0; index < 16; index += 4)
						bits[index / 4] = BitConverter.ToInt32(bytes, index);
					return new Decimal(bits);

				default:
					return Helper.DeserializeByMsgPackCLI(bytes, serializationContext);
			}
		}

		/// <summary>
		/// Deserializes an object from an array of bytes
		/// </summary>
		/// <param name="data"></param>
		/// <param name="typeFlag"></param>
		/// <param name="start"></param>
		/// <param name="count"></param>
		/// <param name="serializationContext"></param>
		/// <returns></returns>
		public static T Deserialize<T>(byte[] data, int typeFlag, int start = -1, int count = -1, SerializationContext serializationContext = null)
		{
			var value = Helper.Deserialize(data, typeFlag, start, count, serializationContext);
			return value != null && value is T val ? val : default;
		}

		/// <summary>
		/// Deserializes an object from an array of bytes using MsgPack CLI Deserializer
		/// </summary>
		/// <param name="value"></param>
		/// <param name="serializationContext"></param>
		/// <returns></returns>
		public static object DeserializeByMsgPackCLI(byte[] value, SerializationContext serializationContext = null)
		{
			using (var stream = Helper.CreateMemoryStream(value))
			{
				using (var unpacker = Unpacker.Create(stream))
				{
					unpacker.Read();
					if (unpacker.IsArrayHeader)
					{
						unpacker.Read();
						var type = Helper.MsgPackReadTypes.GetOrAdd((string)unpacker.LastReadData, typeName => Type.GetType(typeName, true));
						unpacker.Read();
						return MessagePackSerializer.Get(type, serializationContext ?? Helper.MsgPackSerializationContext).UnpackFrom(unpacker);
					}
					else
						throw new InvalidDataException("Invalid headers");
				}
			}
		}

		/// <summary>
		/// Deserializes an object from an array of bytes using MsgPack CLI Deserializer
		/// </summary>
		/// <param name="value"></param>
		/// <param name="serializationContext"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public static async Task<object> DeserializeByMsgPackCLIAsync(byte[] value, SerializationContext serializationContext = null, CancellationToken cancellationToken = default)
		{
			using (var stream = Helper.CreateMemoryStream(value))
			{
				using (var unpacker = Unpacker.Create(stream))
				{
					await unpacker.ReadAsync(cancellationToken).ConfigureAwait(false);
					if (unpacker.IsArrayHeader)
					{
						await unpacker.ReadAsync(cancellationToken).ConfigureAwait(false);
						var type = Helper.MsgPackReadTypes.GetOrAdd((string)unpacker.LastReadData, typeName => Type.GetType(typeName, throwOnError: true));
						await unpacker.ReadAsync(cancellationToken).ConfigureAwait(false);
						return await MessagePackSerializer.Get(type, serializationContext ?? Helper.MsgPackSerializationContext).UnpackFromAsync(unpacker, cancellationToken).ConfigureAwait(false);
					}
					else
						throw new InvalidDataException("Invalid headers");
				}
			}
		}

		/// <summary>
		/// Deserializes an object from an array of bytes using Json.NET BSON Deserializer
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static object DeserializeByBson(byte[] value)
		{
			using (var stream = Helper.CreateMemoryStream(value))
			{
				using (var reader = new BsonDataReader(stream))
				{
					return Helper.JSONSerializer.Deserialize(reader);
				}
			}
		}

		/// <summary>
		/// Deserializes an object from an array of bytes using Json.NET BSON Deserializer
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="value"></param>
		/// <returns></returns>
		public static T DeserializeByBson<T>(byte[] value)
		{
			using (var stream = Helper.CreateMemoryStream(value))
			{
				using (var reader = new BsonDataReader(stream))
				{
					return Helper.JSONSerializer.Deserialize<T>(reader);
				}
			}
		}
		#endregion

		#region Support cancellation token
		/// <summary>
		/// Executes a task with cancellation token
		/// </summary>
		/// <param name="task"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public static async Task WithCancellationToken(this Task task, CancellationToken cancellationToken)
		{
			var tcs = new TaskCompletionSource<bool>();
			using (cancellationToken.Register(state => ((TaskCompletionSource<bool>)state).TrySetResult(true), tcs, false))
			{
				var result = await Task.WhenAny(task, tcs.Task).ConfigureAwait(false);
				if (result != task)
					throw new OperationCanceledException(cancellationToken);
			}
		}

		/// <summary>
		/// Executes a task with cancellation token
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="task"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public static async Task<T> WithCancellationToken<T>(this Task<T> task, CancellationToken cancellationToken)
		{
			var tcs = new TaskCompletionSource<bool>();
			using (cancellationToken.Register(state => ((TaskCompletionSource<bool>)state).TrySetResult(true), tcs, false))
			{
				var result = await Task.WhenAny(task, tcs.Task).ConfigureAwait(false);
				return result != task
					? throw new OperationCanceledException(cancellationToken)
					: task.Result;
			}
		}
		#endregion

	}
}

namespace Microsoft.Extensions.DependencyInjection
{
	public static class MemcachedServiceCollectionExtensions
	{
		/// <summary>
		/// Adds the <see cref="IMemcachedClient">Memcached</see> service into the collection of services for using with dependency injection
		/// </summary>
		/// <param name="services"></param>
		/// <param name="setupAction">The action to bind options of 'Memcached' section from appsettings.json file</param>
		/// <param name="addInstanceOfIDistributedCache">true to add the memcached service as an instance of IDistributedCache</param>
		/// <returns></returns>
		public static IServiceCollection AddMemcached(this IServiceCollection services, Action<MemcachedClientOptions> setupAction, bool addInstanceOfIDistributedCache = true)
		{
			if (setupAction == null)
				throw new ArgumentNullException(nameof(setupAction));

			services.AddOptions().Configure(setupAction);
			services.Add(ServiceDescriptor.Singleton<IMemcachedClientConfiguration, MemcachedClientConfiguration>());
			services.Add(ServiceDescriptor.Singleton<IMemcachedClient, MemcachedClient>(serviceProvider => MemcachedClient.GetInstance(serviceProvider)));
			if (addInstanceOfIDistributedCache)
				services.Add(ServiceDescriptor.Singleton<IDistributedCache, MemcachedClient>(serviceProvider => MemcachedClient.GetInstance(serviceProvider)));

			return services;
		}
	}
}

namespace Microsoft.AspNetCore.Builder
{
	public static class MemcachedApplicationBuilderExtensions
	{
		/// <summary>
		/// Calls to use the <see cref="IMemcachedClient">Memcached</see> service
		/// </summary>
		/// <param name="appBuilder"></param>
		/// <returns></returns>
		public static IApplicationBuilder UseMemcached(this IApplicationBuilder appBuilder)
		{
			try
			{
				appBuilder.ApplicationServices.GetService<ILogger<IMemcachedClient>>().LogInformation($"The service of Memcached client was{(appBuilder.ApplicationServices.GetService<IMemcachedClient>() != null ? " " : " not ")}registered with application service providers");
			}
			catch (Exception ex)
			{
				appBuilder.ApplicationServices.GetService<ILogger<IMemcachedClient>>().LogError(ex, $"Error occurred while collecting information of Memcached client => {ex.Message}");
			}
			return appBuilder;
		}
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
