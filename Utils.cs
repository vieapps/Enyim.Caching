#region Related components
using System;
using System.IO;
using System.Text;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Serialization.Formatters.Binary;

using Enyim.Caching;
using Enyim.Caching.Configuration;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
#endregion

namespace CacheUtils
{
	public static class Helper
	{

		#region Work with caching data
		/// <summary>
		/// Gets the Unix epoch
		/// </summary>
		public static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		/// <summary>
		/// Converts this date-time to time-span with Unix epoch
		/// </summary>
		/// <param name="datetime"></param>
		/// <returns></returns>
		public static TimeSpan ToUnixTimeSpan(this DateTime datetime)
		{
			return datetime < Helper.UnixEpoch
				? throw new ArgumentOutOfRangeException(nameof(datetime), $"{nameof(datetime)} must be >= 1970/1/1")
				: datetime.ToUniversalTime() - Helper.UnixEpoch;
		}

		/// <summary>
		/// Converts this date-time to time-span
		/// </summary>
		/// <param name="datetime"></param>
		/// <param name="useUTC"></param>
		/// <returns></returns>
		public static TimeSpan ToTimeSpan(this DateTime datetime, bool useUTC = false)
		{
			return datetime == DateTime.MaxValue
				? TimeSpan.Zero
				: datetime < DateTime.Now
					? TimeSpan.FromMilliseconds(1)
					: useUTC
						? datetime.ToUniversalTime() - DateTime.Now.ToUniversalTime()
						: datetime.ToLocalTime() - DateTime.Now;
		}

		/// <summary>
		/// Gets the expiration of the date-time value
		/// </summary>
		/// <param name="expiresAt"></param>
		/// <returns></returns>
		public static uint GetExpiration(this DateTime expiresAt)
		{
			if (expiresAt < Helper.UnixEpoch)
				throw new ArgumentOutOfRangeException(nameof(expiresAt), $"{nameof(expiresAt)} must be >= 1970/1/1");

			return expiresAt == DateTime.MaxValue
				? 0
				: (uint)expiresAt.ToUnixTimeSpan().TotalSeconds;
		}

		/// <summary>
		/// Gets the expiration of the time-span value
		/// </summary>
		/// <param name="validFor"></param>
		/// <returns></returns>
		public static uint GetExpiration(this TimeSpan validFor)
		{
			return validFor == TimeSpan.Zero || validFor == TimeSpan.MaxValue
				? 0
				: DateTime.Now.Add(validFor).GetExpiration();
		}

		/// <summary>
		/// Gets the expiration of the distributed cache entry options
		/// </summary>
		/// <param name="options"></param>
		/// <returns></returns>
		public static object GetExpiration(this DistributedCacheEntryOptions options)
		{
			return options.SlidingExpiration != null && options.AbsoluteExpiration != null
				? throw new ArgumentException("You cannot specify both sliding expiration and absolute expiration")
				: options.AbsoluteExpiration != null
					? options.AbsoluteExpiration.Value.ToUnixTimeSeconds()
					: options.AbsoluteExpirationRelativeToNow != null
						? (DateTimeOffset.UtcNow + options.AbsoluteExpirationRelativeToNow.Value).ToUnixTimeSeconds()
						: options.SlidingExpiration == null || options.SlidingExpiration.Value == TimeSpan.Zero || options.SlidingExpiration.Value == TimeSpan.MaxValue
							? (object)TimeSpan.Zero
							: (object)DateTime.Now.Add(options.SlidingExpiration.Value).ToTimeSpan();
		}

		/// <summary>
		/// Gets the key for storing related information of an IDistributedCache item
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		public static string GetIDistributedCacheExpirationKey(this string key)
		{
			return $"i-distributed-cache#{key}";
		}

		/// <summary>
		/// Combines arrays of bytes
		/// </summary>
		/// <param name="arrays"></param>
		/// <returns></returns>
		public static byte[] Combine(params byte[][] arrays)
		{
			var combined = new byte[arrays.Sum(a => a.Length)];
			var offset = 0;
			foreach (var array in arrays)
			{
				Buffer.BlockCopy(array, 0, combined, offset, array.Length);
				offset += array.Length;
			}
			return combined;
		}

		/// <summary>
		/// Gets the flag of raw data
		/// </summary>
		public const int FlagOfRawData = 0xfa52;
		#endregion

		#region Serialize & Deserialize
		/// <summary>
		/// Serialize an object to array of bytes
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static Tuple<int, byte[]> Serialize(object value)
		{
			var data = new byte[0];
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
					Decimal.GetBits((decimal)value).ToList().ForEach(i => data = Helper.Combine(data, BitConverter.GetBytes(i)));
					break;

				default:
					if (value is byte[] || value is ArraySegment<byte>)
					{
						typeFlag = Helper.FlagOfRawData;
						if (value is byte[])
							data = value as byte[];
						else
						{
							data = new byte[((ArraySegment<byte>)value).Count];
							Buffer.BlockCopy(((ArraySegment<byte>)value).Array, ((ArraySegment<byte>)value).Offset, data, 0, ((ArraySegment<byte>)value).Count);
						}
					}
					else
					{
						if (value.GetType().IsSerializable)
							using (var stream = new MemoryStream())
							{
								new BinaryFormatter().Serialize(stream, value);
								data = stream.GetBuffer();
							}
						else
							throw new ArgumentException($"The type '{value.GetType()}' of '{nameof(value)}' must have Serializable attribute or implemented the ISerializable interface");
					}
					break;
			}

			return new Tuple<int, byte[]>(typeFlag, data);
		}

		/// <summary>
		/// Deserializes object from an array of bytes
		/// </summary>
		/// <param name="data"></param>
		/// <param name="typeFlag"></param>
		/// <param name="start"></param>
		/// <param name="count"></param>
		/// <returns></returns>
		public static object Deserialize(byte[] data, int typeFlag, int start = -1, int count = -1)
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
				else
					return data;
			}

			var bytes = new byte[0];
			var typeCode = (TypeCode)(typeFlag & 0xff);
			if (!typeCode.Equals(TypeCode.Empty) && !typeCode.Equals(TypeCode.DBNull) && !typeCode.Equals(TypeCode.Object))
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
					return Encoding.UTF8.GetString(bytes, 0, bytes.Length);

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
					using (var stream = new MemoryStream(data, start, count))
					{
						return new BinaryFormatter().Deserialize(stream);
					}
			}
		}
		#endregion

		#region Support cancellation token
		internal static async Task WithCancellationToken(this Task task, CancellationToken cancellationToken)
		{
			var tcs = new TaskCompletionSource<bool>();
			using (cancellationToken.Register(state => ((TaskCompletionSource<bool>)state).TrySetResult(true), tcs, false))
			{
				if (task != await Task.WhenAny(task, tcs.Task))
					throw new OperationCanceledException(cancellationToken);
			}
			await task;
		}

		internal static async Task<T> WithCancellationToken<T>(this Task<T> task, CancellationToken cancellationToken)
		{
			var tcs = new TaskCompletionSource<bool>();
			using (cancellationToken.Register(state => ((TaskCompletionSource<bool>)state).TrySetResult(true), tcs, false))
			{
				if (task != await Task.WhenAny(task, tcs.Task))
					throw new OperationCanceledException(cancellationToken);
			}
			return await task;
		}
		#endregion

	}
}

namespace Microsoft.Extensions.DependencyInjection
{

	#region Extensions for working with .NET Core dependency injection
	public static partial class ServiceCollectionExtensions
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

			services.AddOptions();
			services.Configure(setupAction);
			services.Add(ServiceDescriptor.Singleton<IMemcachedClientConfiguration, MemcachedClientConfiguration>());
			services.Add(ServiceDescriptor.Singleton<IMemcachedClient, MemcachedClient>(s => MemcachedClient.GetInstance(s)));
			if (addInstanceOfIDistributedCache)
				services.Add(ServiceDescriptor.Singleton<IDistributedCache, MemcachedClient>(s => MemcachedClient.GetInstance(s)));

			return services;
		}
	}
	#endregion

}

namespace Microsoft.AspNetCore.Builder
{

	#region Extensions for working with ASP.NET Core
	public static partial class ApplicationBuilderExtensions
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
				appBuilder.ApplicationServices.GetService<ILogger<IMemcachedClient>>().LogInformation($"Memcached service is {(appBuilder.ApplicationServices.GetService<IMemcachedClient>() != null ? "" : "not-")}started");
			}
			catch (Exception ex)
			{
				appBuilder.ApplicationServices.GetService<ILogger<IMemcachedClient>>().LogError(ex, "Memcached service is failed to start");
			}
			return appBuilder;
		}
	}
	#endregion

}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2018 VIEApps.net
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
