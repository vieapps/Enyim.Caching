using System;
using System.Net;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Protocol;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Provides custom server pool implementations
	/// </summary>
	public interface IServerPool : IDisposable
	{
		IMemcachedNode Locate(string key);

		IOperationFactory OperationFactory { get; }

		IEnumerable<IMemcachedNode> GetWorkingNodes();

		void Start();

		event Action<IMemcachedNode> NodeFailed;
	}

	public interface IMemcachedNode : IDisposable
	{
		EndPoint EndPoint { get; }

		bool IsAlive { get; }

		bool Ping();

		IOperationResult Execute(IOperation op);

		Task<IOperationResult> ExecuteAsync(IOperation op, CancellationToken cancellationToken = default);

		bool ExecuteAsync(IOperation op, Action<bool> next);

		event Action<IMemcachedNode> Failed;
	}

	public interface IOperationFactory
	{
		IGetOperation Get(string key);

		IMultiGetOperation MultiGet(IList<string> keys);

		IStoreOperation Store(StoreMode mode, string key, CacheItem value, uint expires, ulong cas);

		IDeleteOperation Delete(string key, ulong cas);

		IMutatorOperation Mutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas);

		IConcatOperation Concat(ConcatenationMode mode, string key, ulong cas, ArraySegment<byte> data);

		IStatsOperation Stats(string type);

		IFlushOperation Flush();
	}

	public interface IOperation
	{
		int StatusCode { get; }

		IList<ArraySegment<byte>> GetBuffer();

		IOperationResult ReadResponse(PooledSocket socket);

		Task<IOperationResult> ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default);

		bool ReadResponseAsync(PooledSocket socket, Action<bool> next);
	}

	public interface ISingleItemOperation : IOperation
	{
		string Key { get; }

		/// <summary>
		/// The CAS value returned by the server after executing the command.
		/// </summary>
		ulong CasValue { get; }
	}

	public interface IMultiItemOperation : IOperation
	{
		IList<string> Keys { get; }

		Dictionary<string, ulong> Cas { get; }
	}

	public interface IGetOperation : ISingleItemOperation
	{
		CacheItem Result { get; }
	}

	public interface IMultiGetOperation : IMultiItemOperation
	{
		Dictionary<string, CacheItem> Result { get; }
	}

	public interface IStoreOperation : ISingleItemOperation
	{
		StoreMode Mode { get; }
	}

	public interface IDeleteOperation : ISingleItemOperation
	{
	}

	public interface IConcatOperation : ISingleItemOperation
	{
		ConcatenationMode Mode { get; }
	}

	public interface IMutatorOperation : ISingleItemOperation
	{
		MutationMode Mode { get; }

		ulong Result { get; }
	}

	public interface IStatsOperation : IOperation
	{
		Dictionary<string, string> Result { get; }
	}

	public interface IFlushOperation : IOperation
	{
	}

	/// <summary>
	/// Provides a way for custom initalization of the providers (locators, transcoders, key transformers)
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public interface IProviderFactory<T> : IProvider
	{
		T Create();
	}

	public interface IProvider
	{
		void Initialize(Dictionary<string, string> parameters);
	}

	/// <summary>
	/// Provides the base interface for Memcached SASL authentication.
	/// </summary>
	public interface ISaslAuthenticationProvider
	{
		string Type { get; }

		void Initialize(Dictionary<string, object> parameters);

		byte[] Authenticate();

		byte[] Continue(byte[] data);
	}

	public struct CasResult<T>
	{
		public T Result { get; set; }

		public ulong Cas { get; set; }

		public int StatusCode { get; set; }
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
