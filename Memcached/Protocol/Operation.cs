using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol
{
	/// <summary>
	/// Base class for implementing operations.
	/// </summary>
	public abstract class Operation : IOperation
	{
		protected Operation() { }

		internal protected abstract IList<ArraySegment<byte>> GetBuffer();

		internal protected abstract IOperationResult ReadResponse(PooledSocket socket);

		internal protected abstract Task<IOperationResult> ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default);

		internal protected abstract bool ReadResponseAsync(PooledSocket socket, Action<bool> next);

		IList<ArraySegment<byte>> IOperation.GetBuffer()
			=> this.GetBuffer();

		IOperationResult IOperation.ReadResponse(PooledSocket socket)
			=> this.ReadResponse(socket);

		Task<IOperationResult> IOperation.ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default)
			=> this.ReadResponseAsync(socket, cancellationToken);

		bool IOperation.ReadResponseAsync(PooledSocket socket, Action<bool> next)
			=> this.ReadResponseAsync(socket, next);

		int IOperation.StatusCode => this.StatusCode;

		public int StatusCode { get; protected set; }
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    � 2010 Attila Kisk� (aka Enyim), � 2016 CNBlogs, � 2022 VIEApps.net
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *		http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
