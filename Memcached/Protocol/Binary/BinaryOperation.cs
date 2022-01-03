using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public abstract class BinaryOperation : Operation
	{
		protected abstract BinaryRequest Build();

		protected internal override IList<ArraySegment<byte>> GetBuffer()
            => this.Build().CreateBuffer();

        protected internal override Task<Results.IOperationResult> ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
            => throw new NotSupportedException();
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
