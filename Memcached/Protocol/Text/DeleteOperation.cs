using System;
using System.Threading;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Text
{
	public class DeleteOperation : SingleItemOperation, IDeleteOperation
	{
		internal DeleteOperation(string key) : base(key) { }

		protected internal override System.Collections.Generic.IList<ArraySegment<byte>> GetBuffer()
			=> TextSocketHelper.GetCommandBuffer("delete " + this.Key + TextSocketHelper.CommandTerminator);

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
			=> new TextOperationResult
			{
				Success = String.Compare(TextSocketHelper.ReadResponse(socket), "DELETED", StringComparison.Ordinal) == 0
			};

		protected internal override async Task<IOperationResult> ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default)
			=> new TextOperationResult
			{
				Success = String.Compare(await TextSocketHelper.ReadResponseAsync(socket, cancellationToken).ConfigureAwait(false), "DELETED", StringComparison.Ordinal) == 0
			};

		protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
			=> throw new NotSupportedException();
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2022 VIEApps.net
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
