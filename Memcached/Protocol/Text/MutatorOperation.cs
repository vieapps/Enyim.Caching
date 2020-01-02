using System;
using System.Globalization;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Text
{
	public class MutatorOperation : SingleItemOperation, IMutatorOperation
	{
		readonly MutationMode mode;
		readonly ulong delta;
		ulong result;

		internal MutatorOperation(MutationMode mode, string key, ulong delta) : base(key)
		{
			this.delta = delta;
			this.mode = mode;
		}

		public ulong Result
		{
			get { return this.result; }
		}

		MutationMode IMutatorOperation.Mode => this.mode;

		ulong IMutatorOperation.Result => this.result;

		protected internal override IList<ArraySegment<byte>> GetBuffer()
		{
			var command = (this.mode == MutationMode.Increment ? "incr " : "decr ")
				+ this.Key
				+ " "
				+ this.delta.ToString(CultureInfo.InvariantCulture)
				+ TextSocketHelper.CommandTerminator;

			return TextSocketHelper.GetCommandBuffer(command);
		}

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			string response = TextSocketHelper.ReadResponse(socket);
			var result = new TextOperationResult();

			//maybe we should throw an exception when the item is not found?
			if (String.Compare(response, "NOT_FOUND", StringComparison.Ordinal) == 0)
				return result.Fail("Failed to read response.  Item not found");

			result.Success = UInt64.TryParse(response, NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite, CultureInfo.InvariantCulture, out this.result);
			return result;
		}

		protected internal override async Task<IOperationResult> ReadResponseAsync(PooledSocket socket, CancellationToken cancellationToken = default)
		{
			string response = await TextSocketHelper.ReadResponseAsync(socket, cancellationToken).ConfigureAwait(false);
			var result = new TextOperationResult();

			//maybe we should throw an exception when the item is not found?
			if (String.Compare(response, "NOT_FOUND", StringComparison.Ordinal) == 0)
				return result.Fail("Failed to read response.  Item not found");

			result.Success = UInt64.TryParse(response, NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite, CultureInfo.InvariantCulture, out this.result);
			return result;
		}

		protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
			=> throw new NotSupportedException();
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
