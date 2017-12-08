using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class MultiGetOperation : BinaryMultiItemOperation, IMultiGetOperation
	{
		ILogger _logger;

		Dictionary<string, CacheItem> _result;
		Dictionary<int, string> _idToKey;
		int _noopId;

		PooledSocket _socket;
		BinaryResponse _response;
		bool? _loopState;
		Action<bool> _next;

		public MultiGetOperation(IList<string> keys) : base(keys)
		{
			this._logger = Logger.CreateLogger<MultiGetOperation>();
		}

		protected override BinaryRequest Build(string key)
		{
			return new BinaryRequest(OpCode.GetQ)
			{
				Key = key
			};
		}

		protected internal override IList<ArraySegment<byte>> GetBuffer()
		{
			var keys = this.Keys;
			if (keys == null || keys.Count == 0)
			{
				if (this._logger.IsEnabled(LogLevel.Warning))
					this._logger.LogWarning("Multi-Get: Empty multi-get (no key)");

				return new ArraySegment<byte>[0];
			}

			// map the command's correlationId to the item key,
			// so we can use GetQ (which only returns the item data)
			this._idToKey = new Dictionary<int, string>();

			// get ops have 2 segments, header + key
			var buffer = new List<ArraySegment<byte>>(keys.Count * 2);
			foreach (var key in keys)
			{
				var request = this.Build(key);
				request.CreateBuffer(buffer);

				// we use this to map the responses to the keys
				this._idToKey[request.CorrelationId] = key;
			}

			// uncork the server
			var noop = new BinaryRequest(OpCode.NoOp);
			this._noopId = noop.CorrelationId;
			noop.CreateBuffer(buffer);

			if (this._logger.IsEnabled(LogLevel.Debug))
				this._logger.LogInformation($"Multi-Get: Building {keys.Count} keys - Correlation ID: {noop.CorrelationId}");

			return buffer;
		}

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			this._result = new Dictionary<string, CacheItem>();
			this.Cas = new Dictionary<string, ulong>();
			var result = new TextOperationResult();

			var response = new BinaryResponse();
			while (response.Read(socket))
			{
				this.StatusCode = response.StatusCode;

				// found the noop, quit
				if (response.CorrelationId == this._noopId)
					return result.Pass();

				// find the key to the response
				if (!this._idToKey.TryGetValue(response.CorrelationId, out string key))
				{
					// we're not supposed to get here
					this._logger.LogWarning($"Multi-Get: Found response with correlation ID ({response.CorrelationId}), but no key is matching it");
					continue;
				}

				// deserialize the response
				var flags = BinaryConverter.DecodeInt32(response.Extra, 0);
				this._result[key] = new CacheItem((ushort)flags, response.Data);
				this.Cas[key] = response.CAS;

				if (this._logger.IsEnabled(LogLevel.Debug))
					this._logger.LogDebug($"Multi-Get: Reading data of '{key}' (ReadResponse) - CAS: {response.CAS} - Flags: {flags}");
			}

			// finished reading but we did not find the NOOP
			return result.Fail($"Found response with correlation ID {response.CorrelationId}, but no key is matching it");
		}

		protected internal override async Task<IOperationResult> ReadResponseAsync(PooledSocket socket)
		{
			this._result = new Dictionary<string, CacheItem>();
			this.Cas = new Dictionary<string, ulong>();
			var result = new TextOperationResult();

			var response = new BinaryResponse();
			while (await response.ReadAsync(socket).ConfigureAwait(false))
			{
				this.StatusCode = response.StatusCode;

				// found the noop, quit
				if (response.CorrelationId == this._noopId)
					return result.Pass();

				// find the key to the response
				if (!this._idToKey.TryGetValue(response.CorrelationId, out string key))
				{
					// we're not supposed to get here
					this._logger.LogWarning($"Multi-Get: Found response with correlation ID ({response.CorrelationId}), but no key is matching it");
					continue;
				}

				// deserialize the response
				var flags = BinaryConverter.DecodeInt32(response.Extra, 0);
				this._result[key] = new CacheItem((ushort)flags, response.Data);
				this.Cas[key] = response.CAS;

				if (this._logger.IsEnabled(LogLevel.Debug))
					this._logger.LogDebug($"Multi-Get: Reading data of '{key}' (ReadResponseAsync) - CAS: {response.CAS} - Flags: {flags}");
			}

			// finished reading but we did not find the NOOP
			return result.Fail($"Found response with correlation ID {response.CorrelationId}, but no key is matching it");
		}

		protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
		{
			this.Cas = new Dictionary<string, ulong>();
			this._result = new Dictionary<string, CacheItem>();

			this._socket = socket;
			this._response = new BinaryResponse();
			this._loopState = null;
			this._next = next;

			return this.DoReadAsync();
		}

		bool DoReadAsync()
		{
			var reader = this._response;

			while (this._loopState == null)
			{
				var readSuccess = reader.ReadAsync(this._socket, this.EndReadAsync, out bool ioPending);
				this.StatusCode = reader.StatusCode;

				if (ioPending)
					return readSuccess;

				if (!readSuccess)
					this._loopState = false;
				else if (reader.CorrelationId == this._noopId)
					this._loopState = true;
				else
					this.StoreResult(reader);
			}

			this._next((bool)this._loopState);

			return true;
		}

		void EndReadAsync(bool readSuccess)
		{
			if (!readSuccess)
				this._loopState = false;
			else if (this._response.CorrelationId == this._noopId)
				this._loopState = true;
			else
				this.StoreResult(this._response);

			this.DoReadAsync();
		}

		void StoreResult(BinaryResponse reader)
		{
			// find the key to the response
			if (!this._idToKey.TryGetValue(reader.CorrelationId, out string key))
				this._logger.LogWarning($"Multi-Get: Found response with correlation ID ({reader.CorrelationId}), but no key is matching it."); // we're not supposed to get here

			else
			{
				// deserialize the response
				var flags = (ushort)BinaryConverter.DecodeInt32(reader.Extra, 0);
				this._result[key] = new CacheItem(flags, reader.Data);
				this.Cas[key] = reader.CAS;

				if (this._logger.IsEnabled(LogLevel.Debug))
					this._logger.LogDebug($"Multi-Get: Reading data of '{key}' (ReadResponseAsync+StoreResult) - CAS: {reader.CAS} - Flags: {flags}");
			}
		}

		public Dictionary<string, CacheItem> Result
		{
			get { return this._result; }
		}

		Dictionary<string, CacheItem> IMultiGetOperation.Result
		{
			get { return this._result; }
		}
	}
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
