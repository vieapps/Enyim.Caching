using System;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class BinaryResponse
	{
		ILogger _logger;
		PooledSocket _socket;
		int _dataLength, _extraLength;
		bool _shouldCallNext;
		Action<bool> _nextAction;

		const byte MAGIC_VALUE = 0x81;
		const int HeaderLength = 24;

		const int HEADER_OPCODE = 1;
		const int HEADER_KEY = 2; // 2-3
		const int HEADER_EXTRA = 4;
		const int HEADER_DATATYPE = 5;
		const int HEADER_STATUS = 6; // 6-7
		const int HEADER_BODY = 8; // 8-11
		const int HEADER_OPAQUE = 12; // 12-15
		const int HEADER_CAS = 16; // 16-23

		public byte Opcode;
		public int KeyLength;
		public byte DataType;
		public int StatusCode;

		public int CorrelationID;
		public ulong CAS;

		public ArraySegment<byte> Extra;
		public ArraySegment<byte> Data;

		public BinaryResponse()
		{
			this._logger = Logger.CreateLogger<BinaryResponse>();
			this.StatusCode = -1;
		}

		/// <summary>
		/// Reads the response from the socket
		/// </summary>
		/// <param name="socket"></param>
		/// <returns></returns>
		public unsafe bool Read(PooledSocket socket)
		{
			this.StatusCode = -1;

			if (!socket.IsAlive)
				return false;

			try
			{
				var header = new byte[HeaderLength];
				socket.Receive(header, 0, header.Length);

				this.DeserializeHeader(header, out int dataLength, out int extraLength);

				if (dataLength > 0)
				{
					var data = new byte[dataLength];
					socket.Receive(data, 0, dataLength);

					this.Extra = new ArraySegment<byte>(data, 0, extraLength);
					this.Data = new ArraySegment<byte>(data, extraLength, data.Length - extraLength);
				}

				return this.StatusCode == 0;
			}
			catch (Exception ex)
			{
				this._logger.LogError(ex, "Read: Error occurred while reading socket");
				throw ex;
			}
		}

		/// <summary>
		/// Reads the response from the socket
		/// </summary>
		/// <param name="socket"></param>
		/// <returns></returns>
		public async Task<bool> ReadAsync(PooledSocket socket)
		{
			this.StatusCode = -1;

			if (!socket.IsAlive)
				return false;

			try
			{
				var header = new byte[HeaderLength];
				await socket.ReceiveAsync(header, 0, header.Length).ConfigureAwait(false);

				this.DeserializeHeader(header, out int dataLength, out int extraLength);

				if (dataLength > 0)
				{
					var data = new byte[dataLength];
					await socket.ReceiveAsync(data, 0, dataLength).ConfigureAwait(false);

					this.Extra = new ArraySegment<byte>(data, 0, extraLength);
					this.Data = new ArraySegment<byte>(data, extraLength, data.Length - extraLength);
				}

				return this.StatusCode == 0;
			}
			catch (Exception ex)
			{
				this._logger.LogError(ex, "ReadAsync: Error occurred while reading socket");
				throw ex;
			}
		}

		/// <summary>
		/// Reads the response from the socket asynchronously.
		/// </summary>
		/// <param name="socket">The socket to read from.</param>
		/// <param name="next">The delegate which will continue processing the response. This is only called if the read completes asynchronoulsy.</param>
		/// <param name="ioPending">Set to true if the read is still pending when ReadASync returns. In this case 'next' will be called when the read is finished.</param>
		/// <returns>
		/// If the socket is already dead, ReadAsync returns false, next is not called, ioPending = false
		/// If the read completes synchronously (e.g. data is received from the buffer), it returns true/false depending on the StatusCode, and ioPending is set to true, 'next' will not be called.
		/// It returns true if it has to read from the socket, so the operation will complate asynchronously at a later time. ioPending will be true, and 'next' will be called to handle the data
		/// </returns>
		public bool ReadAsync(PooledSocket socket, Action<bool> next, out bool ioPending)
		{
			this.StatusCode = -1;
			this._socket = socket;
			this._nextAction = next;

			var asyncEvent = new AsyncIOArgs
			{
				Count = HeaderLength,
				Next = this.DoDecodeHeaderAsync
			};

			this._shouldCallNext = true;
			if (socket.ReceiveAsync(asyncEvent))
			{
				ioPending = true;
				return true;
			}

			ioPending = false;
			this._shouldCallNext = false;

			return asyncEvent.Fail
				? false
				: this.DoDecodeHeader(asyncEvent, out ioPending);
		}

		void DoDecodeHeaderAsync(AsyncIOArgs asyncEvent)
		{
			this._shouldCallNext = true;
			this.DoDecodeHeader(asyncEvent, out bool tmp);
		}

		bool DoDecodeHeader(AsyncIOArgs asyncEvent, out bool pendingIO)
		{
			pendingIO = false;
			if (asyncEvent.Fail)
			{
				if (this._shouldCallNext)
					this._nextAction(false);
				return false;
			}

			this.DeserializeHeader(asyncEvent.Result, out this._dataLength, out this._extraLength);
			var result = this.StatusCode == 0;

			if (this._dataLength == 0)
			{
				if (this._shouldCallNext)
					this._nextAction(result);
			}
			else
			{
				asyncEvent.Count = this._dataLength;
				asyncEvent.Next = this.DoDecodeBodyAsync;

				if (this._socket.ReceiveAsync(asyncEvent))
				{
					pendingIO = true;
				}
				else
				{
					if (asyncEvent.Fail)
						return false;
					this.DoDecodeBody(asyncEvent);
				}
			}

			return result;
		}

		void DoDecodeBodyAsync(AsyncIOArgs asyncEvent)
		{
			this._shouldCallNext = true;
			this.DoDecodeBody(asyncEvent);
		}

		void DoDecodeBody(AsyncIOArgs asyncEvent)
		{
			if (asyncEvent.Fail)
			{
				if (this._shouldCallNext)
					this._nextAction(false);
				return;
			}

			this.Extra = new ArraySegment<byte>(asyncEvent.Result, 0, this._extraLength);
			this.Data = new ArraySegment<byte>(asyncEvent.Result, this._extraLength, this._dataLength - this._extraLength);

			if (this._shouldCallNext)
				this._nextAction(true);
		}

		unsafe void DeserializeHeader(byte[] header, out int dataLength, out int extraLength)
		{
			fixed (byte* buffer = header)
			{
				if (buffer[0] != MAGIC_VALUE)
					throw new InvalidOperationException($"DeserializeHeader: Expected magic value '{MAGIC_VALUE}' but received '{buffer[0]}'");

				this.DataType = buffer[HEADER_DATATYPE];
				this.Opcode = buffer[HEADER_OPCODE];
				this.StatusCode = BinaryConverter.DecodeUInt16(buffer, HEADER_STATUS);

				this.KeyLength = BinaryConverter.DecodeUInt16(buffer, HEADER_KEY);
				this.CorrelationID = BinaryConverter.DecodeInt32(buffer, HEADER_OPAQUE);
				this.CAS = BinaryConverter.DecodeUInt64(buffer, HEADER_CAS);

				dataLength = BinaryConverter.DecodeInt32(buffer, HEADER_BODY);
				extraLength = buffer[HEADER_EXTRA];
			}
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
