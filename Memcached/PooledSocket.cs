using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Diagnostics;

using Dawn.Net.Sockets;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached
{
	[DebuggerDisplay("Endpoint: {endpoint}, IsAlive = {IsAlive}")]
	public partial class PooledSocket : IDisposable
	{
		ILogger _logger;

		bool _isAlive;
		Socket _socket;
		EndPoint _endpoint;

		Stream _input;
		AsyncSocketHelper _helper;

		public PooledSocket(EndPoint endpoint, TimeSpan connectionTimeout, TimeSpan receiveTimeout)
		{
			this._logger = LogManager.CreateLogger<PooledSocket>();

			var timeout = receiveTimeout == TimeSpan.MaxValue
			    ? Timeout.Infinite
			    : (int)receiveTimeout.TotalMilliseconds;

			var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
			{
				ReceiveTimeout = timeout,
				SendTimeout = timeout
			};
			socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

			this.TryConnect(socket, endpoint, connectionTimeout == TimeSpan.MaxValue ? Timeout.Infinite : (int)connectionTimeout.TotalMilliseconds);

			this._socket = socket;
			this._endpoint = endpoint;
			this._input = new BasicNetworkStream(socket);
			this._isAlive = true;
		}

		void TryConnect(Socket socket, EndPoint endpoint, int timeout)
		{
			if (endpoint is DnsEndPoint && !RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
			{
				var dnsEndPoint = endpoint as DnsEndPoint;
				var host = dnsEndPoint.Host;
				var addresses = Dns.GetHostAddresses(dnsEndPoint.Host);
				var address = addresses.FirstOrDefault(ip => ip.AddressFamily == AddressFamily.InterNetwork);
				if (address == null)
					throw new ArgumentException($"Could not resolve host '{host}'.");

				this._logger.LogDebug($"Resolved '{host}' to '{address}'");
				endpoint = new IPEndPoint(address, dnsEndPoint.Port);
			}

			var completed = new AutoResetEvent(false);
			var args = new SocketAsyncEventArgs()
			{
				RemoteEndPoint = endpoint,
				UserToken = completed
			};
			args.Completed += (sender, socketArgs) =>
			{
				(socketArgs.UserToken as EventWaitHandle)?.Set();
			};

			socket.ConnectAsync(args);
			if (!completed.WaitOne(timeout) || !socket.Connected)
				using (socket)
				{
					throw new TimeoutException($"Could not connect to {endpoint}");
				}
		}

		public Action<PooledSocket> CleanupCallback { get; set; }

		public int Available
		{
			get { return this._socket.Available; }
		}

		public void Reset()
		{
			// discard any buffered data
			this._input.Flush();
			this._helper?.DiscardBuffer();

			var available = this._socket.Available;
			if (available > 0)
			{
				if (this._logger.IsEnabled(LogLevel.Warning))
					this._logger.LogWarning($"Socket bound to {this._socket.RemoteEndPoint} has {available} unread data! This is probably a bug in the code. InstanceID was {this.InstanceId}.");

				var data = new byte[available];
				this.Read(data, 0, available);

				if (this._logger.IsEnabled(LogLevel.Warning))
					this._logger.LogWarning(Encoding.ASCII.GetString(data));
			}

			if (this._logger.IsEnabled(LogLevel.Debug))
				this._logger.LogDebug($"Socket was reset ({this.InstanceId})");
		}

		/// <summary>
		/// The ID of this instance. Used by the <see cref="IServerPool"/> to identify the instance in its inner lists.
		/// </summary>
		public readonly Guid InstanceId = Guid.NewGuid();

		public bool IsAlive
		{
			get { return this._isAlive; }
		}

		/// <summary>
		/// Releases all resources used by this instance and shuts down the inner <see cref="Socket"/>. This instance will not be usable anymore.
		/// </summary>
		/// <remarks>Use the IDisposable.Dispose method if you want to release this instance back into the pool.</remarks>
		public void Destroy()
		{
			this.Dispose(true);
		}

		~PooledSocket()
		{
			try
			{
				this.Dispose(true);
			}
			catch { }
		}

		protected void Dispose(bool disposing)
		{
			if (disposing)
			{
				GC.SuppressFinalize(this);
				try
				{
					try
					{
						this._socket?.Dispose();
					}
					catch { }

					this._input?.Dispose();

					this._input = null;
					this._socket = null;
					this.CleanupCallback = null;
				}
				catch (Exception e)
				{
					this._logger.LogError(nameof(PooledSocket), e);
				}
			}
			else
				this.CleanupCallback?.Invoke(this);
		}

		void IDisposable.Dispose()
		{
			this.Dispose(false);
		}

		void CheckDisposed()
		{
			if (this._socket == null)
				throw new ObjectDisposedException("PooledSocket");
		}

		/// <summary>
		/// Reads the next byte from the server's response.
		/// </summary>
		/// <remarks>This method blocks and will not return until the value is read.</remarks>
		public int ReadByte()
		{
			this.CheckDisposed();
			try
			{
				return this._input.ReadByte();
			}
			catch (IOException)
			{
				this._isAlive = false;
				throw;
			}
		}

		public async Task<byte[]> ReadBytesAsync(int count)
		{
			using (var awaitable = new SocketAwaitable())
			{
				awaitable.Buffer = new ArraySegment<byte>(new byte[count], 0, count);
				await this._socket.ReceiveAsync(awaitable);
				return awaitable.Transferred.Array;
			}
		}

		/// <summary>
		/// Reads data from the server into the specified buffer.
		/// </summary>
		/// <param name="buffer">An array of <see cref="System.Byte"/> that is the storage location for the received data.</param>
		/// <param name="offset">The location in buffer to store the received data.</param>
		/// <param name="count">The number of bytes to read.</param>
		/// <remarks>This method blocks and will not return until the specified amount of bytes are read.</remarks>
		public void Read(byte[] buffer, int offset, int count)
		{
			this.CheckDisposed();

			var read = 0;
			var shouldRead = count;
			while (read < count)
			{
				try
				{
					var currentRead = this._input.Read(buffer, offset, shouldRead);
					if (currentRead < 1)
						continue;

					read += currentRead;
					offset += currentRead;
					shouldRead -= currentRead;
				}
				catch (IOException)
				{
					this._isAlive = false;
					throw;
				}
			}
		}

		public void Write(byte[] data, int offset, int length)
		{
			this.CheckDisposed();

			this._socket.Send(data, offset, length, SocketFlags.None, out SocketError status);
			if (status != SocketError.Success)
			{
				this._isAlive = false;
				throw new Exception($"Failed to write to the socket '{this._endpoint}'. Error: {status}");
			}
		}

		public void Write(IList<ArraySegment<byte>> buffers)
		{
			this.CheckDisposed();

			this._socket.Send(buffers, SocketFlags.None, out SocketError status);
			if (status != SocketError.Success)
			{
				this._isAlive = false;
				throw new Exception($"Failed to write to the socket '{this._endpoint}'. Error: {status}");
			}
		}

		public async Task WriteSync(IList<ArraySegment<byte>> buffers)
		{
			using (var awaitable = new SocketAwaitable())
			{
				awaitable.Arguments.BufferList = buffers;
				try
				{
					await this._socket.SendAsync(awaitable);
				}
				catch
				{
					this._isAlive = false;
					throw new Exception($"Failed to write to the socket '{this._endpoint}'. Error: {awaitable.Arguments.SocketError}");
				}

				if (awaitable.Arguments.SocketError != SocketError.Success)
				{
					this._isAlive = false;
					throw new Exception($"Failed to write to the socket '{this._endpoint}'. Error: {awaitable.Arguments.SocketError}");
				}
			}
		}

		/// <summary>
		/// Receives data asynchronously. Returns true if the IO is pending. Returns false if the socket already failed or the data was available in the buffer.
		/// args.Next will only be called if the call completes asynchronously.
		/// </summary>
		/// <param name="args"></param>
		/// <returns></returns>
		public bool ReceiveAsync(AsyncIOArgs args)
		{
			this.CheckDisposed();

			if (!this.IsAlive)
			{
				args.Fail = true;
				args.Result = null;

				return false;
			}

			if (this._helper == null)
				this._helper = new AsyncSocketHelper(this);

			return this._helper.Read(args);
		}
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2017 VIEApps.net
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
