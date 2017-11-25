#region Related components
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
#endregion

namespace Enyim.Caching.Memcached
{
	[DebuggerDisplay("Endpoint: {endpoint}, IsAlive = {IsAlive}")]
	public class PooledSocket : IDisposable
	{
		ILogger _logger;

		bool _isAlive;
		Socket _socket;
		EndPoint _endpoint;

		NetworkStream _stream;
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
			this._stream = new NetworkStream(socket);
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
			this._stream.Flush();
			this._helper?.DiscardBuffer();

			var available = this._socket.Available;
			if (available > 0)
			{
				if (this._logger.IsEnabled(LogLevel.Warning))
					this._logger.LogWarning($"Socket bound to {this._socket.RemoteEndPoint} has {available} unread data! This is probably a bug in the code. Instance ID was {this.InstanceId}.");

				var data = new byte[available];
				this.Read(data, 0, available);

				if (this._logger.IsEnabled(LogLevel.Warning))
				{
					var unread = Encoding.ASCII.GetString(data);
					unread = unread.Length > 255 ? unread.Substring(0, 255) + " ..." : unread;
					this._logger.LogWarning(unread);
				}
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
					this._stream?.Dispose();

					this._socket = null;
					this._stream = null;
					this.CleanupCallback = null;
				}
				catch (Exception e)
				{
					this._logger.LogError(e, "Error occured while disposing socket");
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
		public int Read()
		{
			this.CheckDisposed();

			try
			{
				return this._stream.Read();
			}
			catch (Exception e)
			{
				this._logger.LogError(e, "Error occured while reading from socket");
				this._isAlive = false;
				throw;
			}
		}

		/// <summary>
		/// Reads data from the server's response into the specified buffer.
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
					var currentRead = this._stream.Read(buffer, offset, shouldRead);
					if (currentRead < 1)
						continue;

					read += currentRead;
					offset += currentRead;
					shouldRead -= currentRead;
				}
				catch (Exception e)
				{
					this._logger.LogError(e, "Error occured while reading from socket");
					this._isAlive = false;
					throw;
				}
			}
		}

		/// <summary>
		/// Reads data from the server's response into the specified buffer.
		/// </summary>
		/// <param name="buffer">An array of <see cref="System.Byte"/> that is the storage location for the received data.</param>
		/// <param name="offset">The location in buffer to store the received data.</param>
		/// <param name="count">The number of bytes to read.</param>
		/// <remarks>This method blocks and will not return until the specified amount of bytes are read.</remarks>
		public async Task ReadAsync(byte[] buffer, int offset, int count)
		{
			this.CheckDisposed();

			var read = 0;
			var shouldRead = count;
			while (read < count)
			{
				try
				{
					var currentRead = await this._stream.ReadAsync(buffer, offset, shouldRead);
					if (currentRead < 1)
						continue;

					read += currentRead;
					offset += currentRead;
					shouldRead -= currentRead;
				}
				catch (Exception e)
				{
					this._logger.LogError(e, "Error occured while reading from socket");
					this._isAlive = false;
					throw;
				}
			}
		}

		/// <summary>
		/// Writes data into socket
		/// </summary>
		/// <param name="data"></param>
		/// <param name="offset"></param>
		/// <param name="length"></param>
		public void Write(byte[] data, int offset, int length)
		{
			this.CheckDisposed();

			this._socket.Send(data, offset, length, SocketFlags.None, out SocketError status);
			if (status != SocketError.Success)
			{
				this._isAlive = false;
				throw new IOException($"Failed to write to the socket '{this._endpoint}'. Error: {status}");
			}
		}

		/// <summary>
		/// Writes data into socket
		/// </summary>
		/// <param name="buffers"></param>
		public void Write(IList<ArraySegment<byte>> buffers)
		{
			this.CheckDisposed();

			this._socket.Send(buffers, SocketFlags.None, out SocketError status);
			if (status != SocketError.Success)
			{
				this._isAlive = false;
				throw new IOException($"Failed to write to the socket '{this._endpoint}'. Error: {status}");
			}
		}

		/// <summary>
		/// Writes data into socket
		/// </summary>
		/// <param name="buffers"></param>
		/// <returns></returns>
		public async Task WriteSync(IList<ArraySegment<byte>> buffers)
		{
			this.CheckDisposed();

			using (var awaitable = new SocketAwaitable())
			{
				awaitable.Arguments.BufferList = buffers;
				try
				{
					await this._socket.SendAsync(awaitable);
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, "Error occured while writting into socket");
					this._isAlive = false;
					throw new IOException($"Failed to write to the socket '{this._endpoint}'. Error: {awaitable.Arguments.SocketError}", ex);
				}

				if (awaitable.Arguments.SocketError != SocketError.Success)
				{
					this._isAlive = false;
					throw new IOException($"Failed to write to the socket '{this._endpoint}'. Error: {awaitable.Arguments.SocketError}");
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

		#region Network stream
		class NetworkStream : Stream
		{
			Socket _socket;

			public NetworkStream(Socket socket)
			{
				this._socket = socket;
			}

			public override bool CanRead
			{
				get { return true; }
			}

			public override bool CanSeek
			{
				get { return false; }
			}

			public override bool CanWrite
			{
				get { return false; }
			}

			public override long Length
			{
				get { throw new NotSupportedException(); }
			}

			public override long Position
			{
				get { throw new NotSupportedException(); }
				set { throw new NotSupportedException(); }
			}

			public override void Flush() { }

			public override long Seek(long offset, SeekOrigin origin)
			{
				throw new NotSupportedException();
			}

			public override void SetLength(long value)
			{
				throw new NotSupportedException();
			}

			public override void Write(byte[] buffer, int offset, int count)
			{
				throw new NotSupportedException();
			}

			public int Read()
			{
				return base.ReadByte();
			}

			public override int Read(byte[] buffer, int offset, int count)
			{
				var result = this._socket.Receive(buffer, offset, count, SocketFlags.None, out SocketError errorCode);

				// actually "0 bytes read" could mean an error as well
				if (errorCode == SocketError.Success && result > 0)
					return result;

				throw new IOException($"Failed to read from the socket '{this._socket.RemoteEndPoint}'. Error: {(errorCode == SocketError.Success ? "?" : errorCode.ToString())}");
			}

			public new Task<int> ReadAsync(byte[] buffer, int offset, int count)
			{
				var tcs = new TaskCompletionSource<int>();
				ThreadPool.QueueUserWorkItem(_ =>
				{
					try
					{
						tcs.SetResult(this.Read(buffer, offset, count));
					}
					catch (Exception ex)
					{
						tcs.SetException(ex);
					}
				});
				/*
				this._socket.BeginReceive(buffer, offset, count, SocketFlags.None, out SocketError errorCode, asyncResult =>
				{
					try
					{
						var result = this._socket.EndReceive(asyncResult, out errorCode);
						if (errorCode == SocketError.Success && result > 0)
							tcs.SetResult(result);

						throw new IOException($"Failed to read from the socket '{this._socket.RemoteEndPoint}'. Error: {(errorCode == SocketError.Success ? "?" : errorCode.ToString())}");
					}
					catch (OperationCanceledException)
					{
						tcs.SetCanceled();
					}
					catch (Exception exc)
					{
						tcs.SetException(exc);
					}
				}, null);
				*/
				return tcs.Task;
			}
		}
		#endregion

		#region AsyncSocket Helper
		/// <summary>
		/// Supports exactly one reader and writer, but they can do IO concurrently
		/// </summary>
		class AsyncSocketHelper
		{
			const int ChunkSize = 65536;

			PooledSocket socket;
			SlidingBuffer asyncBuffer;

			SocketAsyncEventArgs readEvent;
			int remainingRead;
			int expectedToRead;
			AsyncIOArgs pendingArgs;

			int isAborted;
			ManualResetEvent readInProgressEvent;

			public AsyncSocketHelper(PooledSocket socket)
			{
				this.socket = socket;
				this.asyncBuffer = new SlidingBuffer(ChunkSize);

				this.readEvent = new SocketAsyncEventArgs();
				this.readEvent.Completed += new EventHandler<SocketAsyncEventArgs>(AsyncReadCompleted);
				this.readEvent.SetBuffer(new byte[ChunkSize], 0, ChunkSize);

				this.readInProgressEvent = new ManualResetEvent(false);
			}

			/// <summary>
			/// returns true if io is pending
			/// </summary>
			/// <param name="p"></param>
			/// <returns></returns>
			public bool Read(AsyncIOArgs p)
			{
				var count = p.Count;
				if (count < 1)
					throw new ArgumentOutOfRangeException("count", "count must be > 0");
				this.expectedToRead = p.Count;
				this.pendingArgs = p;

				p.Fail = false;
				p.Result = null;

				if (this.asyncBuffer.Available >= count)
				{
					PublishResult(false);

					return false;
				}
				else
				{
					this.remainingRead = count - this.asyncBuffer.Available;
					this.isAborted = 0;

					this.BeginReceive();

					return true;
				}
			}

			public void DiscardBuffer()
			{
				this.asyncBuffer.UnsafeClear();
			}

			void BeginReceive()
			{
				while (this.remainingRead > 0)
				{
					this.readInProgressEvent.Reset();

					if (this.socket._socket.ReceiveAsync(this.readEvent))
					{
						// wait until the timeout elapses, then abort this reading process
						// EndREceive will be triggered sooner or later but its timeout
						// may be higher than our read timeout, so it's not reliable
						if (!readInProgressEvent.WaitOne(this.socket._socket.ReceiveTimeout))
							this.AbortReadAndTryPublishError(false);

						return;
					}

					this.EndReceive();
				}
			}

			void AsyncReadCompleted(object sender, SocketAsyncEventArgs e)
			{
				if (this.EndReceive())
					this.BeginReceive();
			}

			void AbortReadAndTryPublishError(bool markAsDead)
			{
				if (markAsDead)
					this.socket._isAlive = false;

				// we've been already aborted, so quit
				// both the EndReceive and the wait on the event can abort the read
				// but only one should of them should continue the async call chain
				if (Interlocked.CompareExchange(ref this.isAborted, 1, 0) != 0)
					return;

				this.remainingRead = 0;
				var p = this.pendingArgs;

				p.Fail = true;
				p.Result = null;

				this.pendingArgs.Next(p);
			}

			/// <summary>
			/// returns true when io is pending
			/// </summary>
			/// <returns></returns>
			bool EndReceive()
			{
				this.readInProgressEvent.Set();

				var read = this.readEvent.BytesTransferred;
				if (this.readEvent.SocketError != SocketError.Success || read == 0)
				{
					this.AbortReadAndTryPublishError(true);

					return false;
				}

				this.remainingRead -= read;
				this.asyncBuffer.Append(this.readEvent.Buffer, 0, read);

				if (this.remainingRead <= 0)
				{
					this.PublishResult(true);

					return false;
				}

				return true;
			}

			void PublishResult(bool isAsync)
			{
				var retval = this.pendingArgs;

				var data = new byte[this.expectedToRead];
				this.asyncBuffer.Read(data, 0, retval.Count);
				pendingArgs.Result = data;

				if (isAsync)
					pendingArgs.Next(pendingArgs);
			}
		}
		#endregion
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
