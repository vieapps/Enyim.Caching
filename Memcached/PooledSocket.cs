#region Related components
using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Enyim.Collections;
using Microsoft.Extensions.Logging;
using CacheUtils;
#endregion

namespace Enyim.Caching.Memcached
{
	[DebuggerDisplay("EndPoint: {_endpoint}, Alive = {IsAlive}")]
	public class PooledSocket : IDisposable
	{
		internal ILogger _logger;
		internal Socket _socket;
		internal EndPoint _endpoint;
		internal AsyncSocketHelper _asyncHelper;
		internal bool _isAlive;

		public PooledSocket(EndPoint endpoint, TimeSpan connectionTimeout, TimeSpan receiveTimeout, bool noDelay)
		{
			this._logger = Logger.CreateLogger<PooledSocket>();

			var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
			{
				ReceiveTimeout = receiveTimeout == TimeSpan.MaxValue ? Timeout.Infinite : (int)receiveTimeout.TotalMilliseconds,
				SendTimeout = receiveTimeout == TimeSpan.MaxValue ? Timeout.Infinite : (int)receiveTimeout.TotalMilliseconds,
				NoDelay = noDelay
			};

			socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
			this.TryConnect(socket, endpoint, connectionTimeout == TimeSpan.MaxValue ? Timeout.Infinite : (int)connectionTimeout.TotalMilliseconds);

			this._socket = socket;
			this._endpoint = endpoint;
			this._isAlive = true;
		}

		void TryConnect(Socket socket, EndPoint endpoint, int timeout)
		{
			if (endpoint is DnsEndPoint)
			{
				var dnsEndPoint = endpoint as DnsEndPoint;
				var host = dnsEndPoint.Host;
				var addresses = Dns.GetHostAddresses(dnsEndPoint.Host);
				var address = addresses.FirstOrDefault(ip => ip.AddressFamily == AddressFamily.InterNetwork || ip.AddressFamily == AddressFamily.InterNetworkV6);
				if (address == null)
					throw new ArgumentException($"Could not resolve host \"{host}\".");

				endpoint = new IPEndPoint(address, dnsEndPoint.Port);
				if (this._logger.IsEnabled(LogLevel.Trace))
					this._logger.LogDebug($"Resolved \"{host}\" to \"{address}\"");
			}

			var socketArgs = new SocketAsyncEventArgs
			{
				RemoteEndPoint = endpoint,
				UserToken = new AutoResetEvent(false)
			};
			socketArgs.Completed += (sender, args) => (args.UserToken as AutoResetEvent).Set();
			socket.ConnectAsync(socketArgs);

			if (!(socketArgs.UserToken as AutoResetEvent).WaitOne(timeout) || !socket.Connected)
				using (socket)
				{
					throw new Exception($"Could not connect to {endpoint}", new SocketException((int)SocketError.NotConnected));
				}
		}

		public Action<PooledSocket> CleanupCallback { get; set; }

		public void Reset()
		{
			this._asyncHelper?.DiscardBuffer();
			var available = this._socket.Available;
			if (available > 0)
			{
				this._logger.LogWarning($"Socket bound to {this._endpoint} has {available} unread data! This is probably a bug in the code (ID: {this.InstanceID}).");
				var data = new byte[available];
				this.Receive(data, 0, available);
				if (this._logger.IsEnabled(LogLevel.Debug))
					this._logger.LogWarning(Encoding.UTF8.GetString(data.Length > 255 ? data.Take(255).ToArray() : data));
			}
		}

		public async Task ResetAsync(CancellationToken cancellationToken = default)
		{
			this._asyncHelper?.DiscardBuffer();
			var available = this._socket.Available;
			if (available > 0)
			{
				this._logger.LogWarning($"Socket bound to {this._endpoint} has {available} unread data! This is probably a bug in the code (ID: {this.InstanceID}).");
				var data = new byte[available];
				await this.ReceiveAsync(data, 0, available, cancellationToken).ConfigureAwait(false);
				if (this._logger.IsEnabled(LogLevel.Debug))
					this._logger.LogWarning(Encoding.UTF8.GetString(data.Length > 255 ? data.Take(255).ToArray() : data));
			}
		}

		/// <summary>
		/// The identity of this instance. Used by the <see cref="IServerPool"/> to identify the instance in its inner lists.
		/// </summary>
		public Guid InstanceID { get; } = Guid.NewGuid();

		/// <summary>
		/// Gets the alive state
		/// </summary>
		public bool IsAlive => this._isAlive;

		void CheckDisposed()
		{
			if (this._socket == null)
				throw new ObjectDisposedException("PooledSocket");
		}

		/// <summary>
		/// Sends data to socket
		/// </summary>
		/// <param name="buffer"></param>
		/// <param name="offset"></param>
		/// <param name="size"></param>
		public void Send(byte[] buffer, int offset, int size)
		{
			this.CheckDisposed();
			this._socket.Send(buffer, offset, size, SocketFlags.None, out SocketError errorCode);
			if (errorCode != SocketError.Success)
			{
				this._isAlive = false;
				throw new IOException($"Failed to write to the socket \"{this._endpoint}\"");
			}
		}

		/// <summary>
		/// Sends data to socket
		/// </summary>
		/// <param name="buffer"></param>
		/// <param name="offset"></param>
		/// <param name="size"></param>
		/// <param name="cancellationToken">The cancellation token.</param>
		/// <returns></returns>
		public async Task SendAsync(byte[] buffer, int offset, int size, CancellationToken cancellationToken = default)
		{
			this.CheckDisposed();
			try
			{
				await this._socket.SendAsync(new ArraySegment<byte>(buffer, offset, size), SocketFlags.None).WithCancellationToken(cancellationToken).ConfigureAwait(false);
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (Exception ex)
			{
				this._isAlive = false;
				this._logger.LogError(ex, $"Failed to write to the socket \"{this._endpoint}\"");
				throw ex is IOException ? ex : new IOException($"Failed to write to the socket \"{this._endpoint}\"", ex);
			}
		}

		/// <summary>
		/// Sends data to socket
		/// </summary>
		/// <param name="buffers"></param>
		public void Send(IList<ArraySegment<byte>> buffers)
		{
			this.CheckDisposed();
			this._socket.Send(buffers, SocketFlags.None, out SocketError errorCode);
			if (errorCode != SocketError.Success)
			{
				this._isAlive = false;
				throw new IOException($"Failed to write to the socket \"{this._endpoint}\"");
			}
		}

		/// <summary>
		/// Sends data to socket
		/// </summary>
		/// <param name="buffers"></param>
		/// <param name="cancellationToken">The cancellation token.</param>
		/// <returns></returns>
		public async Task SendAsync(IList<ArraySegment<byte>> buffers, CancellationToken cancellationToken = default)
		{
			this.CheckDisposed();
			try
			{
				await this._socket.SendAsync(buffers, SocketFlags.None).WithCancellationToken(cancellationToken).ConfigureAwait(false);
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (Exception ex)
			{
				this._isAlive = false;
				this._logger.LogError(ex, $"Failed to write to the socket \"{this._endpoint}\"");
				throw ex is IOException ? ex : new IOException($"Failed to write to the socket \"{this._endpoint}\"", ex);
			}
		}

		/// <summary>
		/// Receives data from the server's response
		/// </summary>
		/// <param name="buffer">An array of <see cref="System.Byte"/> that is the storage location for the received data.</param>
		/// <param name="offset">The location in buffer to store the received data.</param>
		/// <param name="count">The number of bytes to read.</param>
		/// <returns>The number of read bytes</returns>
		/// <remarks>This method blocks and will not return until the specified amount of bytes are read.</remarks>
		public int Receive(byte[] buffer, int offset, int count)
		{
			this.CheckDisposed();
			var total = 0;
			var should = count;
			while (total < count)
				try
				{
					var read = this._socket.Receive(buffer, offset, should, SocketFlags.None, out SocketError errorCode);
					if (errorCode != SocketError.Success || read == 0)
						throw new IOException($"Failed to read from the socket \"{this._endpoint}\"");
					total += read;
					offset += read;
					should -= read;
				}
				catch (Exception ex)
				{
					this._isAlive = false;
					this._logger.LogError(ex, $"Failed to read from the socket \"{this._endpoint}\"");
					throw ex is IOException ? ex: new IOException($"Failed to read from the socket \"{this._endpoint}\"");
				}
			return total;
		}

		/// <summary>
		/// Receives data from the server's response
		/// </summary>
		/// <param name="buffer">An array of <see cref="System.Byte"/> that is the storage location for the received data.</param>
		/// <param name="offset">The location in buffer to store the received data.</param>
		/// <param name="count">The number of bytes to read.</param>
		/// <param name="cancellationToken">The cancellation token.</param>
		/// <returns>The number of read bytes</returns>
		public async Task<int> ReceiveAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
		{
			this.CheckDisposed();
			var total = 0;
			var should = count;
			var into = new ArraySegment<byte>(buffer, 0, should);
			while (total < count)
				try
				{
					var read = await this._socket.ReceiveAsync(into, SocketFlags.None)
												 .WithCancellationToken(cancellationToken)
												 .ConfigureAwait(false);

					total += read;
					offset += read;
					should -= read;

					into = new ArraySegment<byte>(buffer, offset, should);
				}
				catch (OperationCanceledException)
				{
					throw;
				}
				catch (Exception ex)
				{
					this._isAlive = false;
					this._logger.LogError(ex, $"Failed to read from the socket \"{this._endpoint}\"");
					throw ex is IOException ? ex : new IOException($"Failed to read from the socket \"{this._endpoint}\"");
				}
			return total;
		}

		/// <summary>
		/// Receives the next byte from the server's response
		/// </summary>
		/// <remarks>This method blocks and will not return until the value is read.</remarks>
		public byte Receive()
		{
			var buffer = new byte[1];
			return this.Receive(buffer, 0, 1) > 0 ? buffer[0] : (byte)0;
		}

		/// <summary>
		/// Receives the next byte from the server's response
		/// </summary>
		/// <remarks>This method blocks and will not return until the value is read.</remarks>
		public async Task<byte> ReceiveAsync(CancellationToken cancellationToken = default)
		{
			var buffer = new byte[1];
			return await this.ReceiveAsync(buffer, 0, 1, cancellationToken).ConfigureAwait(false) > 0 ? buffer[0] : (byte)0;
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

			this._asyncHelper = this._asyncHelper ?? new AsyncSocketHelper(this);
			return this._asyncHelper.Read(args);
		}

		/// <summary>
		/// Releases all resources used by this instance and shuts down the inner <see cref="Socket"/>. This instance will not be usable anymore.
		/// </summary>
		/// <remarks>Use the IDisposable.Dispose method if you want to release this instance back into the pool.</remarks>
		public void Destroy()
			=> this.Dispose(true);

		protected void Dispose(bool disposing)
		{
			if (disposing)
			{
				try
				{
					this._socket?.Dispose();
					this._socket = null;
					this.CleanupCallback = null;
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, "Error occurred while disposing");
				}
			}
			else
				this.CleanupCallback?.Invoke(this);
		}

		public void Dispose()
		{
			GC.SuppressFinalize(this);
			this.Dispose(false);
		}

		~PooledSocket()
			=> this.Dispose(true);
	}

	#region Helpers of Async I/O Socket 
	/// <summary>
	/// Supports exactly one reader and writer, but they can do IO concurrently
	/// </summary>
	internal class AsyncSocketHelper
	{
		const int ChunkSize = 65536;

		PooledSocket _socket;
		SlidingBuffer _buffer;

		AsyncIOArgs _pendingArgs;
		SocketAsyncEventArgs _readEvent;
		ManualResetEvent _resetEvent;

		int _remainingRead, _expectedToRead, _isAborted;

		public AsyncSocketHelper(PooledSocket socket)
		{
			this._socket = socket;
			this._buffer = new SlidingBuffer(ChunkSize);

			this._readEvent = new SocketAsyncEventArgs();
			this._readEvent.Completed += new EventHandler<SocketAsyncEventArgs>(AsyncReadCompleted);
			this._readEvent.SetBuffer(new byte[ChunkSize], 0, ChunkSize);

			this._resetEvent = new ManualResetEvent(false);
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
			this._expectedToRead = p.Count;
			this._pendingArgs = p;

			p.Fail = false;
			p.Result = null;

			if (this._buffer.Available >= count)
			{
				PublishResult(false);

				return false;
			}
			else
			{
				this._remainingRead = count - this._buffer.Available;
				this._isAborted = 0;

				this.BeginReceive();

				return true;
			}
		}

		public void DiscardBuffer()
		{
			this._buffer.UnsafeClear();
		}

		void BeginReceive()
		{
			while (this._remainingRead > 0)
			{
				this._resetEvent.Reset();

				if (this._socket._socket.ReceiveAsync(this._readEvent))
				{
					// wait until the timeout elapses, then abort this reading process
					// EndREceive will be triggered sooner or later but its timeout
					// may be higher than our read timeout, so it's not reliable
					if (!this._resetEvent.WaitOne(this._socket._socket.ReceiveTimeout))
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
				this._socket._isAlive = false;

			// we've been already aborted, so quit
			// both the EndReceive and the wait on the event can abort the read
			// but only one should of them should continue the async call chain
			if (Interlocked.CompareExchange(ref this._isAborted, 1, 0) != 0)
				return;

			this._remainingRead = 0;
			var p = this._pendingArgs;

			p.Fail = true;
			p.Result = null;

			this._pendingArgs.Next(p);
		}

		/// <summary>
		/// returns true when io is pending
		/// </summary>
		/// <returns></returns>
		bool EndReceive()
		{
			this._resetEvent.Set();

			var read = this._readEvent.BytesTransferred;
			if (this._readEvent.SocketError != SocketError.Success || read == 0)
			{
				this.AbortReadAndTryPublishError(true);

				return false;
			}

			this._remainingRead -= read;
			this._buffer.Append(this._readEvent.Buffer, 0, read);

			if (this._remainingRead <= 0)
			{
				this.PublishResult(true);

				return false;
			}

			return true;
		}

		void PublishResult(bool isAsync)
		{
			var retval = this._pendingArgs;

			var data = new byte[this._expectedToRead];
			this._buffer.Read(data, 0, retval.Count);
			this._pendingArgs.Result = data;

			if (isAsync)
				this._pendingArgs.Next(this._pendingArgs);
		}
	}

	public class AsyncIOArgs
	{
		public Action<AsyncIOArgs> Next { get; set; }

		public int Count { get; set; }

		public byte[] Result { get; internal set; }

		public bool Fail { get; internal set; }
	}
	#endregion

	#region SlidingBuffer
	/// <summary>
	/// Supports exactly one reader and writer, but they can access the buffer concurrently.
	/// </summary>
	internal class SlidingBuffer
	{
		readonly InterlockedQueue<Segment> _buffers;
		readonly int _chunkSize;
		Segment _lastSegment;
		int _available;

		public SlidingBuffer(int chunkSize)
		{
			this._chunkSize = chunkSize;
			this._buffers = new InterlockedQueue<Segment>();
		}

		public int Available { get { return this._available; } }

		public int Read(byte[] buffer, int offset, int count)
		{
			var read = 0;
			Segment segment;

			while (read < count && this._buffers.Peek(out segment))
			{
				var available = Math.Min(segment.WriteOffset - segment.ReadOffset, count - read);

				if (available > 0)
				{
					Buffer.BlockCopy(segment.Data, segment.ReadOffset, buffer, offset + read, available);
					read += available;
					segment.ReadOffset += available;
				}

				// are we at the end of the segment?
				if (segment.ReadOffset == segment.WriteOffset)
				{
					// we can dispose the current segment if it's not the last 
					// (which is probably being written by the receiver)
					if (this._lastSegment != segment)
						this._buffers.Dequeue(out segment);
				}
			}

			Interlocked.Add(ref this._available, -read);

			return read;
		}

		public void Append(byte[] buffer, int offset, int count)
		{
			if (buffer == null || buffer.Length == 0 || count == 0)
				return;

			// try to append the data to the last segment
			// if the data is larger than the ChunkSize we copy it and append it as one chunk
			// if the data does not fit into the last segment we allocate a new
			// so data is never split (at the price of some wasted bytes)
			var last = this._lastSegment;
			var shouldQueue = false;

			if (count > this._chunkSize)
			{
				// big data, append it
				last = new Segment(new byte[count]);
				shouldQueue = true;
			}
			else
			{
				var remaining = last == null
					? 0
					: last.Data.Length - last.WriteOffset;

				// no space, create a new chunk
				if (remaining < count)
				{
					last = new Segment(new byte[this._chunkSize]);
					shouldQueue = true;
				}
			}

			Buffer.BlockCopy(buffer, offset, last.Data, last.WriteOffset, count);

			// first we update the lastSegment reference, then we enque the new segment
			// this way Read can safely dequeue (discard) the last item it processed and 
			// continue  on the next one
			// doing it in reverse would make Read dequeue the current segment (the one we just inserted)
			if (shouldQueue)
			{
				Interlocked.Exchange(ref this._lastSegment, last);
				this._buffers.Enqueue(last);
			}

			// advertise that we have more data available for reading
			// we have to use Interlocked because in the same time the reader
			// can remove data and will decrease the value of Available
			Interlocked.Add(ref last.WriteOffset, count);
			Interlocked.Add(ref this._available, count);
		}

		public void UnsafeClear()
		{
			Segment tmp;

			this._lastSegment = null;
			while (this._buffers.Dequeue(out tmp)) ;

			this._available = 0;
		}

		#region [ Segment                      ]
		private class Segment
		{
			public Segment(byte[] data)
			{
				this.Data = data;
			}

			public readonly byte[] Data;
			public int WriteOffset;
			public int ReadOffset;
		}
		#endregion

	}
	#endregion

}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2021 VIEApps.net
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
