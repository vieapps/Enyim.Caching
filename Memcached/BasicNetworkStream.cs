using System;
using System.IO;
using System.Net.Sockets;

namespace Enyim.Caching.Memcached
{
	public partial class PooledSocket
	{
		class BasicNetworkStream : Stream
		{
			Socket _socket;

			public BasicNetworkStream(Socket socket)
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

			public override void Flush()
			{
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

			public override int Read(byte[] buffer, int offset, int count)
			{
				var result = this._socket.Receive(buffer, offset, count, SocketFlags.None, out SocketError errorCode);

				// actually "0 bytes read" could mean an error as well
				if (errorCode == SocketError.Success && result > 0)
					return result;

				throw new IOException(String.Format("Failed to read from the socket '{0}'. Error: {1}", this._socket.RemoteEndPoint, errorCode == SocketError.Success ? "?" : errorCode.ToString()));
			}

			public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
			{
				var result = this._socket.BeginReceive(buffer, offset, count, SocketFlags.None, out SocketError errorCode, callback, state);

				if (errorCode == SocketError.Success)
					return result;

				throw new IOException(String.Format("Failed to read from the socket '{0}'. Error: {1}", this._socket.RemoteEndPoint, errorCode));
			}

			public override int EndRead(IAsyncResult asyncResult)
			{
				var result = this._socket.EndReceive(asyncResult, out SocketError errorCode);

				// actually "0 bytes read" could mean an error as well
				if (errorCode == SocketError.Success && result > 0)
					return result;

				throw new IOException(String.Format("Failed to read from the socket '{0}'. Error: {1}", this._socket.RemoteEndPoint, errorCode));
			}
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
