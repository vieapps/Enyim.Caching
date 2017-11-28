using System;
using System.IO;
using System.Text;
using System.Collections.Generic;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Text
{
	internal static class TextSocketHelper
	{
		public const string CommandTerminator = "\r\n";

		const string GenericErrorResponse = "ERROR";
		const string ClientErrorResponse = "CLIENT_ERROR ";
		const string ServerErrorResponse = "SERVER_ERROR ";
		const int ErrorResponseLength = 13;

		static ILogger Logger;

		/// <summary>
		/// Reads the response of the server.
		/// </summary>
		/// <returns>The data sent by the memcached server.</returns>
		/// <exception cref="System.InvalidOperationException">The server did not sent a response or an empty line was returned.</exception>
		/// <exception cref="Enyim.Caching.Memcached.MemcachedException">The server did not specified any reason just returned the string ERROR. - or - The server returned a SERVER_ERROR, in this case the Message of the exception is the message returned by the server.</exception>
		/// <exception cref="Enyim.Caching.Memcached.MemcachedClientException">The server did not recognize the request sent by the client. The Message of the exception is the message returned by the server.</exception>
		public static string ReadResponse(PooledSocket socket)
		{
			string response = TextSocketHelper.ReadLine(socket);

			if (String.IsNullOrEmpty(response))
				throw new MemcachedClientException("Empty response received.");

			if (String.Compare(response, TextSocketHelper.GenericErrorResponse, StringComparison.Ordinal) == 0)
				throw new NotSupportedException("Operation is not supported by the server or the request was malformed. If the latter please report the bug to the developers.");

			TextSocketHelper.Logger = TextSocketHelper.Logger ?? Caching.Logger.CreateLogger(typeof(TextSocketHelper));
			if (TextSocketHelper.Logger.IsEnabled(LogLevel.Debug))
				TextSocketHelper.Logger.LogDebug("Received response: " + response);

			if (response.Length >= ErrorResponseLength)
			{
				if (String.Compare(response, 0, TextSocketHelper.ClientErrorResponse, 0, TextSocketHelper.ErrorResponseLength, StringComparison.Ordinal) == 0)
					throw new MemcachedClientException(response.Remove(0, TextSocketHelper.ErrorResponseLength));

				else if (String.Compare(response, 0, TextSocketHelper.ServerErrorResponse, 0, TextSocketHelper.ErrorResponseLength, StringComparison.Ordinal) == 0)
					throw new MemcachedException(response.Remove(0, TextSocketHelper.ErrorResponseLength));
			}

			return response;
		}

		/// <summary>
		/// Reads a line from the socket. A line is terninated by \r\n.
		/// </summary>
		/// <returns></returns>
		static string ReadLine(PooledSocket socket)
		{
			using (var stream = new MemoryStream(50))
			{
				var gotR = false;
				byte data;

				while (true)
				{
					data = socket.Read();

					if (data == 13)
					{
						gotR = true;
						continue;
					}

					if (gotR)
					{
						if (data == 10)
							break;

						stream.WriteByte(13);

						gotR = false;
					}

					stream.WriteByte(data);
				}

				var result = Encoding.ASCII.GetString(stream.ToArray(), 0, (int)stream.Length);

				Logger = Logger ?? Caching.Logger.CreateLogger(typeof(TextSocketHelper));
				if (Logger.IsEnabled(LogLevel.Debug))
					Logger.LogDebug("ReadLine: " + result);

				return result;
			}
		}

		/// <summary>
		/// Gets the bytes representing the specified command. returned buffer can be used to streamline multiple writes into one Write on the Socket
		/// using the <see cref="Enyim.Caching.Memcached.PooledSocket.Write(IList&lt;ArraySegment&lt;byte&gt;&gt;)"/>
		/// </summary>
		/// <param name="value">The command to be converted.</param>
		/// <returns>The buffer containing the bytes representing the command. The command must be terminated by \r\n.</returns>
		/// <remarks>The Nagle algorithm is disabled on the socket to speed things up, so it's recommended to convert a command into a buffer
		/// and use the <see cref="Enyim.Caching.Memcached.PooledSocket.Write(IList&lt;ArraySegment&lt;byte&gt;&gt;)"/> to send the command and the additional buffers in one transaction.</remarks>
		public unsafe static IList<ArraySegment<byte>> GetCommandBuffer(string value)
		{
			var data = new ArraySegment<byte>(Encoding.ASCII.GetBytes(value));

			return new ArraySegment<byte>[] { data };
		}

		public unsafe static IList<ArraySegment<byte>> GetCommandBuffer(string value, IList<ArraySegment<byte>> list)
		{
			var data = new ArraySegment<byte>(Encoding.ASCII.GetBytes(value));

			list.Add(data);

			return list;
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
