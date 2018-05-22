using System;
using Enyim.Caching.Memcached;

namespace Enyim.Caching.Configuration
{
	public class SocketPoolConfiguration : ISocketPoolConfiguration
	{
		int _minPoolSize = 10;
		int _maxPoolSize = 20;
		TimeSpan _connectionTimeout = new TimeSpan(0, 0, 10);
		TimeSpan _receiveTimeout = new TimeSpan(0, 0, 10);
		TimeSpan _deadTimeout = new TimeSpan(0, 0, 10);
		TimeSpan _queueTimeout = new TimeSpan(0, 0, 0, 0, 100);
		INodeFailurePolicyFactory _policyFactory = new FailImmediatelyPolicyFactory();

		int ISocketPoolConfiguration.MinPoolSize
		{
			get { return this._minPoolSize; }
			set
			{
				if (value < 0)
					throw new ArgumentOutOfRangeException("value", "MinPoolSize must be >= 0!");

				if (value > this._maxPoolSize)
					throw new ArgumentOutOfRangeException("value", "MinPoolSize must be <= MaxPoolSize!");

				this._minPoolSize = value;
			}
		}

		/// <summary>
		/// Gets or sets a value indicating the maximum amount of sockets per server in the socket pool.
		/// </summary>
		/// <returns>The maximum amount of sockets per server in the socket pool. The default is 20.</returns>
		/// <remarks>It should be 0.75 * (number of threads) for optimal performance.</remarks>
		int ISocketPoolConfiguration.MaxPoolSize
		{
			get { return this._maxPoolSize; }
			set
			{
				if (value < this._minPoolSize)
					throw new ArgumentOutOfRangeException("value", "MaxPoolSize must be >= MinPoolSize!");

				this._maxPoolSize = value;
			}
		}

		TimeSpan ISocketPoolConfiguration.ConnectionTimeout
		{
			get { return this._connectionTimeout; }
			set
			{
				if (value < TimeSpan.Zero)
					throw new ArgumentOutOfRangeException("value", "value must be positive");

				this._connectionTimeout = value;
			}
		}

		TimeSpan ISocketPoolConfiguration.ReceiveTimeout
		{
			get { return this._receiveTimeout; }
			set
			{
				if (value < TimeSpan.Zero)
					throw new ArgumentOutOfRangeException("value", "value must be positive");

				this._receiveTimeout = value;
			}
		}

		TimeSpan ISocketPoolConfiguration.QueueTimeout
		{
			get { return this._queueTimeout; }
			set
			{
				if (value < TimeSpan.Zero)
					throw new ArgumentOutOfRangeException("value", "value must be positive");

				this._queueTimeout = value;
			}
		}

		TimeSpan ISocketPoolConfiguration.DeadTimeout
		{
			get { return this._deadTimeout; }
			set
			{
				if (value < TimeSpan.Zero)
					throw new ArgumentOutOfRangeException("value", "value must be positive");

				this._deadTimeout = value;
			}
		}

		bool ISocketPoolConfiguration.NoDelay { get; set; } = true;

		INodeFailurePolicyFactory ISocketPoolConfiguration.FailurePolicyFactory
		{
			get { return this._policyFactory; }
			set
			{
				this._policyFactory = value ?? throw new ArgumentNullException("value");
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
