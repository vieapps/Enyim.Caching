using System;
using System.Net;
using System.Collections.Generic;
using System.Configuration;
using System.Xml;

using Enyim.Reflection;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Protocol.Binary;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Enyim.Caching.Configuration
{
	/// <summary>
	/// Configuration class
	/// </summary>
	public class MemcachedClientConfiguration : IMemcachedClientConfiguration
	{
		// these are lazy initialized in the getters
		private Type nodeLocator;
		private ITranscoder _transcoder;
		private IMemcachedKeyTransformer _keyTransformer;
		private ILogger<MemcachedClientConfiguration> _logger;

		/// <summary>
		/// Initializes a new instance of the <see cref="T:MemcachedClientConfiguration"/> class.
		/// </summary>
		public MemcachedClientConfiguration(ILoggerFactory loggerFactory, IOptions<MemcachedClientOptions> optionsAccessor)
		{
			if (optionsAccessor == null)
				throw new ArgumentNullException(nameof(optionsAccessor));

			this._logger = loggerFactory.CreateLogger<MemcachedClientConfiguration>();

			var options = optionsAccessor.Value;
			this.Servers = new List<EndPoint>();
			foreach (var server in options.Servers)
			{
				if (IPAddress.TryParse(server.Address, out IPAddress address))
					this.Servers.Add(new IPEndPoint(address, server.Port));
				else
					this.Servers.Add(new DnsEndPoint(server.Address, server.Port));
			}
			this.SocketPool = options.SocketPool;
			this.Protocol = options.Protocol;

			if (options.Authentication != null && !string.IsNullOrEmpty(options.Authentication.Type))
			{
				try
				{
					var authenticationType = Type.GetType(options.Authentication.Type);
					if (authenticationType != null)
					{
						this._logger.LogDebug($"Authentication type is {authenticationType}.");

						this.Authentication = new AuthenticationConfiguration();
						this.Authentication.Type = authenticationType;
						foreach (var parameter in options.Authentication.Parameters)
						{
							this.Authentication.Parameters[parameter.Key] = parameter.Value;
							this._logger.LogDebug($"Authentication {parameter.Key} is '{parameter.Value}'.");
						}
					}
					else
					{
						this._logger.LogError($"Unable to load authentication type {options.Authentication.Type}.");
					}
				}
				catch (Exception ex)
				{
					this._logger.LogError(new EventId(), ex, $"Unable to load authentication type {options.Authentication.Type}.");
				}
			}

			if (!string.IsNullOrEmpty(options.KeyTransformer))
			{
				try
				{
					var keyTransformerType = Type.GetType(options.KeyTransformer);
					if (keyTransformerType != null)
					{
						this.KeyTransformer = Activator.CreateInstance(keyTransformerType) as IMemcachedKeyTransformer;
						this._logger.LogDebug($"Use '{options.KeyTransformer}' KeyTransformer");
					}
				}
				catch (Exception ex)
				{
					this._logger.LogError(new EventId(), ex, $"Unable to load '{options.KeyTransformer}' KeyTransformer");
				}
			}

			if (options.Transcoder != null)
				this._transcoder = options.Transcoder;

			if (options.NodeLocatorFactory != null)
				this.NodeLocatorFactory = options.NodeLocatorFactory;
		}

		public MemcachedClientConfiguration(ILoggerFactory loggerFactory, MemcachedClientConfigurationSectionHandler configuration)
		{
			this._logger = loggerFactory.CreateLogger<MemcachedClientConfiguration>();

			if (!Enum.TryParse<MemcachedProtocol>(configuration.Section.Attributes["protocol"]?.Value ?? "Binary", out MemcachedProtocol protocol))
				protocol = MemcachedProtocol.Binary;
			this.Protocol = protocol;

			this.Servers = new List<EndPoint>();
			if (configuration.Section.SelectNodes("servers/add") is XmlNodeList servers)
				foreach (XmlNode server in servers)
				{
					var address = server.Attributes["address"]?.Value ?? "localhost";
					var port = Convert.ToInt32(server.Attributes["port"]?.Value ?? "11211");
					this.AddServer(address, port);
				}

			this.SocketPool = new SocketPoolConfiguration();
			if (configuration.Section.SelectSingleNode("socketPool") is XmlNode socketpool)
			{
				if (socketpool.Attributes["maxPoolSize"]?.Value != null)
					this.SocketPool.MaxPoolSize = Convert.ToInt32(socketpool.Attributes["maxPoolSize"].Value);
				if (socketpool.Attributes["minPoolSize"]?.Value != null)
					this.SocketPool.MinPoolSize = Convert.ToInt32(socketpool.Attributes["minPoolSize"].Value);
				if (socketpool.Attributes["connectionTimeout"]?.Value != null)
					this.SocketPool.ConnectionTimeout = TimeSpan.Parse(socketpool.Attributes["connectionTimeout"].Value);
				if (socketpool.Attributes["deadTimeout"]?.Value != null)
					this.SocketPool.DeadTimeout = TimeSpan.Parse(socketpool.Attributes["deadTimeout"].Value);
				if (socketpool.Attributes["queueTimeout"]?.Value != null)
					this.SocketPool.QueueTimeout = TimeSpan.Parse(socketpool.Attributes["queueTimeout"].Value);
				if (socketpool.Attributes["receiveTimeout"]?.Value != null)
					this.SocketPool.ReceiveTimeout = TimeSpan.Parse(socketpool.Attributes["receiveTimeout"].Value);

				var failurePolicy = socketpool.Attributes["failurePolicy"]?.Value;
				if ("throttling" == failurePolicy)
				{
					var failureThreshold = Convert.ToInt32(socketpool.Attributes["failureThreshold"]?.Value ?? "4");
					var resetAfter = TimeSpan.Parse(socketpool.Attributes["resetAfter"]?.Value ?? "00:05:00");
					this.SocketPool.FailurePolicyFactory = new ThrottlingFailurePolicyFactory(failureThreshold, resetAfter);
				}
			}

			if (configuration.Section.SelectSingleNode("authentication") is XmlNode authentication)
				if (authentication.Attributes["type"]?.Value != null)
					try
					{
						var authenticationType = Type.GetType(authentication.Attributes["type"].Value);
						if (authenticationType != null)
						{
							this._logger.LogDebug($"Authentication type is {authenticationType}.");

							this.Authentication = new AuthenticationConfiguration();
							this.Authentication.Type = authenticationType;
							if (authentication.Attributes["zone"]?.Value != null)
								this.Authentication.Parameters.Add("zone", authentication.Attributes["zone"].Value);
							if (authentication.Attributes["userName"]?.Value != null)
								this.Authentication.Parameters.Add("userName", authentication.Attributes["userName"].Value);
							if (authentication.Attributes["password"]?.Value != null)
								this.Authentication.Parameters.Add("password", authentication.Attributes["password"].Value);
						}
						else
						{
							this._logger.LogError($"Unable to load authentication type {authentication.Attributes["type"].Value}.");
						}
					}
					catch (Exception ex)
					{
						this._logger.LogError(new EventId(), ex, $"Unable to load authentication type {authentication.Attributes["type"].Value}.");
					}
		}

		/// <summary>
		/// Adds a new server to the pool.
		/// </summary>
		/// <param name="address">The address and the port of the server in the format 'host:port'.</param>
		public void AddServer(string address)
		{
			this.Servers.Add(ConfigurationHelper.ResolveToEndPoint(address));
		}

		/// <summary>
		/// Adds a new server to the pool.
		/// </summary>
		/// <param name="address">The host name or IP address of the server.</param>
		/// <param name="port">The port number of the memcached instance.</param>
		public void AddServer(string address, int port)
		{
			this.Servers.Add(ConfigurationHelper.ResolveToEndPoint(address, port));
		}

		/// <summary>
		/// Gets a list of <see cref="T:IPEndPoint"/> each representing a Memcached server in the pool.
		/// </summary>
		public IList<EndPoint> Servers { get; private set; }

		/// <summary>
		/// Gets the configuration of the socket pool.
		/// </summary>
		public ISocketPoolConfiguration SocketPool { get; private set; }

		/// <summary>
		/// Gets the authentication settings.
		/// </summary>
		public IAuthenticationConfiguration Authentication { get; private set; }

		/// <summary>
		/// Gets or sets the <see cref="T:Enyim.Caching.Memcached.IMemcachedKeyTransformer"/> which will be used to convert item keys for Memcached.
		/// </summary>
		public IMemcachedKeyTransformer KeyTransformer
		{
			get { return this._keyTransformer ?? (this._keyTransformer = new DefaultKeyTransformer()); }
			set { this._keyTransformer = value; }
		}

		/// <summary>
		/// Gets or sets the Type of the <see cref="T:Enyim.Caching.Memcached.IMemcachedNodeLocator"/> which will be used to assign items to Memcached nodes.
		/// </summary>
		/// <remarks>If both <see cref="M:NodeLocator"/> and  <see cref="M:NodeLocatorFactory"/> are assigned then the latter takes precedence.</remarks>
		public Type NodeLocator
		{
			get { return this.nodeLocator; }
			set
			{
				ConfigurationHelper.CheckForInterface(value, typeof(IMemcachedNodeLocator));
				this.nodeLocator = value;
			}
		}

		/// <summary>
		/// Gets or sets the NodeLocatorFactory instance which will be used to create a new IMemcachedNodeLocator instances.
		/// </summary>
		/// <remarks>If both <see cref="M:NodeLocator"/> and  <see cref="M:NodeLocatorFactory"/> are assigned then the latter takes precedence.</remarks>
		public IProviderFactory<IMemcachedNodeLocator> NodeLocatorFactory { get; set; }

		/// <summary>
		/// Gets or sets the <see cref="T:Enyim.Caching.Memcached.ITranscoder"/> which will be used serialize or deserialize items.
		/// </summary>
		public ITranscoder Transcoder
		{
			get { return this._transcoder ?? (this._transcoder = new DefaultTranscoder()); }
			set { this._transcoder = value; }
		}

		/// <summary>
		/// Gets or sets the type of the communication between client and server.
		/// </summary>
		public MemcachedProtocol Protocol { get; set; }

		#region [ interface                     ]

		IList<System.Net.EndPoint> IMemcachedClientConfiguration.Servers
		{
			get { return this.Servers; }
		}

		ISocketPoolConfiguration IMemcachedClientConfiguration.SocketPool
		{
			get { return this.SocketPool; }
		}

		IAuthenticationConfiguration IMemcachedClientConfiguration.Authentication
		{
			get { return this.Authentication; }
		}

		IMemcachedKeyTransformer IMemcachedClientConfiguration.CreateKeyTransformer()
		{
			return this.KeyTransformer;
		}

		IMemcachedNodeLocator IMemcachedClientConfiguration.CreateNodeLocator()
		{
			var f = this.NodeLocatorFactory;
			if (f != null) return f.Create();

			return this.NodeLocator == null
					? new SingleNodeLocator()
				: (IMemcachedNodeLocator)FastActivator.Create(this.NodeLocator);
		}

		ITranscoder IMemcachedClientConfiguration.CreateTranscoder()
		{
			return this.Transcoder;
		}

		IServerPool IMemcachedClientConfiguration.CreatePool()
		{
			switch (this.Protocol)
			{
				case MemcachedProtocol.Text:
					return new DefaultServerPool(this, new Memcached.Protocol.Text.TextOperationFactory(), _logger);

				case MemcachedProtocol.Binary:
					return new BinaryPool(this, _logger);
			}

			throw new ArgumentOutOfRangeException("Unknown protocol: " + (int)this.Protocol);
		}

		#endregion

	}

	public class MemcachedClientConfigurationSectionHandler : IConfigurationSectionHandler
	{
		public object Create(object parent, object configContext, XmlNode section)
		{
			this._section = section;
			return this;
		}
		XmlNode _section = null;
		public XmlNode Section { get { return this._section; } }
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk? enyim.com
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
