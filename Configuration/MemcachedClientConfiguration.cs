#region Related components
using System;
using System.Net;
using System.Collections.Generic;
using System.Configuration;
using System.Xml;

using Enyim.Reflection;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Protocol.Text;
using Enyim.Caching.Memcached.Protocol.Binary;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
#endregion

namespace Enyim.Caching.Configuration
{
	/// <summary>
	/// Configuration class
	/// </summary>
	public class MemcachedClientConfiguration : IMemcachedClientConfiguration
	{
		// these are lazy initialized in the getters
		Type _nodeLocatorType;
		ITranscoder _transcoder;
		IMemcachedKeyTransformer _keyTransformer;
		ILogger _logger;

		/// <summary>
		/// Initializes a new instance of the <see cref="MemcachedClientConfiguration"/> class.
		/// </summary>
		/// <param name="loggerFactory"></param>
		public MemcachedClientConfiguration(ILoggerFactory loggerFactory = null)
		{
			this.PrepareLogger(loggerFactory);

			this.Protocol = MemcachedProtocol.Binary;
			this.Servers = new List<EndPoint>();
			this.SocketPool = new SocketPoolConfiguration();
			this.Authentication = new AuthenticationConfiguration();
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MemcachedClientConfiguration"/> class.
		/// </summary>
		/// <param name="loggerFactory"></param>
		/// <param name="options"></param>
		public MemcachedClientConfiguration(ILoggerFactory loggerFactory, IOptions<MemcachedClientOptions> options)
		{
			if (options == null)
				throw new ArgumentNullException(nameof(options));

			this.PrepareLogger(loggerFactory);

			var configuration = options.Value;

			this.Protocol = configuration.Protocol;

			this.Servers = new List<EndPoint>();
			foreach (var server in configuration.Servers)
				if (server.Address.IndexOf(":") > 0)
					this.AddServer(server.Address);
				else
					this.AddServer(server.Address, server.Port);

			this.SocketPool = configuration.SocketPool;

			if (configuration.Authentication != null && !string.IsNullOrEmpty(configuration.Authentication.Type))
			{
				try
				{
					var authenticationType = Type.GetType(configuration.Authentication.Type);
					if (authenticationType != null)
					{
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Authentication type is {authenticationType}");

						this.Authentication = new AuthenticationConfiguration()
						{
							Type = configuration.Authentication.Type
						};
						foreach (var parameter in configuration.Authentication.Parameters)
						{
							this.Authentication.Parameters[parameter.Key] = parameter.Value;
							if (this._logger.IsEnabled(LogLevel.Debug))
								this._logger.LogDebug($"Authentication {parameter.Key} is '{parameter.Value}'.");
						}
					}
					else
					{
						this._logger.LogError($"Unable to load authentication type {configuration.Authentication.Type}.");
					}
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, $"Unable to load authentication type {configuration.Authentication.Type}.");
				}
			}

			if (!string.IsNullOrWhiteSpace(configuration.KeyTransformer))
			{
				try
				{
					var keyTransformerType = Type.GetType(configuration.KeyTransformer);
					if (keyTransformerType != null)
					{
						this.KeyTransformer = FastActivator.Create(keyTransformerType) as IMemcachedKeyTransformer;
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Use '{configuration.KeyTransformer}' of key-transformer");
					}
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, $"Unable to load '{configuration.KeyTransformer}' of key-transformer");
				}
			}

			if (configuration.Transcoder != null)
				this._transcoder = configuration.Transcoder;

			if (configuration.NodeLocatorFactory != null)
				this.NodeLocatorFactory = configuration.NodeLocatorFactory;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MemcachedClientConfiguration"/> class.
		/// </summary>
		/// <param name="loggerFactory"></param>
		/// <param name="configuration"></param>
		public MemcachedClientConfiguration(ILoggerFactory loggerFactory, MemcachedClientConfigurationSectionHandler configuration)
		{
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));

			this.PrepareLogger(loggerFactory);

			if (!Enum.TryParse<MemcachedProtocol>(configuration.Section.Attributes["protocol"]?.Value ?? "Binary", out MemcachedProtocol protocol))
				protocol = MemcachedProtocol.Binary;
			this.Protocol = protocol;

			this.Servers = new List<EndPoint>();
			if (configuration.Section.SelectNodes("servers/add") is XmlNodeList servers)
				foreach (XmlNode server in servers)
				{
					var address = server.Attributes["address"]?.Value ?? "localhost";
					if (address.IndexOf(":") > 0)
						this.AddServer(address);
					else
						this.AddServer(address, Convert.ToInt32(server.Attributes["port"]?.Value ?? "11211"));
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
							if (this._logger.IsEnabled(LogLevel.Debug))
								this._logger.LogDebug($"Authentication type is {authenticationType}");

							this.Authentication = new AuthenticationConfiguration()
							{
								Type = authentication.Attributes["type"].Value
							};
							if (authentication.Attributes["zone"]?.Value != null)
								this.Authentication.Parameters.Add("zone", authentication.Attributes["zone"].Value);
							if (authentication.Attributes["userName"]?.Value != null)
								this.Authentication.Parameters.Add("userName", authentication.Attributes["userName"].Value);
							if (authentication.Attributes["password"]?.Value != null)
								this.Authentication.Parameters.Add("password", authentication.Attributes["password"].Value);
						}
						else
						{
							this._logger.LogError($"Unable to load authentication type [{authentication.Attributes["type"].Value}]");
						}
					}
					catch (Exception ex)
					{
						this._logger.LogError(ex, $"Unable to load authentication type [{authentication.Attributes["type"].Value}]");
					}

			if (configuration.Section.SelectSingleNode("keyTransformer") is XmlNode keyTransformer)
				if (keyTransformer.Attributes["type"]?.Value != null)
					try
					{
						this._keyTransformer = FastActivator.Create(Type.GetType(keyTransformer.Attributes["type"].Value)) as IMemcachedKeyTransformer;
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Use '{keyTransformer.Attributes["type"].Value}' of key-transformer");
					}
					catch (Exception ex)
					{
						this._logger.LogError(ex, $"Unable to load key transformer [{keyTransformer.Attributes["type"].Value}]");
					}

			if (configuration.Section.SelectSingleNode("transcoder") is XmlNode transcoder)
				if (transcoder.Attributes["type"]?.Value != null)
					try
					{
						this._transcoder = FastActivator.Create(Type.GetType(transcoder.Attributes["type"].Value)) as ITranscoder;
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Use '{transcoder.Attributes["type"].Value}' transcoder");
					}
					catch (Exception ex)
					{
						this._logger.LogError(ex, $"Unable to load transcoder [{transcoder.Attributes["type"].Value}]");
					}

			if (configuration.Section.SelectSingleNode("nodeLocator") is XmlNode nodeLocator)
				if (nodeLocator.Attributes["type"]?.Value != null)
					try
					{
						this._nodeLocatorType = Type.GetType(nodeLocator.Attributes["type"].Value);
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Use '{nodeLocator.Attributes["type"].Value}' node-locator");
					}
					catch (Exception ex)
					{
						this._logger.LogError(ex, $"Unable to load node-locator [{nodeLocator.Attributes["type"].Value}]");
					}
		}

		void PrepareLogger(ILoggerFactory loggerFactory)
		{
			LogManager.AssignLoggerFactory(loggerFactory);
			this._logger = LogManager.CreateLogger<MemcachedClientConfiguration>();
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
		/// Gets a list of <see cref="IPEndPoint"/> each representing a Memcached server in the pool.
		/// </summary>
		public IList<EndPoint> Servers { get; internal set; }

		/// <summary>
		/// Gets the configuration of the socket pool.
		/// </summary>
		public ISocketPoolConfiguration SocketPool { get; internal set; }

		/// <summary>
		/// Gets the authentication settings.
		/// </summary>
		public IAuthenticationConfiguration Authentication { get; internal set; }

		/// <summary>
		/// Gets or sets the <see cref="Enyim.Caching.Memcached.IMemcachedKeyTransformer"/> which will be used to convert item keys for Memcached.
		/// </summary>
		public IMemcachedKeyTransformer KeyTransformer
		{
			get
			{
				return this._keyTransformer ?? (this._keyTransformer = new DefaultKeyTransformer());
			}
			set
			{
				this._keyTransformer = value;
			}
		}

		/// <summary>
		/// Gets or sets the Type of the <see cref="Enyim.Caching.Memcached.IMemcachedNodeLocator"/> which will be used to assign items to Memcached nodes.
		/// </summary>
		/// <remarks>If both <see cref="M:NodeLocator"/> and  <see cref="M:NodeLocatorFactory"/> are assigned then the latter takes precedence.</remarks>
		public Type NodeLocator
		{
			get
			{
				return this._nodeLocatorType;
			}
			set
			{
				ConfigurationHelper.CheckForInterface(value, typeof(IMemcachedNodeLocator));
				this._nodeLocatorType = value;
			}
		}

		/// <summary>
		/// Gets or sets the NodeLocatorFactory instance which will be used to create a new IMemcachedNodeLocator instances.
		/// </summary>
		/// <remarks>If both <see cref="NodeLocator"/> and  <see cref="NodeLocatorFactory"/> are assigned then the latter takes precedence.</remarks>
		public IProviderFactory<IMemcachedNodeLocator> NodeLocatorFactory { get; set; }

		/// <summary>
		/// Gets or sets the <see cref="Enyim.Caching.Memcached.ITranscoder"/> which will be used serialize or deserialize items.
		/// </summary>
		public ITranscoder Transcoder
		{
			get
			{
				return this._transcoder ?? (this._transcoder = new DefaultTranscoder());
			}
			set
			{
				this._transcoder = value;
			}
		}

		/// <summary>
		/// Gets or sets the type of the communication between client and server.
		/// </summary>
		public MemcachedProtocol Protocol { get; set; }

		IList<EndPoint> IMemcachedClientConfiguration.Servers
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
			var factory = this.NodeLocatorFactory;
			if (factory != null)
				return factory.Create();

			return this.NodeLocator == null
				? new DefaultNodeLocator()
				: FastActivator.Create(this.NodeLocator) as IMemcachedNodeLocator;
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
					return new DefaultServerPool(this, new TextOperationFactory());

				case MemcachedProtocol.Binary:
					return new BinaryPool(this);
			}
			throw new ArgumentOutOfRangeException($"Unknown protocol: [{this.Protocol}]");
		}

	}

	#region Configuration helpers
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

	public class MemcachedClientOptions : IOptions<MemcachedClientOptions>
	{
		public MemcachedProtocol Protocol { get; set; } = MemcachedProtocol.Binary;

		public SocketPoolConfiguration SocketPool { get; set; } = new SocketPoolConfiguration();

		public List<MemcachedServer> Servers { get; set; } = new List<MemcachedServer>();

		public AuthenticationConfiguration Authentication { get; set; } = new AuthenticationConfiguration();

		public string KeyTransformer { get; set; }

		public ITranscoder Transcoder { get; set; }

		public IProviderFactory<IMemcachedNodeLocator> NodeLocatorFactory { get; set; }

		public MemcachedClientOptions Value => this;
	}

	public class MemcachedServer
	{
		public string Address { get; set; }

		public int Port { get; set; }
	}
	#endregion

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
