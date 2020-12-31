#region Related components
using System;
using System.Net;
using System.Xml;
using System.Configuration;
using System.Collections.Generic;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Protocol.Text;
using Enyim.Caching.Memcached.Protocol.Binary;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
#endregion

namespace Enyim.Caching.Configuration
{
	public class MemcachedClientConfiguration : IMemcachedClientConfiguration
	{
		Type _nodeLocator;
		ITranscoder _transcoder;
		IKeyTransformer _keyTransformer;
		ILogger _logger;

		/// <summary>
		/// Initializes a new instance of the <see cref="MemcachedClientConfiguration"/> class.
		/// </summary>
		/// <param name="loggerFactory"></param>
		public MemcachedClientConfiguration(ILoggerFactory loggerFactory = null)
		{
			this.PrepareLogger(loggerFactory);
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
				throw new ArgumentNullException(nameof(options), "No configuration is found");

			this.PrepareLogger(loggerFactory);

			var configuration = options.Value;

			this.Protocol = configuration.Protocol;

			configuration.Servers.ForEach(server =>
			{
				var address = server.Address;
				if ((address.IndexOf(".") > 0 && address.IndexOf(":") > 0) || (address.IndexOf(":") > 0 && address.IndexOf("]:") > 0))
					this.AddServer(address);
				else
					this.AddServer(address, server.Port);
			});

			this.SocketPool = configuration.SocketPool;

			if (!string.IsNullOrWhiteSpace(configuration.Authentication?.Type))
			{
				try
				{
					this.Authentication = new AuthenticationConfiguration
					{
						Type = configuration.Authentication.Type
					};
					foreach (var parameter in configuration.Authentication.Parameters)
					{
						this.Authentication.Parameters[parameter.Key] = parameter.Value;
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Authentication {parameter.Key} is '{parameter.Value}'.");
					}
					if (this._logger.IsEnabled(LogLevel.Debug))
						this._logger.LogDebug($"Use '{configuration.Authentication.Type}' authentication");
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, $"Unable to load '{configuration.Authentication.Type}' authentication");
				}
			}

			if (!string.IsNullOrWhiteSpace(configuration.KeyTransformer))
			{
				try
				{
					this._keyTransformer = FastActivator.Create(configuration.KeyTransformer) as IKeyTransformer;
					if (this._logger.IsEnabled(LogLevel.Debug))
						this._logger.LogDebug($"Use '{configuration.KeyTransformer}' key-transformer");
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, $"Unable to load '{configuration.KeyTransformer}' key-transformer");
				}
			}

			if (!string.IsNullOrWhiteSpace(configuration.Transcoder))
				try
				{
					this._transcoder = FastActivator.Create(configuration.Transcoder) as ITranscoder;
					if (this._logger.IsEnabled(LogLevel.Debug))
						this._logger.LogDebug($"Use '{configuration.Transcoder}' transcoder");
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, $"Unable to load '{configuration.Transcoder}' transcoder");
				}

			if (!string.IsNullOrWhiteSpace(configuration.NodeLocator))
				try
				{
					this.NodeLocator = Type.GetType(configuration.NodeLocator);
					if (this._logger.IsEnabled(LogLevel.Debug))
						this._logger.LogDebug($"Use '{configuration.NodeLocator}' node-locator");
				}
				catch (Exception ex)
				{
					this._logger.LogError(ex, $"Unable to load '{configuration.NodeLocator}' node-locator");
				}
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MemcachedClientConfiguration"/>.
		/// </summary>
		/// <param name="loggerFactory"></param>
		/// <param name="configuration"></param>
		public MemcachedClientConfiguration(ILoggerFactory loggerFactory, MemcachedClientConfigurationSectionHandler configuration)
		{
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration), "No configuration is found");

			this.PrepareLogger(loggerFactory);

			if (Enum.TryParse(configuration.Section.Attributes["protocol"]?.Value ?? "Binary", out MemcachedProtocol protocol))
				this.Protocol = protocol;

			if (configuration.Section.SelectNodes("servers/add") is XmlNodeList servers)
				foreach (XmlNode server in servers)
					if ("memcached".Equals((server.Attributes["type"]?.Value ?? "Memcached").Trim().ToLower()))
					{
						var address = server.Attributes["address"]?.Value ?? "localhost";
						if ((address.IndexOf(".") > 0 && address.IndexOf(":") > 0) || (address.IndexOf(":") > 0 && address.IndexOf("]:") > 0))
							this.AddServer(address);
						else
							this.AddServer(address, Int32.TryParse(server.Attributes["port"]?.Value ?? "11211", out int port) ? port : 11211);
					}

			this.SocketPool = new SocketPoolConfiguration();
			if (configuration.Section.SelectSingleNode("socketPool") is XmlNode socketpool)
			{
				if (Int32.TryParse(socketpool.Attributes["maxPoolSize"]?.Value, out int intValue))
					this.SocketPool.MaxPoolSize = intValue;
				if (Int32.TryParse(socketpool.Attributes["minPoolSize"]?.Value, out intValue))
					this.SocketPool.MinPoolSize = intValue;
				if (TimeSpan.TryParse(socketpool.Attributes["connectionTimeout"]?.Value, out TimeSpan timespanValue))
					this.SocketPool.ConnectionTimeout = timespanValue;
				if (TimeSpan.TryParse(socketpool.Attributes["deadTimeout"]?.Value, out timespanValue))
					this.SocketPool.DeadTimeout = timespanValue;
				if (TimeSpan.TryParse(socketpool.Attributes["queueTimeout"]?.Value, out timespanValue))
					this.SocketPool.QueueTimeout = timespanValue;
				if (TimeSpan.TryParse(socketpool.Attributes["receiveTimeout"]?.Value, out timespanValue))
					this.SocketPool.ReceiveTimeout = timespanValue;
				if (Boolean.TryParse(socketpool.Attributes["noDelay"]?.Value, out bool boolValue))
					this.SocketPool.NoDelay = boolValue;

				if ("throttling" == socketpool.Attributes["failurePolicy"]?.Value)
					this.SocketPool.FailurePolicyFactory = new ThrottlingFailurePolicyFactory(Int32.TryParse(socketpool.Attributes["failureThreshold"]?.Value, out intValue) ? intValue : 4, TimeSpan.TryParse(socketpool.Attributes["resetAfter"]?.Value, out timespanValue) ? timespanValue : TimeSpan.FromSeconds(5));
			}

			if (configuration.Section.SelectSingleNode("authentication") is XmlNode authentication)
				if (authentication.Attributes["type"]?.Value != null)
					try
					{
						this.Authentication = new AuthenticationConfiguration
						{
							Type = authentication.Attributes["type"].Value
						};
						if (authentication.Attributes["zone"]?.Value != null)
							this.Authentication.Parameters.Add("zone", authentication.Attributes["zone"].Value);
						if (authentication.Attributes["userName"]?.Value != null)
							this.Authentication.Parameters.Add("userName", authentication.Attributes["userName"].Value);
						if (authentication.Attributes["password"]?.Value != null)
							this.Authentication.Parameters.Add("password", authentication.Attributes["password"].Value);
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Use '{authentication.Attributes["type"].Value}' authentication");
					}
					catch (Exception ex)
					{
						this._logger.LogError(ex, $"Unable to load '{authentication.Attributes["type"].Value}' authentication");
					}

			if (configuration.Section.SelectSingleNode("keyTransformer") is XmlNode keyTransformer)
				if (keyTransformer.Attributes["type"]?.Value != null)
					try
					{
						this._keyTransformer = FastActivator.Create(keyTransformer.Attributes["type"].Value) as IKeyTransformer;
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Use '{keyTransformer.Attributes["type"].Value}' key-transformer");
					}
					catch (Exception ex)
					{
						this._logger.LogError(ex, $"Unable to load '{keyTransformer.Attributes["type"].Value}' key-transformer");
					}

			if (configuration.Section.SelectSingleNode("transcoder") is XmlNode transcoder)
				if (transcoder.Attributes["type"]?.Value != null)
					try
					{
						this._transcoder = FastActivator.Create(transcoder.Attributes["type"].Value) as ITranscoder;
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Use '{transcoder.Attributes["type"].Value}' transcoder");
					}
					catch (Exception ex)
					{
						this._logger.LogError(ex, $"Unable to load '{transcoder.Attributes["type"].Value}' transcoder");
					}

			if (configuration.Section.SelectSingleNode("nodeLocator") is XmlNode nodeLocator)
				if (nodeLocator.Attributes["type"]?.Value != null)
					try
					{
						this.NodeLocator = Type.GetType(nodeLocator.Attributes["type"].Value);
						if (this._logger.IsEnabled(LogLevel.Debug))
							this._logger.LogDebug($"Use '{nodeLocator.Attributes["type"].Value}' node-locator");
					}
					catch (Exception ex)
					{
						this._logger.LogError(ex, $"Unable to load '{nodeLocator.Attributes["type"].Value}' node-locator");
					}
		}

		void PrepareLogger(ILoggerFactory loggerFactory)
		{
			Logger.AssignLoggerFactory(loggerFactory);
			this._logger = Logger.CreateLogger<MemcachedClientConfiguration>();
		}

		/// <summary>
		/// Adds a new server to the pool.
		/// </summary>
		/// <param name="address">The address and the port of the server in the format 'host:port'.</param>
		public void AddServer(string address)
			=> this.Servers.Add(ConfigurationHelper.ResolveToEndPoint(address));

		/// <summary>
		/// Adds a new server to the pool.
		/// </summary>
		/// <param name="address">The host name or IP address of the server.</param>
		/// <param name="port">The port number of the memcached instance.</param>
		public void AddServer(string address, int port)
			=> this.Servers.Add(ConfigurationHelper.ResolveToEndPoint(address, port));

		/// <summary>
		/// Gets a list of <see cref="IPEndPoint"/> each representing a Memcached server in the pool.
		/// </summary>
		public IList<EndPoint> Servers { get; } = new List<EndPoint>();

		/// <summary>
		/// Gets or sets the type of the communication between client and server.
		/// </summary>
		public MemcachedProtocol Protocol { get; set; } = MemcachedProtocol.Binary;

		/// <summary>
		/// Gets the configuration of the socket pool.
		/// </summary>
		public ISocketPoolConfiguration SocketPool { get; internal set; }

		/// <summary>
		/// Gets the authentication settings.
		/// </summary>
		public IAuthenticationConfiguration Authentication { get; internal set; }

		/// <summary>
		/// Gets or sets the <see cref="IKeyTransformer"/> which will be used to convert item keys for Memcached.
		/// </summary>
		public IKeyTransformer KeyTransformer
		{
			get => this._keyTransformer ?? (this._keyTransformer = new DefaultKeyTransformer());
			set => this._keyTransformer = value;
		}

		/// <summary>
		/// Gets or sets the <see cref="ITranscoder"/> which will be used serialize or deserialize items.
		/// </summary>
		public ITranscoder Transcoder
		{
			get => this._transcoder ?? (this._transcoder = new DefaultTranscoder());
			set => this._transcoder = value;
		}

		/// <summary>
		/// Gets or sets the Type of the <see cref="INodeLocator"/> which will be used to assign items to Memcached nodes.
		/// </summary>
		/// <remarks>If both <see cref="NodeLocator"/> and  <see cref="NodeLocatorFactory"/> are assigned then the latter takes precedence.</remarks>
		public Type NodeLocator
		{
			get => this._nodeLocator;
			set
			{
				ConfigurationHelper.CheckForInterface(value, typeof(INodeLocator));
				this._nodeLocator = value;
			}
		}

		/// <summary>
		/// Gets or sets the NodeLocatorFactory instance which will be used to create a new IMemcachedNodeLocator instances.
		/// </summary>
		/// <remarks>If both <see cref="NodeLocator"/> and  <see cref="NodeLocatorFactory"/> are assigned then the latter takes precedence.</remarks>
		public IProviderFactory<INodeLocator> NodeLocatorFactory { get; set; }

		IList<EndPoint> IMemcachedClientConfiguration.Servers
			=> this.Servers;

		ISocketPoolConfiguration IMemcachedClientConfiguration.SocketPool
			=> this.SocketPool;

		IAuthenticationConfiguration IMemcachedClientConfiguration.Authentication
			=> this.Authentication;

		IKeyTransformer IMemcachedClientConfiguration.CreateKeyTransformer()
			=> this.KeyTransformer;

		ITranscoder IMemcachedClientConfiguration.CreateTranscoder()
			=> this.Transcoder;

		INodeLocator IMemcachedClientConfiguration.CreateNodeLocator()
			=> this.NodeLocatorFactory != null
				? this.NodeLocatorFactory.Create()
				: this.NodeLocator != null
					? FastActivator.Create(this.NodeLocator) as INodeLocator ?? new DefaultNodeLocator() as INodeLocator
					: this.Servers.Count > 1 ? new KetamaNodeLocator() as INodeLocator : new SingleNodeLocator() as INodeLocator;

		IServerPool IMemcachedClientConfiguration.CreatePool()
			=> this.Protocol.Equals(MemcachedProtocol.Text)
				? new DefaultServerPool(this, new TextOperationFactory())
				: new BinaryPool(this);
	}

	public class MemcachedClientConfigurationSectionHandler : IConfigurationSectionHandler
	{
		public object Create(object parent, object configContext, XmlNode section)
		{
			this.Section = section;
			return this;
		}

		public XmlNode Section { get; private set; } = null;
	}

	public class MemcachedClientOptions : IOptions<MemcachedClientOptions>
	{
		public MemcachedProtocol Protocol { get; set; } = MemcachedProtocol.Binary;

		public SocketPoolConfiguration SocketPool { get; set; } = new SocketPoolConfiguration();

		public List<MemcachedServer> Servers { get; set; } = new List<MemcachedServer>();

		public AuthenticationConfiguration Authentication { get; set; } = new AuthenticationConfiguration();

		public string KeyTransformer { get; set; }

		public string Transcoder { get; set; }

		public string NodeLocator { get; set; }

		public MemcachedClientOptions Value => this;
	}

	public class MemcachedServer
	{
		public string Address { get; set; }

		public int Port { get; set; }
	}

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
