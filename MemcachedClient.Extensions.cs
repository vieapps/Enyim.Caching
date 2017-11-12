using System;

using Enyim.Caching;
using Enyim.Caching.Configuration;

using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Microsoft.Extensions.DependencyInjection
{
	public static class ServiceCollectionExtensions
	{

		#region Opts
		/*
		static IServiceCollection AddMemcached(IServiceCollection services, Action<IServiceCollection> setupAction)
		{
			if (services == null)
				throw new ArgumentNullException(nameof(services));

			if (setupAction == null)
				throw new ArgumentNullException(nameof(setupAction));

			services.AddOptions();
			setupAction(services);
			services.Add(ServiceDescriptor.Transient<IMemcachedClientConfiguration, MemcachedClientConfiguration>());
			services.Add(ServiceDescriptor.Singleton<IMemcachedClient, MemcachedClient>());
			return services;
		}

		public static IServiceCollection AddMemcached(this IServiceCollection services, IConfiguration configuration)
		{
			if (configuration == null)
				throw new ArgumentNullException(nameof(configuration));
			return ServiceCollectionExtensions.AddMemcached(services, svc => svc.Configure<MemcachedClientOptions>(configuration));
		}

		public static IServiceCollection AddMemcached(this IServiceCollection services, Action<MemcachedClientOptions> setupAction)
		{
			return ServiceCollectionExtensions.AddMemcached(services, svc => svc.Configure(setupAction));
		}
		*/
		#endregion

		/// <summary>
		/// Adds the memcached as stand-alone caching storage
		/// </summary>
		/// <param name="services"></param>
		/// <param name="setupAction"></param>
		/// <returns></returns>
		public static IServiceCollection AddMemcached(this IServiceCollection services, Action<MemcachedClientOptions> setupAction)
		{
			if (services == null)
				throw new ArgumentNullException(nameof(services));

			if (setupAction == null)
				throw new ArgumentNullException(nameof(setupAction));

			services.AddOptions();
			services.Configure(setupAction);
			services.Add(ServiceDescriptor.Transient<IMemcachedClientConfiguration, MemcachedClientConfiguration>());
			services.Add(ServiceDescriptor.Singleton<IMemcachedClient, MemcachedClient>());

			return services;
		}

		/// <summary>
		/// Adds the memcached as an instance of IDistributedCache
		/// </summary>
		/// <param name="services"></param>
		/// <param name="setupAction"></param>
		/// <returns></returns>
		public static IServiceCollection AddMemcachedAsIDistributedCache(this IServiceCollection services, Action<MemcachedClientOptions> setupAction)
		{
			if (services == null)
				throw new ArgumentNullException(nameof(services));

			if (setupAction == null)
				throw new ArgumentNullException(nameof(setupAction));

			services.AddOptions();
			services.Configure(setupAction);
			services.Add(ServiceDescriptor.Transient<IMemcachedClientConfiguration, MemcachedClientConfiguration>());
			services.Add(ServiceDescriptor.Singleton<IDistributedCache, MemcachedClient>());

			return services;
		}
	}
}

namespace Microsoft.AspNetCore.Builder
{
	public static class ApplicationBuilderExtensions
	{
		public static IApplicationBuilder UseMemcached(this IApplicationBuilder appBuilder)
		{
			try
			{
				appBuilder.ApplicationServices.GetService<IMemcachedClient>().DoGetAsync<string>("Memcached").Wait();
				appBuilder.ApplicationServices.GetService<ILogger<IMemcachedClient>>().LogInformation(new EventId(), null, "Memcached is started");
			}
			catch (Exception ex)
			{
				appBuilder.ApplicationServices.GetService<ILogger<IMemcachedClient>>().LogError(new EventId(), ex, "Memcached is failed");
			}
			return appBuilder;
		}
	}
}