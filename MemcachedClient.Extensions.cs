using System;

using Enyim.Caching;
using Enyim.Caching.Configuration;

using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Microsoft.Extensions.DependencyInjection
{
	public static class EnyimMemcachedServiceCollectionExtensions
	{
		public static IServiceCollection AddEnyimMemcached(this IServiceCollection services, Action<MemcachedClientOptions> setupAction)
		{
			if (services == null)
			{
				throw new ArgumentNullException(nameof(services));
			}

			if (setupAction == null)
			{
				throw new ArgumentNullException(nameof(setupAction));
			}

			return AddEnyimMemcached(services, s => s.Configure(setupAction));
		}

		public static IServiceCollection AddEnyimMemcached(this IServiceCollection services, IConfiguration configuration)
		{
			if (services == null)
			{
				throw new ArgumentNullException(nameof(services));
			}

			if (configuration == null)
			{
				throw new ArgumentNullException(nameof(configuration));
			}

			return AddEnyimMemcached(services, s => s.Configure<MemcachedClientOptions>(configuration));
		}

		private static IServiceCollection AddEnyimMemcached(IServiceCollection services, Action<IServiceCollection> configure)
		{
			services.AddOptions();
			configure(services);
			services.Add(ServiceDescriptor.Transient<IMemcachedClientConfiguration, MemcachedClientConfiguration>());
			services.Add(ServiceDescriptor.Singleton<IMemcachedClient, MemcachedClient>());
			return services;
		}

		public static IServiceCollection AddDistributedEnyimMemcached(this IServiceCollection services, Action<MemcachedClientOptions> setupAction)
		{
			if (services == null)
			{
				throw new ArgumentNullException(nameof(services));
			}

			if (setupAction == null)
			{
				throw new ArgumentNullException(nameof(setupAction));
			}

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
	public static class EnyimMemcachedApplicationBuilderExtensions
	{
		public static IApplicationBuilder UseEnyimMemcached(this IApplicationBuilder app)
		{
			try
			{
				app.ApplicationServices.GetService<IMemcachedClient>().DoGetAsync<string>("EnyimMemcached").Wait();
				Console.WriteLine("EnyimMemcached Started.");
			}
			catch (Exception ex)
			{
				app.ApplicationServices.GetService<ILogger<IMemcachedClient>>().LogError(new EventId(), ex, "EnyimMemcached Failed.");
			}
			return app;
		}
	}
}
