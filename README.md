# VIEApps.Enyim.Caching
The .NET Standard 2.0 memcached client library: 
- 100% compatible with EnyimMemcached 2.x library, fully async
- Objects are serializing with various transcoders: BinaryFormatter, Protocol Buffers, Json.NET Bson, MessagePack
- Ready with .NET Core 2.0 and .NET Framework 4.6.1 (and higher) with more useful methods (Add, Replace, Exists)
### NuGet
- Package ID: VIEApps.Enyim.Caching
- Details: https://www.nuget.org/packages/VIEApps.Enyim.Caching
### Information
- Migrated from the fork [EnyimMemcachedCore](https://github.com/cnblogs/EnyimMemcachedCore) (.NET Core 2.0)
- Reference from the original [EnyimMemcached](https://github.com/enyim/EnyimMemcached) (.NET Framework 3.5)
## Usage of ASP.NET Core 2.0 apps
- Add services.AddMemcached(...) and app.UseMemcached() in Startup.cs
- Add IMemcachedClient or IDistributedCache into constructor (using dependency injection)
### Configure (by the appsettings.json file) without authentication
```json
{
	"Memcached": {
		"Servers": [
			{
				"Address": "127.0.0.1",
				"Port": 11211
			}
		],
		"SocketPool": {
			"MinPoolSize": 10,
			"MaxPoolSize": 100,
			"DeadTimeout": "00:01:00",
			"ConnectionTimeout": "00:00:05",
			"ReceiveTimeout": "00:00:01"
		}
	}
}
```
### Configure (by the appsettings.json file) with authentication
```json
{
	"Memcached": {
		"Servers": [
			{
				"Address": "127.0.0.1",
				"Port": 11211
			}
		],
		"SocketPool": {
			"MinPoolSize": 10,
			"MaxPoolSize": 100,
			"DeadTimeout": "00:01:00",
			"ConnectionTimeout": "00:00:05",
			"ReceiveTimeout": "00:00:01"
		},
		"Authentication": {
			"Type": "Enyim.Caching.Memcached.PlainTextAuthenticator, Enyim.Caching",
			"Parameters": {
				"zone": "",
				"userName": "username",
				"password": "password"
			}
		}
	}
}
```
### Startup.cs
```cs
public class Startup
{
	public void ConfigureServices(IServiceCollection services)
	{
		// ....
		services.AddMemcached(options => Configuration.GetSection("Memcached").Bind(options));
	}
	
	public void Configure(IApplicationBuilder appBuilder, IHostingEnvironment env, ILoggerFactory loggerFactory)
	{ 
		// ....
		appBuilder.UseMemcached();
	}
}
```
### Use IMemcachedClient interface
```cs
public class TabNavService
{
	ITabNavRepository _tabNavRepository;
	IMemcachedClient _cache;

	public TabNavService(ITabNavRepository tabNavRepository, IMemcachedClient memcachedClient)
	{
		_tabNavRepository = tabNavRepository;
		_cache = memcachedClient;
	}

	public async Task<IEnumerable<TabNav>> GetAll()
	{
		var cacheKey = "aboutus_tabnavs_all";
		var result = await _cache.GetAsync<IEnumerable<TabNav>>(cacheKey);
		if (result == null)
		{
			var tabNavs = await _tabNavRepository.GetAll();
			await _cache.SetAsync(cacheKey, tabNavs, TimeSpan.FromMinutes(30));
			return tabNavs;
		}
		else
			return result;
	}
}
```
### Use IDistributedCache interface
```cs
public class CreativeService
{
	ICreativeRepository _creativeRepository;
	IDistributedCache _cache;

	public CreativeService(ICreativeRepository creativeRepository, IDistributedCache cache)
	{
		_creativeRepository = creativeRepository;
		_cache = cache;
	}

	public async Task<IList<CreativeDTO>> GetCreatives(string unitName)
	{
		var cacheKey = $"creatives_{unitName}";
		IList<CreativeDTO> creatives = null;

		var creativesBytes = await _cache.GetAsync(cacheKey);
		var creativesJson = creativesBytes != null ? System.Text.Encoding.UTF8.GetString(creativesBytes) : null;
		if (creativesJson == null)
		{
			creatives = await _creativeRepository.GetCreatives(unitName).ProjectTo<CreativeDTO>().ToListAsync();
			var json = string.Empty;
			if (creatives != null && creatives.Count() > 0)
				json = JsonConvert.SerializeObject(creatives);
			creativesBytes = System.Text.Encoding.UTF8.GetBytes(json);
			await _cache.SetAsync(cacheKey, creativesBytes, new DistributedCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromMinutes(30)));
		}
		else
			creatives = JsonConvert.DeserializeObject<List<CreativeDTO>>(creativesJson);

		return creatives;
	}
}
```
## Usage of .NET Core 2.0/.NET Framework 4.6.1 (and higher) stand-alone apps
### Configure (by the app.config/web.config) without authentication
```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
	<configSections>
		<section name="memcached" type="Enyim.Caching.Configuration.MemcachedClientConfigurationSectionHandler, Enyim.Caching" />
	</configSections>
	<memcached>
		<servers>
			<add address="127.0.0.1" port="11211" />
		</servers>
		<socketPool minPoolSize="10" maxPoolSize="100" deadTimeout="00:01:00" connectionTimeout="00:00:05" receiveTimeout="00:00:01" />
	</memcached>
</configuration>
```
### Configure (by the app.config/web.config) with authentication
```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
	<configSections>
		<section name="memcached" type="Enyim.Caching.Configuration.MemcachedClientConfigurationSectionHandler, Enyim.Caching" />
	</configSections>
	<memcached>
		<servers>
			<add address="127.0.0.1" port="11211" />
		</servers>
		<socketPool minPoolSize="10" maxPoolSize="100" deadTimeout="00:01:00" connectionTimeout="00:00:05" receiveTimeout="00:00:01" />
		<authentication type="Enyim.Caching.Memcached.PlainTextAuthenticator, Enyim.Caching" zone="" userName="username" password="password" />
	</memcached>
</configuration>
```
### Example
```cs
public class CreativeService
{
	MemcachedClient _cache;

	public CreativeService()
	{
		_cache = new MemcachedClient(ConfigurationManager.GetSection("memcached") as MemcachedClientConfigurationSectionHandler);
	}

	public async Task<IList<CreativeDTO>> GetCreatives(string unitName)
	{
		return await _cache.GetAsync<IList<CreativeDTO>>($"creatives_{unitName}");
	}
}
```
## Other transcoders (Protocol Buffers, Json.NET Bson, MessagePack)
See [VIEApps.Enyim.Caching.Transcoders](https://github.com/vieapps/Enyim.Caching.Transcoders)
