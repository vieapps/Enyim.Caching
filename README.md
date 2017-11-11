# VIEApps.Enyim.Caching
The .NET Standard 2.0 memcached client library: 
- 100% compatible with EnyimMemcached 2.x library, fully async
- Object serialization with various transcoders: BinaryFormatter, Protocol Buffers, Json.NET Bson, MessagePack
- Ready with .NET Core 2.0 and .NET Framework 4.6.1 (and higher) with more useful methods (Add, Replace, Exists)
### Nuget
- Package ID: VIEApps.Enyim.Caching
- Details: https://www.nuget.org/packages/VIEApps.Enyim.Caching
### Information
- Migrated from the fork [EnyimMemcachedCore](https://github.com/cnblogs/EnyimMemcachedCore) (.NET Core 2.0)
- Reference from the original [EnyimMemcached](https://github.com/enyim/EnyimMemcached) (.NET Framework 3.5)
### Usage (ASP.NET Core)
- Add services.AddEnyimMemcached(...) and app.UseEnyimMemcached() in Startup
- Add IMemcachedClient into constructor
## Configure with the appsettings.json file
### Without authentication
```json
{
	"EnyimMemcached": {
		"Servers": [
			{
				"Address": "127.0.0.1",
				"Port": 11211
			}
		],
	}
}
```
### With authentication
```json
{
  "EnyimMemcached": {
		"Servers": [
		  {
				"Address": "127.0.0.1",
				"Port": 11211
		  }
		],
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
		services.AddEnyimMemcached(options => Configuration.GetSection("EnyimMemcached").Bind(options));
	}
	
	public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
	{ 
		app.UseEnyimMemcached();
	}
}
```

## Example usage (.NET Core)
### Use IMemcachedClient interface
```cs
public class TabNavService
{
	private ITabNavRepository _tabNavRepository;
	private IMemcachedClient _memcachedClient;

	public TabNavService(
		ITabNavRepository tabNavRepository,
		IMemcachedClient memcachedClient)
	{
		_tabNavRepository = tabNavRepository;
		_memcachedClient = memcachedClient;
	}

	public async Task<IEnumerable<TabNav>> GetAll()
	{
		var cacheKey = "aboutus_tabnavs_all";
		var result = await _memcachedClient.GetAsync<IEnumerable<TabNav>>(cacheKey);
		if (!result.Success)
		{
			var tabNavs = await _tabNavRepository.GetAll();
			await _memcachedClient.AddAsync(cacheKey, tabNavs, 300);
			return tabNavs;
		}
		else
		{
			return result.Value;
		}
	}
}
```
### Use IDistributedCache interface
```cs
public class CreativeService
{
	private ICreativeRepository _creativeRepository;
	private IDistributedCache _cache;

	public CreativeService(ICreativeRepository creativeRepository, IDistributedCache cache)
	{
		_creativeRepository = creativeRepository;
		_cache = cache;
	}

	public async Task<IList<CreativeDTO>> GetCreatives(string unitName)
	{
		var cacheKey = $"creatives_{unitName}";
		IList<CreativeDTO> creatives = null;

		var creativesJson = await _cache.GetStringAsync(cacheKey);
		if (creativesJson == null)
		{
			creatives = await _creativeRepository.GetCreatives(unitName).ProjectTo<CreativeDTO>().ToListAsync();
			var json = string.Empty;
			if (creatives != null && creatives.Count() > 0)
				json = JsonConvert.SerializeObject(creatives);
			await _cache.SetStringAsync(cacheKey, json, new DistributedCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromMinutes(5)));
		}
		else
		{
			creatives = JsonConvert.DeserializeObject<List<CreativeDTO>>(creativesJson);
		}

		return creatives;
	}
}
```
## Configure with the app.config/web.config file
### Without authentication
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
### With authentication
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
## Example usage (.NET Core 2.0/.NET Framework 4.6.1) with the .config file
```cs
public class CreativeService
{
	private MemcachedClient _memcachedClient;

	public CreativeService()
	{
		_memcachedClient = new MemcachedClient(ConfigurationManager.GetSection("memcached") as MemcachedClientConfigurationSectionHandler);
	}

	public async Task<IList<CreativeDTO>> GetCreatives(string unitName)
	{
		return await _memcachedClient.GetAsync<IList<CreativeDTO>>($"creatives_{unitName}");
	}
}
```
## Other transcoders (Protocol Buffers, Json.NET Bson, MessagePack)
See [VIEApps.Enyim.Caching.Transcoders](https://github.com/vieapps/Enyim.Caching.Transcoders)
