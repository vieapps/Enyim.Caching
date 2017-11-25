using System;

using Microsoft.Extensions.Logging;

namespace Enyim.Caching
{
	public static class LogManager
	{
		static ILoggerFactory LoggerFactory;

		public static void AssignLoggerFactory(ILoggerFactory loggerFactory)
		{
			if (LogManager.LoggerFactory == null && loggerFactory != null)
				LogManager.LoggerFactory = loggerFactory;
		}

		public static ILogger CreateLogger(Type type)
		{
			return (LogManager.LoggerFactory ?? new NullLoggerFactory()).CreateLogger(type);
		}

		public static ILogger CreateLogger<T>()
		{
			return LogManager.CreateLogger(typeof(T));
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
