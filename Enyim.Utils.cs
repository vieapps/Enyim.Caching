#region Related components
using System;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Reflection;
using System.Runtime.Loader;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyModel;
using Microsoft.Extensions.DependencyModel.Resolution;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
#endregion

namespace Enyim.Collections
{

	#region Queue
	/// <summary>
	/// Implements a non-locking queue.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class InterlockedQueue<T>
	{
		Node headNode;
		Node tailNode;

		public InterlockedQueue()
		{
			Node node = new Node(default);
			this.headNode = node;
			this.tailNode = node;
		}

		public bool Dequeue(out T value)
		{
			Node head;
			Node tail;
			Node next;

			while (true)
			{
				// read head
				head = this.headNode;
				tail = this.tailNode;
				next = head.Next;

				// are head, tail, and next consistent?
				if (object.ReferenceEquals(this.headNode, head))
				{
					// is tail falling behind
					if (object.ReferenceEquals(head, tail))
					{
						// is the queue empty?
						if (next == null)
						{
							value = default;
							return false; // queue is empty and cannot dequeue
						}
						Interlocked.CompareExchange<Node>(ref this.tailNode, next, tail);
					}

					// no need to deal with tail
					else
					{
						// read value before CAS otherwise another deque might try to free the next node
						value = next.Value;

						// try to swing the head to the next node
						if (Interlocked.CompareExchange(ref this.headNode, next, head) == head)
							return true;
					}
				}
			}
		}

		public bool Peek(out T value)
		{
			Node head, tail, next;
			while (true)
			{
				// read head
				head = this.headNode;
				tail = this.tailNode;
				next = head.Next;

				// Are head, tail, and next consistent?
				if (object.ReferenceEquals(this.headNode, head))
				{
					// is tail falling behind
					if (object.ReferenceEquals(head, tail))
					{
						// is the queue empty?
						if (next is null)
						{
							value = default;

							// queue is empty
							return false;
						}

						Interlocked.CompareExchange<Node>(ref this.tailNode, next, tail);
					}
					// No need to deal with tail
					else
					{
						// read value before CAS otherwise another deque might try to free the next node
						value = next.Value;
						return true;
					}
				}
			}
		}

		public void Enqueue(T value)
		{
			// Allocate a new node from the free list
			var valueNode = new Node(value);
			while (true)
			{
				var tail = this.tailNode;
				var next = tail.Next;

				// are tail and next consistent
				if (object.ReferenceEquals(tail, this.tailNode))
				{
					// was tail pointing to the last node?
					if (next is null)
					{
						if (object.ReferenceEquals(Interlocked.CompareExchange(ref tail.Next, valueNode, next), next))
						{
							Interlocked.CompareExchange(ref this.tailNode, valueNode, tail);
							break;
						}
					}

					// tail was not pointing to last node
					else
					{
						// try to swing Tail to the next node
						Interlocked.CompareExchange<Node>(ref this.tailNode, next, tail);
					}
				}
			}
		}

		class Node
		{
			public readonly T Value;
			public Node Next;

			public Node(T value)
			{
				this.Value = value;
			}
		}
	}
	#endregion

	#region Stack
	/// <summary>
	/// Implements a non-locking stack.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class InterlockedStack<T>
	{
		readonly Node head;

		public InterlockedStack()
			=> this.head = new Node(default);

		public void Push(T item)
		{
			var node = new Node(item);
			do
			{
				node.Next = this.head.Next;
			}
			while (Interlocked.CompareExchange(ref this.head.Next, node, node.Next) != node.Next);
		}

		public bool TryPop(out T value)
		{
			value = default;
			Node node;

			do
			{
				node = head.Next;
				if (node == null)
					return false;
			}
			while (Interlocked.CompareExchange(ref head.Next, node.Next, node) != node);

			value = node.Value;
			return true;
		}

		class Node
		{
			public readonly T Value;
			public Node Next;

			public Node(T value)
			{
				this.Value = value;
			}
		}
	}
	#endregion

}

namespace Enyim.Caching
{

	#region Logger
	public static class Logger
	{
		static ILoggerFactory LoggerFactory;

		/// <summary>
		/// Assigns a logger factory
		/// </summary>
		/// <param name="loggerFactory"></param>
		public static void AssignLoggerFactory(ILoggerFactory loggerFactory)
		{
			if (Logger.LoggerFactory == null && loggerFactory != null)
				Logger.LoggerFactory = loggerFactory;
		}

		/// <summary>
		/// Gets a logger factory
		/// </summary>
		/// <returns></returns>
		public static ILoggerFactory GetLoggerFactory()
			=> Logger.LoggerFactory ?? new NullLoggerFactory();

		/// <summary>
		/// Creates a logger
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static ILogger CreateLogger(Type type)
			=> Logger.GetLoggerFactory().CreateLogger(type);

		/// <summary>
		/// Creates a logger
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public static ILogger CreateLogger<T>()
			=> Logger.CreateLogger(typeof(T));

		/// <summary>
		/// Writes a log message
		/// </summary>
		/// <param name="logger"></param>
		/// <param name="mode">Write mode</param>
		/// <param name="message">The log message</param>
		/// <param name="exception">The exception</param>
		public static void Log(this ILogger logger, LogLevel mode, string message, Exception exception = null)
		{
			switch (mode)
			{
				case LogLevel.Trace:
					if (exception != null)
						logger.LogTrace(exception, message);
					else
						logger.LogTrace(message);
					break;

				case LogLevel.Information:
					if (exception != null)
						logger.LogInformation(exception, message);
					else
						logger.LogInformation(message);
					break;

				case LogLevel.Warning:
					if (exception != null)
						logger.LogWarning(exception, message);
					else
						logger.LogWarning(message);
					break;

				case LogLevel.Error:
					if (exception != null)
						logger.LogError(exception, message);
					else
						logger.LogError(message);
					break;

				case LogLevel.Critical:
					if (exception != null)
						logger.LogCritical(exception, message);
					else
						logger.LogCritical(message);
					break;

				default:
					if (exception != null)
						logger.LogDebug(exception, message);
					else
						logger.LogDebug(message);
					break;
			}
		}

		/// <summary>
		/// Writes a log message
		/// </summary>
		/// <param name="logger"></param>
		/// <param name="minLevel">The minimum level (for checking when write)</param>
		/// <param name="mode">Write mode</param>
		/// <param name="message">The log message</param>
		/// <param name="exception">The exception</param>
		public static void Log(this ILogger logger, LogLevel minLevel, LogLevel mode, string message, Exception exception = null)
		{
			if (logger.IsEnabled(minLevel))
				logger.Log(mode, message, exception);
		}

		/// <summary>
		/// Writes a log message
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="minLevel">The minimum level (for checking when write)</param>
		/// <param name="mode">Write mode</param>
		/// <param name="message">The log message</param>
		/// <param name="exception">The exception</param>
		public static void Log<T>(LogLevel minLevel, LogLevel mode, string message, Exception exception = null)
			=> Logger.CreateLogger<T>().Log(minLevel, mode, message, exception);
	}
	#endregion

	#region Assembly loader
	public class AssemblyLoader
	{
		/// <summary>
		/// Gets the loaded assembly
		/// </summary>
		public Assembly Assembly { get; }

		AssemblyLoadContext AssemblyLoadContext { get; }

		DependencyContext DependencyContext { get; }

		ICompilationAssemblyResolver AssemblyResolver { get; }

		/// <summary>
		/// Creates new instance to dynamic load an assembly
		/// </summary>
		/// <param name="assemblyPath">The full path to assembly</param>
		public AssemblyLoader(string assemblyPath)
		{
			// load assembly
			var directory = Path.GetDirectoryName(assemblyPath);
			this.Assembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(assemblyPath);
			this.AssemblyLoadContext = AssemblyLoadContext.GetLoadContext(this.Assembly);

			// load all assemblies that specified by dependencies file
			if (File.Exists(Path.Combine(directory, $"{Path.GetFileNameWithoutExtension(assemblyPath)}.deps.json")))
			{
				this.DependencyContext = DependencyContext.Load(this.Assembly);

				this.AssemblyResolver = new CompositeCompilationAssemblyResolver(new ICompilationAssemblyResolver[]
				{
					new AppBaseCompilationAssemblyResolver(directory),
					new ReferenceAssemblyPathResolver(),
					new PackageCompilationAssemblyResolver()
				});

				this.AssemblyLoadContext.Resolving += (assemblyLoadContext, assemblyName) =>
				{
					var runtimeLib = this.DependencyContext.RuntimeLibraries.FirstOrDefault(runtime => string.Equals(runtime.Name, assemblyName.Name, StringComparison.OrdinalIgnoreCase));
					if (runtimeLib != null)
					{
						var compilationLib = new CompilationLibrary(
							runtimeLib.Type,
							runtimeLib.Name,
							runtimeLib.Version,
							runtimeLib.Hash,
							runtimeLib.RuntimeAssemblyGroups.SelectMany(g => g.AssetPaths),
							runtimeLib.Dependencies,
							runtimeLib.Serviceable
						);
						var assemblyPaths = new List<string>();
						this.AssemblyResolver.TryResolveAssemblyPaths(compilationLib, assemblyPaths);
						return assemblyPaths.Count > 0 ? assemblyLoadContext.LoadFromAssemblyPath(assemblyPaths.First()) : null;
					}
					return null;
				};
			}

			// doesn't have dependencies file => load referenced assembies
			else
				this.Assembly.GetReferencedAssemblies()
					.Where(assemblyName => File.Exists(Path.Combine(directory, $"{assemblyName.Name}.dll")))
					.ToList()
					.ForEach(assemblyName => this.AssemblyLoadContext.LoadFromAssemblyPath(Path.Combine(directory, $"{assemblyName.Name}.dll")));
		}

		/// <summary>
		/// Gets the type from an assembly by the specified name (full class name)
		/// </summary>
		/// <param name="assemblyFilePath">The absolute path of assembly</param>
		/// <param name="typeName">The type name (full class name)</param>
		/// <returns></returns>
		public static Type GetType(string assemblyFilePath, string typeName)
			=> !string.IsNullOrWhiteSpace(assemblyFilePath) && !string.IsNullOrWhiteSpace(typeName) && File.Exists(assemblyFilePath)
				? new AssemblyLoader(assemblyFilePath).Assembly.GetExportedTypes().FirstOrDefault(type => typeName.Equals(type.ToString()))
				:  null;

		/// <summary>
		/// Gets the type by the specified type name (full class name with assembly)
		/// </summary>
		/// <param name="typeNameWithAssembly">The type name (full class name with assembly)</param>
		/// <returns></returns>
		public static Type GetType(string typeNameWithAssembly)
		{
			if (string.IsNullOrWhiteSpace(typeNameWithAssembly) || typeNameWithAssembly.IndexOf(",") < 0)
				return null;

			var type = Type.GetType(typeNameWithAssembly);
			if (type == null)
			{
				var info = typeNameWithAssembly.Trim().Split(',').Select(data => data.Trim()).ToList();
				type = AssemblyLoader.GetType(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"{info[1]}{(info[1].EndsWith(".dll", StringComparison.OrdinalIgnoreCase) ? "" : ".dll")}"), info[0]);
			}

			return type;
		}
	}
	#endregion

	#region Fast activator to avoid reflection
	/// <summary>
	/// <para>Implements a very fast object factory for dynamic object creation. Dynamically generates a factory class which will use the new() constructor of the requested type.</para>
	/// <para>Much faster than using Activator at the price of the first invocation being significantly slower than subsequent calls.</para>
	/// </summary>
	public static class FastActivator
	{
		static ConcurrentDictionary<Type, Func<object>> Factories { get; } = new ConcurrentDictionary<Type, Func<object>>();

		/// <summary>
		/// Creates an instance of the specified type using a generated factory to avoid using Reflection.
		/// </summary>
		/// <param name="type">The type to be created.</param>
		/// <returns>The newly created instance.</returns>
		public static object Create(Type type)
		{
			if (!FastActivator.Factories.TryGetValue(type, out var func))
				lock (FastActivator.Factories)
				{
					if (!FastActivator.Factories.TryGetValue(type, out func))
						FastActivator.Factories[type] = func = Expression.Lambda<Func<object>>(Expression.New(type)).Compile();
				}
			return func();
		}

		/// <summary>
		/// Creates an instance of the specified type using a generated factory to avoid using Reflection.
		/// </summary>
		/// <typeparam name="T">The type to be created.</typeparam>
		/// <returns>The newly created instance.</returns>
		public static T Create<T>()
			=> (T)FastActivator.Create(typeof(T));

		/// <summary>
		/// Creates an instance of the specified type using a generated factory to avoid using Reflection.
		/// </summary>
		/// <param name="type">The type to be created.</param>
		/// <returns>The newly created instance.</returns>
		public static object Create(string type)
		{
			var theType = AssemblyLoader.GetType(type);
			return theType != null ? FastActivator.Create(theType) : null;
		}
	}
	#endregion

}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2022 VIEApps.net
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *		http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
