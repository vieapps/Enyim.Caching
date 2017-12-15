using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Threading;

using Microsoft.Extensions.Logging;

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
			Node node = new Node(default(T));
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

				// Are head, tail, and next consistent?
				if (object.ReferenceEquals(this.headNode, head))
				{
					// is tail falling behind
					if (object.ReferenceEquals(head, tail))
					{
						// is the queue empty?
						if (object.ReferenceEquals(next, null))
						{
							value = default(T);
							// queue is empty and cannot dequeue
							return false;
						}

						Interlocked.CompareExchange<Node>(ref this.tailNode, next, tail);
					}

					// No need to deal with tail
					else
					{
						// read value before CAS otherwise another deque might try to free the next node
						value = next.Value;

						// try to swing the head to the next node
						if (Interlocked.CompareExchange<Node>(ref this.headNode, next, head) == head)
						{
							return true;
						}
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
						if (object.ReferenceEquals(next, null))
						{
							value = default(T);

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
					if (object.ReferenceEquals(next, null))
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
		Node head;

		public InterlockedStack()
		{
			this.head = new Node(default(T));
		}

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
			value = default(T);
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

namespace Enyim.Reflection
{

	#region Fast activator to avoid reflection
	/// <summary>
	/// <para>Implements a very fast object factory for dynamic object creation. Dynamically generates a factory class which will use the new() constructor of the requested type.</para>
	/// <para>Much faster than using Activator at the price of the first invocation being significantly slower than subsequent calls.</para>
	/// </summary>
	public static class FastActivator
	{
		static Dictionary<Type, Func<object>> Factories = new Dictionary<Type, Func<object>>();

		/// <summary>
		/// Creates an instance of the specified type using a generated factory to avoid using Reflection.
		/// </summary>
		/// <param name="type">The type to be created.</param>
		/// <returns>The newly created instance.</returns>
		public static object Create(Type type)
		{
			if (!FastActivator.Factories.TryGetValue(type, out Func<object> func))
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
		{
			return (T)FastActivator.Create(typeof(T));
		}

		/// <summary>
		/// Creates an instance of the specified type using a generated factory to avoid using Reflection.
		/// </summary>
		/// <param name="type">The type to be created.</param>
		/// <returns>The newly created instance.</returns>
		public static object Create(string type)
		{
			return FastActivator.Create(Type.GetType(type));
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
		/// Creates a logger
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static ILogger CreateLogger(Type type)
		{
			return (Logger.LoggerFactory ?? new NullLoggerFactory()).CreateLogger(type);
		}

		/// <summary>
		/// Creates a logger
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public static ILogger CreateLogger<T>()
		{
			return Logger.CreateLogger(typeof(T));
		}
	}
	#endregion

	#region NullLogger
	public class NullLoggerFactory : ILoggerFactory
	{
		public void AddProvider(ILoggerProvider provider) { }

		public ILogger CreateLogger(string categoryName)
		{
			return NullLogger.Instance;
		}

		public void Dispose() { }
	}

	public class NullLogger : ILogger
	{
		internal static NullLogger Instance = new NullLogger();

		private NullLogger() { }

		public IDisposable BeginScope<TState>(TState state)
		{
			return null;
		}

		public bool IsEnabled(LogLevel logLevel)
		{
			return false;
		}

		public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter) { }
	}
	#endregion

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
