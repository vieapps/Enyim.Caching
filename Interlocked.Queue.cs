using System;
using System.Threading;

namespace Enyim.Collections
{
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
				if (Object.ReferenceEquals(this.headNode, head))
				{
					// is tail falling behind
					if (Object.ReferenceEquals(head, tail))
					{
						// is the queue empty?
						if (Object.ReferenceEquals(next, null))
						{
							value = default(T);

							// queue is empty and cannot dequeue
							return false;
						}

						Interlocked.CompareExchange<Node>(
							ref this.tailNode,
							next,
							tail);
					}
					else // No need to deal with tail
					{
						// read value before CAS otherwise another deque might try to free the next node
						value = next.Value;

						// try to swing the head to the next node
						if (Interlocked.CompareExchange<Node>(
							ref this.headNode,
							next,
							head) == head)
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
				if (Object.ReferenceEquals(this.headNode, head))
				{
					// is tail falling behind
					if (Object.ReferenceEquals(head, tail))
					{
						// is the queue empty?
						if (Object.ReferenceEquals(next, null))
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
				if (Object.ReferenceEquals(tail, this.tailNode))
				{
					// was tail pointing to the last node?
					if (Object.ReferenceEquals(next, null))
					{
						if (Object.ReferenceEquals(Interlocked.CompareExchange(ref tail.Next, valueNode, next), next))
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

		#region [ Node                        ]
		class Node
		{
			public readonly T Value;
			public Node Next;

			public Node(T value)
			{
				this.Value = value;
			}
		}
		#endregion

	}
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
