using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// This is a ketama-like consistent hashing based node locator.
	/// Used when no other <see cref="INodeLocator"/> is specified for the pool.
	/// </summary>
	public sealed class DefaultNodeLocator : INodeLocator, IDisposable
	{
		const int ServerAddressMutations = 100;

		// holds all server keys for mapping an item key to the server consistently
		uint[] _keys;

		// used to lookup a server based on its key
		Dictionary<uint, IMemcachedNode> _servers;
		Dictionary<IMemcachedNode, bool> _deadServers;
		List<IMemcachedNode> _allServers;
		ReaderWriterLockSlim _locker;

		public DefaultNodeLocator()
		{
			this._servers = new Dictionary<uint, IMemcachedNode>(new UIntEqualityComparer());
			this._deadServers = new Dictionary<IMemcachedNode, bool>();
			this._allServers = new List<IMemcachedNode>();
			this._locker = new ReaderWriterLockSlim();
		}

		void BuildIndex(List<IMemcachedNode> nodes)
		{
			var keys = new uint[nodes.Count * DefaultNodeLocator.ServerAddressMutations];
			var nodeIndex = 0;
			foreach (var node in nodes)
			{
				var tempKeys = DefaultNodeLocator.GenerateKeys(node, DefaultNodeLocator.ServerAddressMutations);
				for (var index = 0; index < tempKeys.Length; index++)
					this._servers[tempKeys[index]] = node;
				tempKeys.CopyTo(keys, nodeIndex);
				nodeIndex += DefaultNodeLocator.ServerAddressMutations;
			}

			Array.Sort<uint>(keys);
			Interlocked.Exchange(ref this._keys, keys);
		}

		void INodeLocator.Initialize(IList<IMemcachedNode> nodes)
		{
			this._locker.EnterWriteLock();
			try
			{
				this._allServers = nodes.ToList();
				this.BuildIndex(this._allServers);
			}
			finally
			{
				this._locker.ExitWriteLock();
			}
		}

		IMemcachedNode INodeLocator.Locate(string key)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));

			this._locker.EnterUpgradeableReadLock();
			try
			{
				return this.Locate(key);
			}
			finally
			{
				this._locker.ExitUpgradeableReadLock();
			}
		}

		IEnumerable<IMemcachedNode> INodeLocator.GetWorkingNodes()
		{
			this._locker.EnterReadLock();
			try
			{
				return this._allServers.Except(this._deadServers.Keys).ToList();
			}
			finally
			{
				this._locker.ExitReadLock();
			}
		}

		IMemcachedNode Locate(string key)
		{
			var node = this.FindNode(key);
			if (node == null || node.IsAlive)
				return node;

			// move the current node to the dead list and rebuild the indexes
			this._locker.EnterWriteLock();
			try
			{
				// check if it's still dead or it came back
				// while waiting for the write lock
				if (!node.IsAlive)
					this._deadServers[node] = true;
				this.BuildIndex(this._allServers.Except(this._deadServers.Keys).ToList());
			}
			finally
			{
				this._locker.ExitWriteLock();
			}

			// try again with the dead server removed from the lists
			return this.Locate(key);
		}

		/// <summary>
		/// locates a node by its key
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		IMemcachedNode FindNode(string key)
		{
			if (this._keys.Length == 0)
				return null;

			var itemKeyHash = BitConverter.ToUInt32(new FNV1a().ComputeHash(Encoding.UTF8.GetBytes(key)), 0);

			// get the index of the server assigned to this hash
			var foundIndex = Array.BinarySearch<uint>(this._keys, itemKeyHash);

			// no exact match
			if (foundIndex < 0)
			{
				// this is the nearest server in the list
				foundIndex = ~foundIndex;

				// it's smaller than everything, so use the last server (with the highest key)
				if (foundIndex == 0)
					foundIndex = this._keys.Length - 1;

				// the key was larger than all server keys, so return the first server
				else if (foundIndex >= this._keys.Length)
					foundIndex = 0;
			}

			if (foundIndex < 0 || foundIndex > this._keys.Length)
				return null;

			return this._servers[this._keys[foundIndex]];
		}

		static uint[] GenerateKeys(IMemcachedNode node, int numberOfKeys)
		{
			const int KeyLength = 4;
			const int PartCount = 1; // (ModifiedFNV.HashSize / 8) / KeyLength; // HashSize is in bits, uint is 4 byte long

			var keys = new uint[PartCount * numberOfKeys];

			// every server is registered numberOfKeys times
			// using UInt32s generated from the different parts of the hash
			// i.e. hash is 64 bit:
			// 00 00 aa bb 00 00 cc dd
			// server will be stored with keys 0x0000aabb & 0x0000ccdd
			// (or a bit differently based on the little/big indianness of the host)
			var address = node.EndPoint.ToString();
			var fnv = new FNV1a();

			for (int index = 0; index < numberOfKeys; index++)
			{
				var data = fnv.ComputeHash(Encoding.UTF8.GetBytes(String.Concat(address, "-", index)));
				for (var partIndex = 0; partIndex < PartCount; partIndex++)
					keys[index * PartCount + partIndex] = BitConverter.ToUInt32(data, partIndex * KeyLength);
			}

			return keys;
		}

		#region [ IDisposable                  ]
		void IDisposable.Dispose()
		{
			using (this._locker)
			{
				this._locker.EnterWriteLock();

				try
				{
					// kill all pending operations (with an exception)
					// it's not nice, but disposeing an instance while being used is bad practice
					this._allServers = null;
					this._servers = null;
					this._keys = null;
					this._deadServers = null;
				}
				finally
				{
					this._locker.ExitWriteLock();
				}
			}

			this._locker = null;
		}
		#endregion

	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2019 VIEApps.net
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
