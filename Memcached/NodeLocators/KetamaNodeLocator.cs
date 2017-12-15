using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Security.Cryptography;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Implements Ketama cosistent hashing, compatible with the "spymemcached" Java client
	/// </summary>
	public sealed class KetamaNodeLocator : INodeLocator
	{
		static int ServerAddressMutations = 160;
		static Dictionary<string, Func<HashAlgorithm>> Factories = new Dictionary<string, Func<HashAlgorithm>>(StringComparer.OrdinalIgnoreCase)
		{
			{ "md5", () => MD5.Create() },
			{ "sha1", () => SHA1.Create() },
			{ "tiger", () => new TigerHash() },
			{ "crc", () => new HashkitCrc32() },
			{ "fnv1_32", () => new FNV1() },
			{ "fnv1_64", () => new FNV1a() },
			{ "fnv1a_32", () => new FNV64() },
			{ "fnv1a_64", () => new FNV64a() },
			{ "murmur", () => new HashkitMurmur() },
			{ "oneatatime", () => new HashkitOneAtATime() }
		};


		LookupData _lookupData;
		string _hashName;
		Func<HashAlgorithm> _hashFactory;

		/// <summary>
		/// Initialized a new instance of the <see cref="KetamaNodeLocator"/> using a custom hash algorithm.
		/// </summary>
		/// <param name="hashName">The name of the hash algorithm to use.
		/// <list type="table">
		/// <listheader><term>Name</term><description>Description</description></listheader>
		/// <item><term>md5</term><description>Equivalent of System.Security.Cryptography.MD5</description></item>
		/// <item><term>sha1</term><description>Equivalent of System.Security.Cryptography.SHA1</description></item>
		/// <item><term>tiger</term><description>Tiger Hash</description></item>
		/// <item><term>crc</term><description>CRC32</description></item>
		/// <item><term>fnv1_32</term><description>FNV Hash 32bit</description></item>
		/// <item><term>fnv1_64</term><description>FNV Hash 64bit</description></item>
		/// <item><term>fnv1a_32</term><description>Modified FNV Hash 32bit</description></item>
		/// <item><term>fnv1a_64</term><description>Modified FNV Hash 64bit</description></item>
		/// <item><term>murmur</term><description>Murmur Hash</description></item>
		/// <item><term>oneatatime</term><description>Jenkin's "One at A time Hash"</description></item>
		/// </list>
		/// </param>
		/// <remarks>If the hashName does not match any of the item on the list it will be passed to HashAlgorithm.Create, the default hash (MD5) will be used</remarks>
		public KetamaNodeLocator(string hashName = null)
		{
			this._hashName = hashName ?? "md5";
			if (!KetamaNodeLocator.Factories.TryGetValue(this._hashName, out this._hashFactory))
				this._hashFactory = () => MD5.Create();
		}

		void INodeLocator.Initialize(IList<IMemcachedNode> nodes)
		{
			// quit if we've been initialized because we can handle dead nodes,
			// so there is no need to recalculate everything
			if (this._lookupData != null)
				return;

			// sizeof(uint)
			const int KeyLength = 4;
			var hashAlgorithm = this._hashFactory();

			int partCount = hashAlgorithm.HashSize / 8 / KeyLength; // HashSize is in bits, uint is 4 bytes long
			if (partCount < 1)
				throw new ArgumentOutOfRangeException("The hash algorithm must provide at least 32 bits long hashes");

			var keys = new List<uint>(nodes.Count * KetamaNodeLocator.ServerAddressMutations);
			var keyToServer = new Dictionary<uint, IMemcachedNode>(keys.Count, new UIntEqualityComparer());

			for (int nodeIndex = 0; nodeIndex < nodes.Count; nodeIndex++)
			{
				var currentNode = nodes[nodeIndex];

				// every server is registered numberOfKeys times
				// using UInt32s generated from the different parts of the hash
				// i.e. hash is 64 bit:
				// 01 02 03 04 05 06 07
				// server will be stored with keys 0x07060504 & 0x03020100
				var address = currentNode.EndPoint.ToString();
				for (var mutation = 0; mutation < KetamaNodeLocator.ServerAddressMutations / partCount; mutation++)
				{
					var data = hashAlgorithm.ComputeHash(Encoding.ASCII.GetBytes(address + "-" + mutation));
					for (var part = 0; part < partCount; part++)
					{
						var tmp = part * 4;
						var key = ((uint)data[tmp + 3] << 24)
							| ((uint)data[tmp + 2] << 16)
							| ((uint)data[tmp + 1] << 8)
							| ((uint)data[tmp]);
						keys.Add(key);
						keyToServer[key] = currentNode;
					}
				}
			}

			keys.Sort();

			var lookupData = new LookupData
			{
				keys = keys.ToArray(),
				KeyToServer = keyToServer,
				Servers = nodes.ToArray()
			};

			Interlocked.Exchange(ref this._lookupData, lookupData);
		}

		uint GetKeyHash(string key)
		{
			var keyData = Encoding.UTF8.GetBytes(key);
			var hashAlgorithm = this._hashFactory();
			var uintHash = hashAlgorithm as IUIntHashAlgorithm;

			// shortcut for internal hash algorithms
			if (uintHash == null)
			{
				var data = hashAlgorithm.ComputeHash(keyData);
				return ((uint)data[3] << 24) | ((uint)data[2] << 16) | ((uint)data[1] << 8) | ((uint)data[0]);
			}

			return uintHash.ComputeHash(keyData);
		}

		IMemcachedNode INodeLocator.Locate(string key)
		{
			if (key == null)
				throw new ArgumentNullException("key");

			var id = this._lookupData;
			switch (id.Servers.Length)
			{
				case 0:
					return null;
				case 1:
					var tmp = id.Servers[0];
					return tmp.IsAlive ? tmp : null;
			}

			var node = KetamaNodeLocator.LocateNode(id, this.GetKeyHash(key));

			// if the result is not alive then try to mutate the item key and 
			// find another node this way we do not have to reinitialize every 
			// time a node dies/comes back
			// (DefaultServerPool will resurrect the nodes in the background without affecting the hashring)
			if (!node.IsAlive)
			{
				for (var i = 0; i < id.Servers.Length; i++)
				{
					// -- this is from spymemcached so we select the same node for the same items
					var tmpKey = (ulong)GetKeyHash(i + key);
					tmpKey += (uint)(tmpKey ^ (tmpKey >> 32));
					tmpKey &= 0xffffffffL; /* truncate to 32-bits */
					// -- end

					node = KetamaNodeLocator.LocateNode(id, (uint)tmpKey);

					if (node.IsAlive)
						return node;
				}
			}

			return node.IsAlive ? node : null;
		}

		IEnumerable<IMemcachedNode> INodeLocator.GetWorkingNodes()
		{
			var id = this._lookupData;
			if (id.Servers == null || id.Servers.Length == 0)
				return Enumerable.Empty<IMemcachedNode>();

			var nodes = new IMemcachedNode[id.Servers.Length];
			Array.Copy(id.Servers, nodes, nodes.Length);

			return nodes;
		}

		static IMemcachedNode LocateNode(LookupData id, uint itemKeyHash)
		{
			// get the index of the server assigned to this hash
			var index = Array.BinarySearch<uint>(id.keys, itemKeyHash);

			// no exact match
			if (index < 0)
			{
				// this is the nearest server in the list
				index = ~index;

				// it's smaller than everything, so use the last server (with the highest key)
				if (index == 0)
					index = id.keys.Length - 1;

				// the key was larger than all server keys, so return the first server
				else if (index >= id.keys.Length)
					index = 0;
			}

			return index < 0 || index > id.keys.Length
				? null
				: id.KeyToServer[id.keys[index]];
		}

		/// <summary>
		/// This will encapsulate all the indexes we need for lookup so the instance can be reinitialized without locking
		/// in case an <see cref="Enyim.Caching.Configuration.IMemcachedClientConfiguration"/> implementation returns the same instance form the CreateLocator()
		/// </summary>
		class LookupData
		{
			public IMemcachedNode[] Servers;

			// holds all server keys for mapping an item key to the server consistently
			public uint[] keys;

			// used to lookup a server based on its key
			public Dictionary<uint, IMemcachedNode> KeyToServer;
		}
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
