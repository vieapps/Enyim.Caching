using System.Collections.Generic;
namespace Enyim.Caching
{
	/// <summary>
	/// A fast comparer for dictionaries indexed by UInt. Faster than using Comparer.Default
	/// </summary>
	public sealed class UIntEqualityComparer : IEqualityComparer<uint>
	{
		bool IEqualityComparer<uint>.Equals(uint x, uint y)
            => x == y;

		int IEqualityComparer<uint>.GetHashCode(uint value)
            => value.GetHashCode();
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    © 2010 Attila Kiskó (aka Enyim), © 2016 CNBlogs, © 2021 VIEApps.net
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
