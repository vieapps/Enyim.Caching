﻿namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Fails a node immediately when an error occures. This is the default policy.
	/// </summary>
	public sealed class FailImmediatelyPolicy : INodeFailurePolicy
	{
		bool INodeFailurePolicy.ShouldFail() => true;
	}

	/// <summary>
	/// Creates instances of <see cref="FailImmediatelyPolicy"/>.
	/// </summary>
	public class FailImmediatelyPolicyFactory : INodeFailurePolicyFactory
	{
		static readonly INodeFailurePolicy PolicyInstance = new FailImmediatelyPolicy();

		INodeFailurePolicy INodeFailurePolicyFactory.Create(IMemcachedNode node) => PolicyInstance;
	}
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