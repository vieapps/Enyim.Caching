namespace Enyim.Caching.Memcached.Results
{
	public class GetOperationResult : OperationResultBase, IGetOperationResult
	{
		public ulong Cas { get; set; }

		public bool HasValue { get { return Value != null; } }

		public object Value { get; set; }
	}

	public class GetOperationResult<T> : OperationResultBase, IGetOperationResult<T>
	{
		public bool HasValue { get { return Value != null; } }

		public T Value { get; set; }

		public ulong Cas { get; set; }
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
