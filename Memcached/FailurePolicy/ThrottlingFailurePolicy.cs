﻿using System;
using System.Collections.Generic;
using Enyim.Caching.Configuration;
using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Fails a node when the specified number of failures happen in a specified time window.
	/// </summary>
	public class ThrottlingFailurePolicy : INodeFailurePolicy
	{
		readonly ILogger _logger;
		readonly int resetAfter;
		readonly int failureThreshold;
		int failCounter;
		DateTime lastFailed;

		/// <summary>
		/// Creates a new instance of <see cref="ThrottlingFailurePolicy"/>.
		/// </summary>
		/// <param name="resetAfter">Specifies the time in milliseconds how long a node should function properly to reset its failure counter.</param>
		/// <param name="failureThreshold">Specifies the number of failures that must occur in the specified time window to fail a node.</param>
		public ThrottlingFailurePolicy(int resetAfter, int failureThreshold)
		{
			this._logger = Logger.CreateLogger<ThrottlingFailurePolicy>();
			this.resetAfter = resetAfter;
			this.failureThreshold = failureThreshold;
		}

		bool INodeFailurePolicy.ShouldFail()
		{
			var now = DateTime.UtcNow;

			if (lastFailed == DateTime.MinValue)
			{
				this._logger.Log(LogLevel.Debug, LogLevel.Debug, "Setting fail counter to 1.");
				failCounter = 1;
			}
			else
			{
				var diff = (int)(now - lastFailed).TotalMilliseconds;
				this._logger.Log(LogLevel.Debug, LogLevel.Debug, $"Last fail was {diff} msec ago with counter {this.failCounter}.");

				if (diff <= this.resetAfter)
					this.failCounter++;
				else
				{
					this.failCounter = 1;
				}
			}

			lastFailed = now;

			if (this.failCounter == this.failureThreshold)
			{
				this._logger.Log(LogLevel.Debug, LogLevel.Debug, "Threshold reached, node will fail.");
				this.lastFailed = DateTime.MinValue;
				this.failCounter = 0;
				return true;
			}

			this._logger.Log(LogLevel.Debug, LogLevel.Debug, $"Current counter is {this.failCounter}, threshold not reached.");
			return false;
		}
	}

	/// <summary>
	/// Creates instances of <see cref="ThrottlingFailurePolicy"/>.
	/// </summary>
	public class ThrottlingFailurePolicyFactory : INodeFailurePolicyFactory, IProviderFactory<INodeFailurePolicyFactory>
	{
		public ThrottlingFailurePolicyFactory(int failureThreshold, TimeSpan resetAfter) : this(failureThreshold, (int)resetAfter.TotalMilliseconds) { }

		public ThrottlingFailurePolicyFactory(int failureThreshold, int resetAfter)
		{
			this.ResetAfter = resetAfter;
			this.FailureThreshold = failureThreshold;
		}

		// used by the config files
		internal ThrottlingFailurePolicyFactory() { }

		/// <summary>
		/// Gets or sets the amount of time of in milliseconds after the failure counter is reset.
		/// </summary>
		public int ResetAfter { get; set; }

		/// <summary>
		/// Gets or sets the number of failures that must happen in a time window to make a node marked as failed.
		/// </summary>
		public int FailureThreshold { get; set; }

		INodeFailurePolicy INodeFailurePolicyFactory.Create(IMemcachedNode node)
			=> new ThrottlingFailurePolicy(this.ResetAfter, this.FailureThreshold);

		INodeFailurePolicyFactory IProviderFactory<INodeFailurePolicyFactory>.Create()
			=> new ThrottlingFailurePolicyFactory(this.FailureThreshold, this.ResetAfter);

		void IProvider.Initialize(Dictionary<string, string> parameters)
		{
			ConfigurationHelper.TryGetAndRemove(parameters, "failureThreshold", out int failureThreshold, true);
			if (failureThreshold < 1)
				throw new InvalidOperationException("failureThreshold must be > 0");

			this.FailureThreshold = failureThreshold;

			ConfigurationHelper.TryGetAndRemove(parameters, "resetAfter", out TimeSpan reset, true);
			if (reset <= TimeSpan.Zero)
				throw new InvalidOperationException("resetAfter must be > 0msec");

			this.ResetAfter = (int)reset.TotalMilliseconds;
		}
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
