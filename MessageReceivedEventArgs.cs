//   Copyright © 2005-2016 Commerce Technologies, LLC
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
using System;

namespace Mercent.AWS.Redshift
{
	/// <summary>
	/// Event arguments to pass a message.
	/// </summary>
	/// <remarks>
	/// This class mirrors the functionality of <see cref="System.Diagnostics.DataReceivedEventArgs"/>.
	/// Since that class does not have a public constructor it can't be used when we need to create our own events.
	/// </remarks>
	public class MessageReceivedEventArgs : EventArgs
	{
		public MessageReceivedEventArgs(string message)
		{
			this.Message = message;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <remarks>
		/// Just like the <see cref="System.Diagnostics.DataReceivedEventArgs">DataReceivedEventArgs.Data</see> property,
		/// this Message property may be null. Event handlers should be prepared to handle a null value.
		/// </remarks>
		public string Message { get; private set; }
	}
}
