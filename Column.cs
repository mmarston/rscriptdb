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
using System.Collections.Generic;

namespace Mercent.AWS.Redshift
{
	public class Column : NamedChildObject
	{
		public string CompressionEncoding { get; set; }
		public string DataType { get; set; }
		public string DefaultValue { get; set; }
		public bool HasCompressionEncoding
		{
			get
			{
				return CompressionEncoding != null
					&& !CompressionEncoding.Equals("NONE", StringComparison.OrdinalIgnoreCase)
					&& !CompressionEncoding.Equals("RAW", StringComparison.OrdinalIgnoreCase);
			}
		}
		public bool IsDistributionKey { get; set; }
		public bool IsNullable { get; set; }
		public int SortKeyNumber { get; set; }
	}
}
